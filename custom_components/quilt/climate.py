"""Climate platform for Quilt heat-pump rooms."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
import time
from typing import Any

from homeassistant.components.climate import (
    ATTR_HVAC_MODE,
    ClimateEntity,
    ClimateEntityFeature,
    HVACAction,
    HVACMode,
)
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_call_later

from . import api
from .const import (
    COOL_MAX,
    COOL_MIN,
    DOMAIN,
    HEAT_MAX,
    HEAT_MIN,
    WRITE_HOLD_SECONDS,
)
from .coordinator import QuiltCoordinator
from .entity import QuiltEntity

QUILT_TO_HA = {
    api.MODE_OFF: HVACMode.OFF,
    api.MODE_COOL: HVACMode.COOL,
    api.MODE_HEAT: HVACMode.HEAT,
    api.MODE_HEAT_COOL: HVACMode.HEAT_COOL,
    api.MODE_FAN: HVACMode.FAN_ONLY,
    api.MODE_DRY: HVACMode.DRY,
}
HA_TO_QUILT = {ha: quilt for quilt, ha in QUILT_TO_HA.items()}
# Read-only: fallback modes Quilt can pick itself; we never write them.
QUILT_TO_HA[api.MODE_FALLBACK_AUTO] = HVACMode.HEAT_COOL
QUILT_TO_HA[api.MODE_FALLBACK_OFF] = HVACMode.OFF

# What the unit itself reports it is doing (Space.state f4). "Deferred" means it
# is waiting out a mode-switch delay, so it isn't conditioning yet.
HVAC_STATE_TO_ACTION = {
    api.HVAC_STATE_STANDBY: HVACAction.IDLE,
    api.HVAC_STATE_COOL: HVACAction.COOLING,
    api.HVAC_STATE_HEAT: HVACAction.HEATING,
    api.HVAC_STATE_DRIFT: HVACAction.IDLE,
    api.HVAC_STATE_FAN: HVACAction.FAN,
    api.HVAC_STATE_COOL_DEFERRED: HVACAction.IDLE,
    api.HVAC_STATE_HEAT_DEFERRED: HVACAction.IDLE,
    api.HVAC_STATE_FAN_DEFERRED: HVACAction.IDLE,
    api.HVAC_STATE_COOL_PREPARING: HVACAction.COOLING,
    api.HVAC_STATE_HEAT_PREPARING: HVACAction.PREHEATING,
    api.HVAC_STATE_DRY: HVACAction.DRYING,
    api.HVAC_STATE_DRY_DEFERRED: HVACAction.IDLE,
    api.HVAC_STATE_DRY_PREPARING: HVACAction.DRYING,
}


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up Quilt climate entities."""
    coordinator: QuiltCoordinator = entry.runtime_data
    async_add_entities(
        QuiltClimate(coordinator, room_id) for room_id in coordinator.data["rooms"]
    )


class QuiltClimate(QuiltEntity, ClimateEntity):
    """A Quilt room exposed as a thermostat with Quilt's own modes."""

    _attr_name = None
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_hvac_modes = [
        HVACMode.OFF,
        HVACMode.COOL,
        HVACMode.HEAT,
        HVACMode.HEAT_COOL,
        HVACMode.FAN_ONLY,
        HVACMode.DRY,
    ]
    _attr_supported_features = (
        ClimateEntityFeature.TARGET_TEMPERATURE
        | ClimateEntityFeature.TARGET_TEMPERATURE_RANGE
        | ClimateEntityFeature.PRESET_MODE
        | ClimateEntityFeature.TURN_ON
        | ClimateEntityFeature.TURN_OFF
    )
    _attr_target_temperature_step = 0.5
    _attr_min_temp = HEAT_MIN
    _attr_max_temp = COOL_MAX

    def __init__(self, coordinator: QuiltCoordinator, room_id: str) -> None:
        super().__init__(coordinator)
        self._room_id = room_id
        self._attr_unique_id = f"quilt_{room_id}"
        # Desired setpoints, preserved across off states / writes (see accessory.js).
        self._desired_heat = 20.0
        self._desired_cool = 24.0
        self._hold_until = 0.0
        # What we last wrote, shown until the first poll after the hold.
        self._optimistic: dict | None = None
        self._unsub_hold: Callable[[], None] | None = None
        # HomeKit sends a mode change and a temperature change as concurrent
        # calls; each write is a read-modify-write of the room, so serialize them.
        self._write_lock = asyncio.Lock()
        self._ingest()
        # Comfort presets minus "Off" (handled by HVACMode.OFF), e.g. Eco/Sleep/Active.
        self._attr_preset_modes = [
            name for name in self.room.get("presets", {}) if name.lower() != "off"
        ]
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, room_id)},
            name=self.room.get("name"),
            manufacturer="Quilt",
            model="Heat Pump",
        )

    # --- helpers ---------------------------------------------------------
    @property
    def room(self) -> dict:
        base = self.coordinator.data["rooms"][self._room_id]
        if self._optimistic:
            return {**base, **self._optimistic}
        return base

    def _held(self) -> bool:
        return time.monotonic() < self._hold_until

    def _hold(self) -> None:
        self._hold_until = time.monotonic() + WRITE_HOLD_SECONDS

    def _ingest(self) -> None:
        """Seed desired heat/cool from the room (Active preset when off)."""
        if self._held():
            return
        room = self.room
        ap = room.get("presets", {}).get("Active")
        s_heat = room["heat_setpoint"] if room["on"] else (ap["heat"] if ap else room["heat_setpoint"])
        s_cool = room["cool_setpoint"] if room["on"] else (ap["cool"] if ap else room["cool_setpoint"])
        # The Off preset parks the setpoints at unusable extremes; keep the last real ones.
        if s_heat is not None and s_heat > 9:
            self._desired_heat = s_heat
        if s_cool is not None and s_cool < 39:
            self._desired_cool = s_cool

    @callback
    def _handle_coordinator_update(self) -> None:
        if not self._held():
            self._optimistic = None
        self._ingest()
        super()._handle_coordinator_update()

    async def _write(self, func: Callable[[], int]) -> int:
        """Run one blocking write; the hold keeps a mid-write poll from resetting setpoints."""
        async with self._write_lock:
            self._hold()
            return await self.hass.async_add_executor_job(func)

    async def _applied(self, mode: int, **extra: Any) -> None:
        """Show a successful write immediately; refresh from the cloud once the hold ends."""
        self._optimistic = {"mode": mode, "on": mode != api.MODE_OFF, **extra}
        self._hold()
        if self._unsub_hold:
            self._unsub_hold()
        self._unsub_hold = async_call_later(self.hass, WRITE_HOLD_SECONDS, self._hold_expired)
        self.async_write_ha_state()
        await self.coordinator.async_request_refresh()

    @callback
    def _hold_expired(self, _now: Any) -> None:
        self._unsub_hold = None
        self.hass.async_create_task(self.coordinator.async_request_refresh())

    async def async_will_remove_from_hass(self) -> None:
        if self._unsub_hold:
            self._unsub_hold()
            self._unsub_hold = None
        await super().async_will_remove_from_hass()

    def _clamp(self, mode: HVACMode | None) -> None:
        self._desired_heat = min(max(self._desired_heat, HEAT_MIN), HEAT_MAX)
        self._desired_cool = min(max(self._desired_cool, COOL_MIN), COOL_MAX)
        if self._desired_heat > self._desired_cool:
            if mode == HVACMode.HEAT:
                self._desired_cool = self._desired_heat
            else:
                self._desired_heat = self._desired_cool

    # --- state -----------------------------------------------------------
    @property
    def hvac_mode(self) -> HVACMode | None:
        return QUILT_TO_HA.get(self.room["mode"])

    @property
    def hvac_action(self) -> HVACAction:
        mode = self.hvac_mode
        if mode == HVACMode.OFF or not self.room["on"]:
            return HVACAction.OFF
        # Prefer what the unit reports; right after one of our own writes the
        # optimistic mode may be ahead of it, so fall back to the estimate then.
        reported = HVAC_STATE_TO_ACTION.get(self.room.get("hvac_state"))
        if reported is not None and not self._optimistic:
            return reported
        if mode is None:
            return HVACAction.IDLE
        if mode == HVACMode.FAN_ONLY:
            return HVACAction.FAN
        if mode == HVACMode.DRY:
            return HVACAction.DRYING
        t = self.room["current_temp"]
        if t is None:
            return HVACAction.IDLE
        if mode in (HVACMode.COOL, HVACMode.HEAT_COOL) and t > self._desired_cool + 0.2:
            return HVACAction.COOLING
        if mode in (HVACMode.HEAT, HVACMode.HEAT_COOL) and t < self._desired_heat - 0.2:
            return HVACAction.HEATING
        return HVACAction.IDLE

    @property
    def current_temperature(self) -> float | None:
        return self.room["current_temp"]

    @property
    def current_humidity(self) -> int | None:
        hum = self.room.get("humidity")
        return int(hum) if hum else None

    @property
    def preset_mode(self) -> str | None:
        room = self.room
        if not room["on"]:
            return None
        cid = room.get("active_comfort_id")
        for name, preset in room.get("presets", {}).items():
            if preset["id"] == cid:
                return name
        return None

    @property
    def target_temperature(self) -> float | None:
        mode = self.hvac_mode
        if mode == HVACMode.HEAT:
            return self._desired_heat
        if mode in (HVACMode.COOL, HVACMode.DRY):
            return self._desired_cool
        return None  # HEAT_COOL uses the range attributes; FAN_ONLY/OFF have no target

    @property
    def target_temperature_high(self) -> float | None:
        return min(max(self._desired_cool, COOL_MIN), COOL_MAX)

    @property
    def target_temperature_low(self) -> float | None:
        return min(max(self._desired_heat, HEAT_MIN), HEAT_MAX)

    # --- commands --------------------------------------------------------
    async def _write_mode(self, hvac_mode: HVACMode) -> None:
        if hvac_mode == HVACMode.OFF:
            mode = await self._write(
                lambda: self.coordinator.client.set_active(self._room_id, False)
            )
            await self._applied(mode)
            return
        self._clamp(hvac_mode)
        heat, cool = self._desired_heat, self._desired_cool
        mode = await self._write(
            lambda: self.coordinator.client.set_setpoints(
                self._room_id, heat=heat, cool=cool, mode=HA_TO_QUILT[hvac_mode]
            )
        )
        await self._applied(mode, heat_setpoint=heat, cool_setpoint=cool)

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        await self._write_mode(hvac_mode)

    async def async_set_temperature(self, **kwargs: Any) -> None:
        requested = kwargs.get(ATTR_HVAC_MODE)
        if requested is not None and requested not in HA_TO_QUILT:
            raise ServiceValidationError(f"Quilt has no {requested} mode")
        mode = requested or self.hvac_mode
        if (high := kwargs.get("target_temp_high")) is not None:
            self._desired_cool = high
        if (low := kwargs.get("target_temp_low")) is not None:
            self._desired_heat = low
        if (temp := kwargs.get(ATTR_TEMPERATURE)) is not None:
            if mode == HVACMode.HEAT:
                self._desired_heat = temp
            else:
                self._desired_cool = temp
        if requested is not None:
            await self._write_mode(requested)
            return
        # No mode given: the room keeps whatever mode Quilt reports at write
        # time, and an off room only stores the setpoints for its next turn-on.
        self._clamp(mode)
        heat, cool = self._desired_heat, self._desired_cool
        written = await self._write(
            lambda: self.coordinator.client.set_setpoints(self._room_id, heat=heat, cool=cool)
        )
        await self._applied(written, heat_setpoint=heat, cool_setpoint=cool)

    async def async_set_preset_mode(self, preset_mode: str) -> None:
        mode = await self._write(
            lambda: self.coordinator.client.set_preset(self._room_id, preset_mode)
        )
        preset = self.room.get("presets", {}).get(preset_mode) or {}
        await self._applied(mode, active_comfort_id=preset.get("id"))

    async def async_turn_off(self) -> None:
        await self._write_mode(HVACMode.OFF)

    async def async_turn_on(self) -> None:
        mode = await self._write(
            lambda: self.coordinator.client.set_active(self._room_id, True)
        )
        await self._applied(mode)
