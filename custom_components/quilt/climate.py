"""Climate platform for Quilt heat-pump rooms."""
from __future__ import annotations

import time
from typing import Any

from homeassistant.components.climate import (
    ClimateEntity,
    ClimateEntityFeature,
    HVACAction,
    HVACMode,
)
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

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

# Quilt "Off"-preset sentinels: a very low heat / very high cool threshold
# disables that side, which is how we express heat-only / cool-only.
HEAT_DISABLED = api.HEAT_DISABLED  # 8.0
COOL_DISABLED = api.COOL_DISABLED  # 40.0


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up Quilt climate entities."""
    coordinator: QuiltCoordinator = entry.runtime_data
    async_add_entities(
        QuiltClimate(coordinator, room_id) for room_id in coordinator.data["rooms"]
    )


class QuiltClimate(CoordinatorEntity[QuiltCoordinator], ClimateEntity):
    """A Quilt room exposed as a heat/cool/auto thermostat."""

    _attr_has_entity_name = True
    _attr_name = None
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_hvac_modes = [HVACMode.OFF, HVACMode.HEAT, HVACMode.COOL, HVACMode.HEAT_COOL]
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
        return self.coordinator.data["rooms"][self._room_id]

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
        if s_heat is not None and s_heat > 9:
            self._desired_heat = s_heat
        if s_cool is not None and s_cool < 39:
            self._desired_cool = s_cool

    @callback
    def _handle_coordinator_update(self) -> None:
        self._ingest()
        super()._handle_coordinator_update()

    # --- state -----------------------------------------------------------
    @property
    def hvac_mode(self) -> HVACMode:
        room = self.room
        if not room["on"]:
            return HVACMode.OFF
        cool = room["cool_setpoint"]
        heat = room["heat_setpoint"]
        if cool is not None and cool >= 39:
            return HVACMode.HEAT
        if heat is not None and heat <= 9:
            return HVACMode.COOL
        return HVACMode.HEAT_COOL

    @property
    def hvac_action(self) -> HVACAction:
        room = self.room
        if not room["on"]:
            return HVACAction.OFF
        t = room["current_temp"]
        if t is None:
            return HVACAction.IDLE
        heat = room["heat_setpoint"]
        cool = room["cool_setpoint"]
        if cool is not None and cool < 39 and t > cool + 0.2:
            return HVACAction.COOLING
        if heat is not None and heat > 9 and t < heat - 0.2:
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
        if mode == HVACMode.COOL:
            return self._desired_cool
        return None  # HEAT_COOL / OFF use the range attributes

    @property
    def target_temperature_high(self) -> float | None:
        return min(max(self._desired_cool, COOL_MIN), COOL_MAX)

    @property
    def target_temperature_low(self) -> float | None:
        return min(max(self._desired_heat, HEAT_MIN), HEAT_MAX)

    # --- commands --------------------------------------------------------
    def _effective(self, mode: HVACMode) -> tuple[float, float]:
        if mode == HVACMode.HEAT:
            return self._desired_heat, COOL_DISABLED
        if mode == HVACMode.COOL:
            return HEAT_DISABLED, self._desired_cool
        return self._desired_heat, self._desired_cool  # HEAT_COOL

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        self._hold()
        if hvac_mode == HVACMode.OFF:
            await self.hass.async_add_executor_job(
                self.coordinator.client.set_active, self._room_id, False
            )
        else:
            heat, cool = self._effective(hvac_mode)
            await self.hass.async_add_executor_job(
                lambda: self.coordinator.client.set_setpoints(
                    self._room_id, heat=heat, cool=cool
                )
            )
        self._hold()
        await self.coordinator.async_request_refresh()

    async def async_set_temperature(self, **kwargs: Any) -> None:
        if (high := kwargs.get("target_temp_high")) is not None:
            self._desired_cool = high
        if (low := kwargs.get("target_temp_low")) is not None:
            self._desired_heat = low
        if (temp := kwargs.get(ATTR_TEMPERATURE)) is not None:
            mode = self.hvac_mode
            if mode == HVACMode.COOL:
                self._desired_cool = temp
            else:
                self._desired_heat = temp
        self._hold()
        # Apply only when on; when off, Quilt applies on next activation.
        if self.room["on"]:
            heat, cool = self._effective(self.hvac_mode)
            await self.hass.async_add_executor_job(
                lambda: self.coordinator.client.set_setpoints(
                    self._room_id, heat=heat, cool=cool
                )
            )
            self._hold()
            await self.coordinator.async_request_refresh()
        else:
            self.async_write_ha_state()

    async def async_set_preset_mode(self, preset_mode: str) -> None:
        self._hold()
        await self.hass.async_add_executor_job(
            self.coordinator.client.set_preset, self._room_id, preset_mode
        )
        self._hold()
        await self.coordinator.async_request_refresh()

    async def async_turn_off(self) -> None:
        await self.async_set_hvac_mode(HVACMode.OFF)

    async def async_turn_on(self) -> None:
        await self.async_set_hvac_mode(HVACMode.HEAT_COOL)
