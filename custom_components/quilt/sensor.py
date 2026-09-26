"""Quilt sensors: per-room humidity and energy, and the wall Dial.

Room humidity comes off each indoor head unit. Energy is Quilt's own per-room
metering (hourly buckets), summed from local midnight. The Dial reports the
temperature it measures plus three channels whose meaning isn't confirmed yet,
exposed as disabled diagnostic sensors. (The Dial value earlier versions showed
as humidity was a circuit-board temperature; it is no longer exposed.)
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from datetime import datetime

from homeassistant.const import (
    PERCENTAGE,
    EntityCategory,
    UnitOfEnergy,
    UnitOfTemperature,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import QuiltCoordinator
from .entity import QuiltEntity, room_device

DIAL_DEVICE_ID = "dial"


@dataclass(frozen=True, kw_only=True)
class QuiltDialSensorDescription(SensorEntityDescription):
    """Describes a dial sensor and how to read its value from the dial dict."""

    value_fn: Callable[[dict], float | int | None]


DIAL_SENSORS: tuple[QuiltDialSensorDescription, ...] = (
    QuiltDialSensorDescription(
        key="temperature",
        device_class=SensorDeviceClass.TEMPERATURE,
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: d.get("temperature"),
    ),
    # Unidentified ambient channels — diagnostic until their meaning is confirmed.
    QuiltDialSensorDescription(
        key="ambient_1",
        name="Ambient 1",
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda d: d.get("ambient_1"),
    ),
    QuiltDialSensorDescription(
        key="ambient_2",
        name="Ambient 2",
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda d: d.get("ambient_2"),
    ),
    QuiltDialSensorDescription(
        key="ambient_3",
        name="Ambient 3",
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda d: d.get("ambient_3"),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up Quilt room humidity, room energy and Dial sensors."""
    coordinator: QuiltCoordinator = entry.runtime_data
    entities: list[SensorEntity] = [
        QuiltRoomHumidity(coordinator, room_id)
        for room_id, room in coordinator.data["rooms"].items()
        if room.get("humidity") is not None
    ]
    entities.extend(QuiltRoomEnergy(coordinator, room_id) for room_id in coordinator.data["rooms"])
    if coordinator.data.get("dial"):
        entities.extend(
            QuiltDialSensor(coordinator, desc) for desc in DIAL_SENSORS
        )
    async_add_entities(entities)


class QuiltRoomHumidity(QuiltEntity, SensorEntity):
    """Per-room relative humidity measured by the indoor head."""

    _attr_name = "Humidity"
    _attr_device_class = SensorDeviceClass.HUMIDITY
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, coordinator: QuiltCoordinator, room_id: str) -> None:
        super().__init__(coordinator)
        self._room_id = room_id
        self._attr_unique_id = f"quilt_{room_id}_humidity"
        self._attr_device_info = room_device(coordinator, room_id)

    @property
    def native_value(self) -> int | None:
        return self.coordinator.data["rooms"][self._room_id].get("humidity")


class QuiltRoomEnergy(QuiltEntity, SensorEntity):
    """Energy the room's heat pump has used since local midnight (Quilt's metering)."""

    _attr_name = "Energy today"
    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    # TOTAL with last_reset, not TOTAL_INCREASING: Quilt can revise the
    # current hour's bucket down slightly, which must not read as a meter reset.
    _attr_state_class = SensorStateClass.TOTAL
    _attr_suggested_display_precision = 2

    def __init__(self, coordinator: QuiltCoordinator, room_id: str) -> None:
        super().__init__(coordinator)
        self._room_id = room_id
        self._attr_unique_id = f"quilt_{room_id}_energy_today"
        self._attr_device_info = room_device(coordinator, room_id)

    @property
    def available(self) -> bool:
        # Metered by Quilt's cloud, not live telemetry: stays valid through a
        # poll or stream outage once read.
        return self._room_id in self.coordinator.energy

    @property
    def native_value(self) -> float | None:
        kwh = self.coordinator.energy.get(self._room_id)
        return round(kwh, 3) if kwh is not None else None

    @property
    def last_reset(self) -> datetime | None:
        return self.coordinator.energy_last_reset


class QuiltDialSensor(QuiltEntity, SensorEntity):
    """A single sensor channel on the Quilt wall Dial."""

    entity_description: QuiltDialSensorDescription

    def __init__(
        self, coordinator: QuiltCoordinator, description: QuiltDialSensorDescription
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"quilt_dial_{description.key}"
        # Quilt's API names the dial after its serial ("Dial QD1-0B000VG2S");
        # use a friendly device name and keep the serial as serial_number.
        raw = coordinator.data["dial"].get("name") or ""
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, DIAL_DEVICE_ID)},
            name="Quilt Dial",
            manufacturer="Quilt",
            model="Dial",
            serial_number=raw.removeprefix("Dial ").strip() or None,
        )

    @property
    def native_value(self) -> float | int | None:
        dial = self.coordinator.data.get("dial") or {}
        return self.entity_description.value_fn(dial)
