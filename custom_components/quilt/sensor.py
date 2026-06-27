"""Quilt sensors: per-room humidity (from the indoor head) and the dial.

The space-level humidity field reads 0; the real per-room value comes off the
indoor head unit. The wall Dial additionally reports its own temperature,
humidity, and three ambient channels whose exact meaning is not yet confirmed
(likely air-quality / illuminance) — those are exposed as diagnostic sensors so
their readings can be correlated with real-world conditions to identify them.
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
from homeassistant.const import PERCENTAGE, EntityCategory, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import QuiltCoordinator

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
    QuiltDialSensorDescription(
        key="humidity",
        device_class=SensorDeviceClass.HUMIDITY,
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        value_fn=lambda d: d.get("humidity"),
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
    """Set up Quilt room-humidity and dial sensors."""
    coordinator: QuiltCoordinator = entry.runtime_data
    entities: list[SensorEntity] = [
        QuiltRoomHumidity(coordinator, room_id)
        for room_id, room in coordinator.data["rooms"].items()
        if room.get("humidity") is not None
    ]
    if coordinator.data.get("dial"):
        entities.extend(
            QuiltDialSensor(coordinator, desc) for desc in DIAL_SENSORS
        )
    async_add_entities(entities)


class QuiltRoomHumidity(CoordinatorEntity[QuiltCoordinator], SensorEntity):
    """Per-room relative humidity measured by the indoor head."""

    _attr_has_entity_name = True
    _attr_name = "Humidity"
    _attr_device_class = SensorDeviceClass.HUMIDITY
    _attr_native_unit_of_measurement = PERCENTAGE
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, coordinator: QuiltCoordinator, room_id: str) -> None:
        super().__init__(coordinator)
        self._room_id = room_id
        self._attr_unique_id = f"quilt_{room_id}_humidity"
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, room_id)})

    @property
    def native_value(self) -> int | None:
        return self.coordinator.data["rooms"][self._room_id].get("humidity")


class QuiltDialSensor(CoordinatorEntity[QuiltCoordinator], SensorEntity):
    """A single sensor channel on the Quilt wall Dial."""

    _attr_has_entity_name = True
    entity_description: QuiltDialSensorDescription

    def __init__(
        self, coordinator: QuiltCoordinator, description: QuiltDialSensorDescription
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"quilt_dial_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, DIAL_DEVICE_ID)},
            name=coordinator.data["dial"].get("name") or "Quilt Dial",
            manufacturer="Quilt",
            model="Dial",
        )

    @property
    def native_value(self) -> float | int | None:
        dial = self.coordinator.data.get("dial") or {}
        return self.entity_description.value_fn(dial)
