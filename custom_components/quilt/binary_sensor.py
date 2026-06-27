"""Occupancy binary sensors for Quilt rooms.

Each indoor head unit has its own presence sensor; Quilt reports it per room
(this is what drives the cloud's auto-away). We surface it as an occupancy
binary sensor so it can be used in Home Assistant automations directly.
"""
from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import QuiltCoordinator


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up Quilt occupancy sensors for rooms whose head reports presence."""
    coordinator: QuiltCoordinator = entry.runtime_data
    async_add_entities(
        QuiltOccupancy(coordinator, room_id)
        for room_id, room in coordinator.data["rooms"].items()
        if room.get("occupied") is not None
    )


class QuiltOccupancy(CoordinatorEntity[QuiltCoordinator], BinarySensorEntity):
    """Per-room presence as reported by the indoor head unit."""

    _attr_has_entity_name = True
    _attr_name = "Occupancy"
    _attr_device_class = BinarySensorDeviceClass.OCCUPANCY

    def __init__(self, coordinator: QuiltCoordinator, room_id: str) -> None:
        super().__init__(coordinator)
        self._room_id = room_id
        self._attr_unique_id = f"quilt_{room_id}_occupancy"
        # Attach to the same device as the room's climate entity.
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, room_id)})

    @property
    def is_on(self) -> bool | None:
        return self.coordinator.data["rooms"][self._room_id].get("occupied")
