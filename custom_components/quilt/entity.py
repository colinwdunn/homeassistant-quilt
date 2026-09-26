"""Base entity for Quilt."""
from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import QuiltCoordinator


def room_device(coordinator: QuiltCoordinator, room_id: str) -> DeviceInfo:
    """The room's device, named by every entity that attaches to it.

    Platforms set up concurrently; if a sensor registered the device first with
    only its identifier, the device (and so the entity ids) would be nameless.
    """
    return DeviceInfo(
        identifiers={(DOMAIN, room_id)},
        name=coordinator.data["rooms"][room_id].get("name"),
        manufacturer="Quilt",
        model="Heat Pump",
    )


class QuiltEntity(CoordinatorEntity[QuiltCoordinator]):
    """Stays available while either the poll or the push stream is working.

    One failed poll would otherwise mark every Quilt entity unavailable (and
    the HomeKit tiles "No Response") until the next poll, even while pushed
    updates keep arriving.
    """

    _attr_has_entity_name = True

    @property
    def available(self) -> bool:
        return super().available or self.coordinator.push_healthy
