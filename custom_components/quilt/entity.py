"""Base entity for Quilt."""
from __future__ import annotations

from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .coordinator import QuiltCoordinator


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
