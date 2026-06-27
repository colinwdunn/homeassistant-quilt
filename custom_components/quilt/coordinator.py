"""Data update coordinator for Quilt."""
from __future__ import annotations

from datetime import timedelta
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import CognitoAuth, QuiltAuthError, QuiltClient
from .const import CONF_REFRESH_TOKEN, CONF_SYSTEM_ID, DEFAULT_SCAN_INTERVAL, DOMAIN

_LOGGER = logging.getLogger(__name__)


class QuiltCoordinator(DataUpdateCoordinator[dict[str, dict]]):
    """Polls Quilt's cloud and exposes rooms keyed by id."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL),
        )
        self.entry = entry
        auth = CognitoAuth(entry.data[CONF_REFRESH_TOKEN])
        self.client = QuiltClient(auth, entry.data[CONF_SYSTEM_ID])

    async def _async_update_data(self) -> dict[str, dict]:
        try:
            rooms = await self.hass.async_add_executor_job(self.client.get_rooms)
        except QuiltAuthError as err:
            # A revoked/expired refresh token needs the user to re-login.
            if "revoked" in str(err).lower() or "NotAuthorized" in str(err):
                raise ConfigEntryAuthFailed(str(err)) from err
            raise UpdateFailed(str(err)) from err
        return {room["id"]: room for room in rooms}
