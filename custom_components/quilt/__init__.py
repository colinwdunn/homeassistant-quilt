"""The Quilt integration."""
from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .coordinator import QuiltCoordinator

PLATFORMS: list[Platform] = [Platform.CLIMATE]

type QuiltConfigEntry = ConfigEntry[QuiltCoordinator]


async def async_setup_entry(hass: HomeAssistant, entry: QuiltConfigEntry) -> bool:
    """Set up Quilt from a config entry."""
    coordinator = QuiltCoordinator(hass, entry)
    await coordinator.async_config_entry_first_refresh()
    entry.runtime_data = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: QuiltConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        await hass.async_add_executor_job(entry.runtime_data.client.close)
    return unload_ok
