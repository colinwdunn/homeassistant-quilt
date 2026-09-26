"""The Quilt integration."""
from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from .const import DOMAIN
from .coordinator import QuiltCoordinator, scan_interval

PLATFORMS: list[Platform] = [
    Platform.CLIMATE,
    Platform.BINARY_SENSOR,
    Platform.SENSOR,
]

type QuiltConfigEntry = ConfigEntry[QuiltCoordinator]


async def async_setup_entry(hass: HomeAssistant, entry: QuiltConfigEntry) -> bool:
    """Set up Quilt from a config entry."""
    _remove_retired_entities(hass)
    coordinator = QuiltCoordinator(hass, entry)
    await coordinator.async_config_entry_first_refresh()
    entry.runtime_data = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    entry.async_on_unload(coordinator.async_stop_push)
    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, coordinator.async_stop_push)
    )
    if not hass.is_stopping:
        coordinator.start_push()
    return True


def _remove_retired_entities(hass: HomeAssistant) -> None:
    """Drop entities earlier versions created that turned out to be wrong."""
    registry = er.async_get(hass)
    # v0.2-0.4 "Dial humidity" was really a circuit-board temperature.
    if entity_id := registry.async_get_entity_id("sensor", DOMAIN, "quilt_dial_humidity"):
        registry.async_remove(entity_id)


async def _async_options_updated(hass: HomeAssistant, entry: QuiltConfigEntry) -> None:
    coordinator = entry.runtime_data
    coordinator.update_interval = scan_interval(entry)
    await coordinator.async_request_refresh()


async def async_unload_entry(hass: HomeAssistant, entry: QuiltConfigEntry) -> bool:
    """Unload a config entry."""
    coordinator = entry.runtime_data
    # Stop the stream before anything it calls back into goes away. The
    # on_unload hooks run only after this returns.
    await coordinator.async_stop_push()
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        await hass.async_add_executor_job(coordinator.client.close)
    return unload_ok
