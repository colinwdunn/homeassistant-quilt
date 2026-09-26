"""Setup, unload, and entity inventory."""
from __future__ import annotations

from homeassistant.config_entries import ConfigEntryState
from homeassistant.helpers import entity_registry as er

from custom_components.quilt.const import DOMAIN

from .conftest import BEDROOM, DIAL_ID, DINING, LIVING


async def test_setup_creates_entities_and_starts_push(hass, setup_integration, fake_stream):
    entry = setup_integration
    assert entry.state is ConfigEntryState.LOADED
    assert fake_stream.instance.started

    registry = er.async_get(hass)
    unique_ids = {
        e.unique_id for e in er.async_entries_for_config_entry(registry, entry.entry_id)
    }
    for room in (DINING, LIVING, BEDROOM):
        assert f"quilt_{room}" in unique_ids  # climate
        assert f"quilt_{room}_occupancy" in unique_ids
        assert f"quilt_{room}_humidity" in unique_ids
        assert f"quilt_{room}_energy_today" in unique_ids
    assert "quilt_dial_temperature" in unique_ids
    assert "quilt_dial_humidity" not in unique_ids

    # The stream follows every room, every indoor unit, and the Dial.
    topics = fake_stream.instance.topics_fn()
    assert f"hds/space/{DINING}" in topics
    assert f"hds/indoor_unit/unit-{BEDROOM}" in topics
    assert f"hds/controller/{DIAL_ID}" in topics


async def test_retired_dial_humidity_is_removed(hass, config_entry, mock_client, fake_stream):
    registry = er.async_get(hass)
    config_entry.add_to_hass(hass)
    registry.async_get_or_create(
        "sensor", DOMAIN, "quilt_dial_humidity", config_entry=config_entry,
        suggested_object_id="quilt_dial_humidity",
    )
    assert registry.async_get_entity_id("sensor", DOMAIN, "quilt_dial_humidity")

    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done()

    assert registry.async_get_entity_id("sensor", DOMAIN, "quilt_dial_humidity") is None


async def test_unload_stops_push_then_closes_client(hass, setup_integration, mock_client, fake_stream):
    entry = setup_integration
    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED
    assert fake_stream.instance.stopped
    mock_client.close.assert_called_once()
