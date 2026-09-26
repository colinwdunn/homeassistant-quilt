"""Regression checks for the owner's real setup (v0.4.0 -> v0.5.0 upgrade).

- Entity ids / unique ids / device identifiers the owner's automations and the
  Apple Home bridge are bound to survive the upgrade unchanged.
- Availability: v0.5.0 keeps entities available while the push stream is
  healthy. After a revoked login the coordinator stops polling, so once the
  stream also goes away nothing re-evaluates availability.
- Quilt fallback modes (6/7) used to surface as climate state "unknown".
"""
from __future__ import annotations

from datetime import timedelta

import pytest

from homeassistant.components.climate import (
    ATTR_HVAC_MODE,
    DOMAIN as CLIMATE_DOMAIN,
    SERVICE_SET_TEMPERATURE,
    HVACMode,
)
from homeassistant.config_entries import SOURCE_REAUTH, ConfigEntryState
from homeassistant.const import ATTR_ENTITY_ID, ATTR_TEMPERATURE, STATE_UNAVAILABLE
from homeassistant.helpers import device_registry as dr, entity_registry as er
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from custom_components.quilt import api
from custom_components.quilt.const import DOMAIN

from .conftest import BEDROOM, DINING, LIVING

# What the owner's registry holds after running v0.4.0 (bedroom climate renamed).
OWNER_IDS = {
    ("climate", f"quilt_{DINING}"): "climate.dining_room",
    ("climate", f"quilt_{LIVING}"): "climate.living_room",
    ("climate", f"quilt_{BEDROOM}"): "climate.primary_bedroom_quilt",
    ("binary_sensor", f"quilt_{DINING}_occupancy"): "binary_sensor.dining_room_occupancy",
    ("binary_sensor", f"quilt_{LIVING}_occupancy"): "binary_sensor.living_room_occupancy",
    ("binary_sensor", f"quilt_{BEDROOM}_occupancy"): "binary_sensor.primary_bedroom_occupancy",
    ("sensor", f"quilt_{DINING}_humidity"): "sensor.dining_room_humidity",
    ("sensor", f"quilt_{LIVING}_humidity"): "sensor.living_room_humidity",
    ("sensor", f"quilt_{BEDROOM}_humidity"): "sensor.primary_bedroom_humidity",
    ("sensor", "quilt_dial_temperature"): "sensor.quilt_dial_temperature",
    ("sensor", "quilt_dial_ambient_1"): "sensor.quilt_dial_ambient_1",
    ("sensor", "quilt_dial_ambient_2"): "sensor.quilt_dial_ambient_2",
    ("sensor", "quilt_dial_ambient_3"): "sensor.quilt_dial_ambient_3",
}
ROOM_NAMES = {DINING: "Dining Room", LIVING: "Living Room", BEDROOM: "Primary Bedroom"}


def _require(condition: bool, message: str) -> None:
    """Precondition for a strict-xfail test: a normal failure, not an AssertionError."""
    if not condition:
        pytest.fail(f"precondition failed: {message}")


@pytest.fixture(autouse=True)
async def _unload_after(hass, config_entry):
    yield
    if config_entry.state is ConfigEntryState.LOADED:
        await hass.config_entries.async_unload(config_entry.entry_id)
        await hass.async_block_till_done()


def _seed_owner_registry(hass, config_entry) -> dict[str, str]:
    """Registry + devices as v0.4.0 left them. Returns device ids by identifier."""
    devices = dr.async_get(hass)
    entities = er.async_get(hass)
    device_ids = {}
    for room_id, name in ROOM_NAMES.items():
        dev = devices.async_get_or_create(
            config_entry_id=config_entry.entry_id, identifiers={(DOMAIN, room_id)},
            name=name, manufacturer="Quilt", model="Heat Pump",
        )
        device_ids[room_id] = dev.id
    dial = devices.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={(DOMAIN, "dial")},
        name="Quilt Dial", manufacturer="Quilt", model="Dial",
    )
    device_ids["dial"] = dial.id
    for (platform, unique_id), entity_id in OWNER_IDS.items():
        entry = entities.async_get_or_create(
            platform, DOMAIN, unique_id, config_entry=config_entry,
            suggested_object_id=entity_id.split(".", 1)[1],
        )
        if entry.entity_id != entity_id:
            entities.async_update_entity(entry.entity_id, new_entity_id=entity_id)
        if unique_id.startswith("quilt_dial_ambient"):
            entities.async_update_entity(entity_id, disabled_by=er.RegistryEntryDisabler.INTEGRATION)
    entities.async_get_or_create(
        "sensor", DOMAIN, "quilt_dial_humidity", config_entry=config_entry,
        suggested_object_id="quilt_dial_humidity",
    )
    return device_ids


async def test_owner_entity_ids_and_devices_survive_upgrade(
    hass, config_entry, mock_client, fake_stream
):
    config_entry.add_to_hass(hass)
    device_ids = _seed_owner_registry(hass, config_entry)

    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done()

    entities = er.async_get(hass)
    for (platform, unique_id), entity_id in OWNER_IDS.items():
        assert entities.async_get_entity_id(platform, DOMAIN, unique_id) == entity_id
    for entity_id in ("climate.dining_room", "climate.living_room",
                      "climate.primary_bedroom_quilt"):
        state = hass.states.get(entity_id)
        assert state is not None and state.state != STATE_UNAVAILABLE, entity_id
    # Disabled ambient sensors stay disabled; the retired dial humidity is gone.
    assert entities.async_get("sensor.quilt_dial_ambient_1").disabled_by is not None
    assert entities.async_get_entity_id("sensor", DOMAIN, "quilt_dial_humidity") is None
    # Exactly the owner's entities plus one energy sensor per room; no new devices.
    energy_ids = {f"sensor.{n.lower().replace(' ', '_')}_energy_today" for n in ROOM_NAMES.values()}
    assert {
        e.entity_id for e in er.async_entries_for_config_entry(entities, config_entry.entry_id)
    } == set(OWNER_IDS.values()) | energy_ids
    devices = dr.async_get(hass)
    assert {d.id for d in dr.async_entries_for_config_entry(devices, config_entry.entry_id)} == set(
        device_ids.values()
    )
    # New energy sensors land on the existing room devices with room-based ids.
    for room_id, name in ROOM_NAMES.items():
        entity_id = entities.async_get_entity_id("sensor", DOMAIN, f"quilt_{room_id}_energy_today")
        assert entity_id == f"sensor.{name.lower().replace(' ', '_')}_energy_today"
        assert entities.async_get(entity_id).device_id == device_ids[room_id]


async def test_owner_sync_call_shape_still_works(hass, config_entry, mock_client, fake_stream):
    """set_temperature(hvac_mode=cool, temperature=X) then set_hvac_mode off, as the syncs do."""
    config_entry.add_to_hass(hass)
    _seed_owner_registry(hass, config_entry)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done()

    await hass.services.async_call(
        CLIMATE_DOMAIN, SERVICE_SET_TEMPERATURE,
        {ATTR_ENTITY_ID: "climate.dining_room", ATTR_HVAC_MODE: HVACMode.COOL,
         ATTR_TEMPERATURE: 23.0},
        blocking=True,
    )
    state = hass.states.get("climate.dining_room")
    assert state.state == HVACMode.COOL
    assert state.attributes[ATTR_TEMPERATURE] == 23.0
    mock_client.set_setpoints.assert_called_with(DINING, heat=16.0, cool=23.0, mode=api.MODE_COOL)

    await hass.services.async_call(
        CLIMATE_DOMAIN, "set_hvac_mode",
        {ATTR_ENTITY_ID: "climate.dining_room", ATTR_HVAC_MODE: HVACMode.OFF}, blocking=True,
    )
    assert hass.states.get("climate.dining_room").state == HVACMode.OFF


# --- availability after a revoked login -----------------------------------------------
REVOKED = "NotAuthorizedException: Refresh Token has been revoked"


async def test_revoked_login_with_dead_stream_goes_unavailable(
    hass, setup_integration, mock_client, fake_stream
):
    """Control: stream already unhealthy when the login is rejected -> unavailable (as v0.4)."""
    coordinator = setup_integration.runtime_data
    fake_stream.instance.healthy = False
    mock_client.get_system.side_effect = api.QuiltAuthError(REVOKED)
    await coordinator.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get("climate.primary_bedroom").state == STATE_UNAVAILABLE


async def test_revoked_login_then_stream_loss_goes_unavailable(
    hass, setup_integration, mock_client, fake_stream
):
    coordinator = setup_integration.runtime_data
    fake_stream.instance.healthy = True  # stream opened with a still-valid IdToken
    mock_client.get_system.side_effect = api.QuiltAuthError(REVOKED)
    await coordinator.async_refresh()
    await hass.async_block_till_done()
    flows = hass.config_entries.flow.async_progress()
    _require([f["context"]["source"] for f in flows] == [SOURCE_REAUTH], "reauth started")
    _require(hass.states.get("climate.primary_bedroom").state == HVACMode.COOL,
             "healthy stream keeps the room available")

    # The stream ends (Quilt reconnect request / network blip); its reconnect needs a
    # new IdToken, the refresh token is revoked, so it parks. The real stream calls
    # on_disconnect when a connected stream ends; the coordinator re-checks
    # availability then (formerly a strict xfail: nothing re-checked it). Time passes.
    fake_stream.instance.healthy = False
    fake_stream.instance.on_disconnect()
    await hass.async_block_till_done()
    polls = mock_client.get_system.call_count
    for _ in range(10):
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=10))
        await hass.async_block_till_done()
    _require(mock_client.get_system.call_count == polls, "no polls after auth failure")

    # What an owner sync sees: a write aimed at the "available" room is dropped silently.
    await hass.services.async_call(
        CLIMATE_DOMAIN, SERVICE_SET_TEMPERATURE,
        {ATTR_ENTITY_ID: "climate.primary_bedroom", ATTR_HVAC_MODE: HVACMode.COOL,
         ATTR_TEMPERATURE: 22.0},
        blocking=True,
    )
    _require(mock_client.set_setpoints.call_count == 0, "write skipped as unavailable")

    assert hass.states.get("climate.primary_bedroom").state == STATE_UNAVAILABLE


# --- fallback modes as the owner's automations see them ---------------------------------
async def test_fallback_modes_are_no_longer_unknown(
    hass, system, config_entry, mock_client, fake_stream
):
    """v0.4.0 showed modes 6/7 as 'unknown' (ignored by the syncs); v0.5.0 shows real modes."""
    system["rooms"][LIVING].update(
        mode=api.MODE_FALLBACK_AUTO, on=True, heat_setpoint=20.0, cool_setpoint=24.0,
    )
    system["rooms"][DINING].update(mode=api.MODE_FALLBACK_OFF, on=False)
    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done()

    living = hass.states.get("climate.living_room")
    assert living.state == HVACMode.HEAT_COOL
    assert living.attributes[ATTR_TEMPERATURE] is None  # a temperature compare has nothing
    assert hass.states.get("climate.dining_room").state == HVACMode.OFF
