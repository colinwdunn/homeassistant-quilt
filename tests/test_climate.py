"""Climate entities, driven only through Home Assistant services and states.

The Quilt cloud client is the MagicMock from conftest. Two fixtures give its
writes more realistic behaviour when a test needs it:

- `cloud` makes the fake client's writes follow QuiltClient's rules
  (set_setpoints with no mode leaves an off room off, set_active resumes the
  Active preset's mode, ...) and land in the `system` data so the next poll
  sees them.
- `wire` runs the writes through the real QuiltClient methods onto a fake gRPC
  stub, so a test can check the UpdateSpace / UpdateComfortSetting requests.

Pushes are delivered through the FakeStream's on_events callback, as the
stream thread would.
"""
from __future__ import annotations

import asyncio
import copy
from datetime import timedelta
import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest
import voluptuous as vol

from homeassistant.components.climate import (
    ATTR_CURRENT_TEMPERATURE,
    ATTR_HVAC_ACTION,
    ATTR_HVAC_MODE,
    ATTR_HVAC_MODES,
    ATTR_PRESET_MODE,
    ATTR_PRESET_MODES,
    ATTR_TARGET_TEMP_HIGH,
    ATTR_TARGET_TEMP_LOW,
    DOMAIN as CLIMATE_DOMAIN,
    SERVICE_SET_HVAC_MODE,
    SERVICE_SET_PRESET_MODE,
    SERVICE_SET_TEMPERATURE,
    HVACAction,
    HVACMode,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import (
    ATTR_ENTITY_ID,
    ATTR_TEMPERATURE,
    EVENT_STATE_CHANGED,
    SERVICE_TURN_OFF,
    SERVICE_TURN_ON,
)
from homeassistant.core import callback
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import entity_registry as er
from homeassistant.util.unit_system import US_CUSTOMARY_SYSTEM
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from custom_components.quilt import api
from custom_components.quilt.const import WRITE_HOLD_SECONDS

from .conftest import BEDROOM, DINING, LIVING, SYSTEM_ID

DINING_ENTITY = "climate.dining_room"
LIVING_ENTITY = "climate.living_room"
BEDROOM_ENTITY = "climate.primary_bedroom"

# The Active preset every fake room starts with (conftest._presets).
ACTIVE_HEAT, ACTIVE_COOL = 16.0, 24.0


def f_to_c(f: float) -> float:
    return (f - 32) * 5 / 9


async def _setup(hass, config_entry) -> None:
    """Set the integration up after the test has shaped `system`."""
    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done()


async def _call(hass, service: str, entity_id: str, **data: Any) -> None:
    await hass.services.async_call(
        CLIMATE_DOMAIN, service, {ATTR_ENTITY_ID: entity_id, **data}, blocking=True
    )


def _state(hass, entity_id: str):
    state = hass.states.get(entity_id)
    assert state is not None, entity_id
    return state


async def _push(hass, fake_stream, *events: dict) -> None:
    """Deliver events the way the stream thread does."""
    fake_stream.instance.on_events(list(events))
    await hass.async_block_till_done()


async def _expire_hold(hass, freezer) -> None:
    """Let the post-write hold run out; the entity then re-reads Quilt."""
    freezer.tick(timedelta(seconds=WRITE_HOLD_SECONDS + 1))
    async_fire_time_changed(hass)
    await hass.async_block_till_done()


@pytest.fixture(autouse=True)
async def _unload_after(hass, config_entry):
    """Unload at teardown so hold timers and debouncers are cancelled."""
    yield
    if config_entry.state is ConfigEntryState.LOADED:
        await hass.config_entries.async_unload(config_entry.entry_id)
        await hass.async_block_till_done()


@pytest.fixture
def cloud(system, mock_client):
    """Make the fake client's writes follow QuiltClient's rules and land in `system`."""

    def room(room_id: str) -> dict:
        return system["rooms"][room_id]

    def set_active(room_id: str, on: bool) -> int:
        r = room(room_id)
        preset = r["presets"]["Active" if on else "Off"]
        mode = api.QuiltClient.resume_mode(r) if on else api.MODE_OFF
        r.update(mode=mode, on=on, heat_setpoint=preset["heat"],
                 cool_setpoint=preset["cool"], active_comfort_id=preset["id"])
        return mode

    def set_setpoints(room_id, heat=None, cool=None, mode=None) -> int:
        r = room(room_id)
        ap = r["presets"]["Active"]
        if mode is not None and (mode not in api.KNOWN_MODES or mode == api.MODE_OFF):
            raise ValueError(f"not an active Quilt mode: {mode}")
        if mode is None:
            mode = api.QuiltClient.current_writable_mode(r)
            if mode is None and r["on"]:  # an unknown mode we can't write back
                mode = api.QuiltClient.resume_mode(r)
        apply = mode is not None
        if mode is None:
            mode = api.MODE_OFF
        ap["heat"] = heat if heat is not None else ap["heat"]
        ap["cool"] = cool if cool is not None else ap["cool"]
        if mode != api.MODE_OFF:
            ap["mode"] = mode
        if not apply:
            return api.MODE_OFF
        r.update(mode=mode, on=True, heat_setpoint=ap["heat"],
                 cool_setpoint=ap["cool"], active_comfort_id=ap["id"])
        return mode

    def set_preset(room_id: str, name: str) -> int:
        r = room(room_id)
        preset = r["presets"][name]
        if name.lower() == "off":
            mode = api.MODE_OFF
        else:
            mode = (api.QuiltClient.current_writable_mode(r)
                    or api.QuiltClient.resume_mode(r))
        r.update(mode=mode, on=mode != api.MODE_OFF, heat_setpoint=preset["heat"],
                 cool_setpoint=preset["cool"], active_comfort_id=preset["id"])
        return mode

    mock_client.set_active.side_effect = set_active
    mock_client.set_setpoints.side_effect = set_setpoints
    mock_client.set_preset.side_effect = set_preset
    return mock_client


@pytest.fixture
def wire(system, mock_client):
    """Run writes through the real QuiltClient methods onto a fake gRPC stub.

    Returns the stub, so tests can read the UpdateSpace / UpdateComfortSetting
    requests that would have gone to Quilt.
    """
    client = object.__new__(api.QuiltClient)
    client._system_id = SYSTEM_ID
    client._stub = MagicMock(name="HomeDatastoreServiceStub")
    client._meta = lambda: ()

    def fresh_room(room_id: str) -> dict:
        src = system["rooms"][room_id]
        room = copy.deepcopy({k: v for k, v in src.items() if k != "presets"})
        room["space_updated"] = api.pb.Timestamp(seconds=1)
        room["presets"] = {
            name: {
                **p,
                "meta_updated": api.pb.Timestamp(seconds=1),
                "value": api.pb.ComfortValue(
                    name=name, heat_setpoint=p["heat"], cool_setpoint=p["cool"], f8=p["mode"]
                ),
            }
            for name, p in src["presets"].items()
        }
        return room

    client._fresh_room = fresh_room
    mock_client.set_setpoints.side_effect = client.set_setpoints
    mock_client.set_active.side_effect = client.set_active
    mock_client.set_preset.side_effect = client.set_preset
    return client._stub


def _space_writes(stub) -> list:
    return [c.args[0].update.value for c in stub.UpdateSpace.call_args_list]


def _preset_writes(stub) -> list:
    return [c.args[0].update.value for c in stub.UpdateComfortSetting.call_args_list]


# --- entities ------------------------------------------------------------


async def test_one_climate_entity_per_room(hass, setup_integration):
    registry = er.async_get(hass)
    for entity_id, room_id in (
        (DINING_ENTITY, DINING),
        (LIVING_ENTITY, LIVING),
        (BEDROOM_ENTITY, BEDROOM),
    ):
        entry = registry.async_get(entity_id)
        assert entry is not None, entity_id
        assert entry.unique_id == f"quilt_{room_id}"
    climate_ids = sorted(hass.states.async_entity_ids(CLIMATE_DOMAIN))
    assert climate_ids == sorted([DINING_ENTITY, LIVING_ENTITY, BEDROOM_ENTITY])


async def test_initial_states(hass, setup_integration):
    dining = _state(hass, DINING_ENTITY)
    assert dining.state == HVACMode.OFF
    assert dining.attributes[ATTR_HVAC_ACTION] == HVACAction.OFF
    assert dining.attributes[ATTR_CURRENT_TEMPERATURE] == 24.0
    assert dining.attributes[ATTR_TEMPERATURE] is None

    bedroom = _state(hass, BEDROOM_ENTITY)
    assert bedroom.state == HVACMode.COOL
    assert bedroom.attributes[ATTR_HVAC_ACTION] == HVACAction.COOLING
    assert bedroom.attributes[ATTR_TEMPERATURE] == 24.0
    assert bedroom.attributes[ATTR_CURRENT_TEMPERATURE] == 24.5


async def test_hvac_modes_are_exactly_quilts(hass, setup_integration):
    # HomeKit builds its mode picker from this list; order and contents matter.
    for entity_id in (DINING_ENTITY, LIVING_ENTITY, BEDROOM_ENTITY):
        assert _state(hass, entity_id).attributes[ATTR_HVAC_MODES] == [
            HVACMode.OFF,
            HVACMode.COOL,
            HVACMode.HEAT,
            HVACMode.HEAT_COOL,
            HVACMode.FAN_ONLY,
            HVACMode.DRY,
        ]


async def test_preset_modes_and_current_preset(hass, setup_integration):
    bedroom = _state(hass, BEDROOM_ENTITY)
    # "Off" is HVACMode.OFF, not a preset.
    assert bedroom.attributes[ATTR_PRESET_MODES] == ["Active", "Sleep", "Eco"]
    assert bedroom.attributes[ATTR_PRESET_MODE] == "Active"
    # A room that is off has no active preset.
    assert _state(hass, DINING_ENTITY).attributes[ATTR_PRESET_MODE] is None


async def test_set_preset_mode(hass, setup_integration, mock_client):
    await _call(hass, SERVICE_SET_PRESET_MODE, BEDROOM_ENTITY, **{ATTR_PRESET_MODE: "Eco"})
    mock_client.set_preset.assert_called_once_with(BEDROOM, "Eco")
    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_PRESET_MODE] == "Eco"


@pytest.mark.parametrize("preset", ["Off", "Vacation"])
async def test_set_preset_mode_rejects_unknown_preset(hass, setup_integration, mock_client, preset):
    with pytest.raises(ServiceValidationError):
        await _call(hass, SERVICE_SET_PRESET_MODE, BEDROOM_ENTITY, **{ATTR_PRESET_MODE: preset})
    mock_client.set_preset.assert_not_called()


# --- set_temperature with hvac_mode (the Ecobee sync automations) -----------


async def test_set_temperature_with_cool_mode_turns_off_room_to_cool(
    hass, setup_integration, mock_client
):
    assert _state(hass, DINING_ENTITY).state == HVACMode.OFF

    await _call(hass, SERVICE_SET_TEMPERATURE, DINING_ENTITY,
                **{ATTR_HVAC_MODE: HVACMode.COOL, ATTR_TEMPERATURE: 22.5})

    # One write carries both the setpoint and the mode; no separate turn-on
    # that could resume some other mode first.
    mock_client.set_setpoints.assert_called_once_with(
        DINING, heat=ACTIVE_HEAT, cool=22.5, mode=api.MODE_COOL
    )
    mock_client.set_active.assert_not_called()
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.COOL
    assert state.attributes[ATTR_TEMPERATURE] == 22.5


@pytest.mark.parametrize("fahrenheit", [72, 75, 68.5])
async def test_set_temperature_with_cool_mode_in_fahrenheit(
    hass, setup_integration, mock_client, fahrenheit
):
    hass.config.units = US_CUSTOMARY_SYSTEM

    await _call(hass, SERVICE_SET_TEMPERATURE, DINING_ENTITY,
                **{ATTR_HVAC_MODE: HVACMode.COOL, ATTR_TEMPERATURE: fahrenheit})

    mock_client.set_setpoints.assert_called_once()
    args, kwargs = mock_client.set_setpoints.call_args
    assert args == (DINING,)
    assert kwargs["mode"] == api.MODE_COOL
    assert kwargs["cool"] == pytest.approx(f_to_c(fahrenheit))
    assert kwargs["heat"] == ACTIVE_HEAT  # untouched, still in °C
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.COOL
    # Shown back in the user's unit.
    assert state.attributes[ATTR_TEMPERATURE] == pytest.approx(fahrenheit, abs=0.5)


async def test_set_temperature_with_cool_mode_survives_the_next_poll(
    hass, setup_integration, cloud, freezer
):
    """After the hold, the room reads back as cooling to the requested target."""
    await _call(hass, SERVICE_SET_TEMPERATURE, DINING_ENTITY,
                **{ATTR_HVAC_MODE: HVACMode.COOL, ATTR_TEMPERATURE: 22.5})
    await _expire_hold(hass, freezer)

    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.COOL
    assert state.attributes[ATTR_TEMPERATURE] == 22.5


async def test_set_temperature_with_heat_mode_on_off_room(hass, setup_integration, mock_client):
    await _call(hass, SERVICE_SET_TEMPERATURE, DINING_ENTITY,
                **{ATTR_HVAC_MODE: HVACMode.HEAT, ATTR_TEMPERATURE: 21.0})
    mock_client.set_setpoints.assert_called_once_with(
        DINING, heat=21.0, cool=ACTIVE_COOL, mode=api.MODE_HEAT
    )
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.HEAT
    assert state.attributes[ATTR_TEMPERATURE] == 21.0


async def test_set_temperature_with_cool_mode_on_heating_room_targets_cool(
    hass, system, config_entry, mock_client, fake_stream
):
    system["rooms"][LIVING].update(
        mode=api.MODE_HEAT, on=True, heat_setpoint=21.0, cool_setpoint=26.0,
        active_comfort_id=f"{LIVING}-active",
    )
    await _setup(hass, config_entry)

    await _call(hass, SERVICE_SET_TEMPERATURE, LIVING_ENTITY,
                **{ATTR_HVAC_MODE: HVACMode.COOL, ATTR_TEMPERATURE: 23.0})

    mock_client.set_setpoints.assert_called_once_with(
        LIVING, heat=21.0, cool=23.0, mode=api.MODE_COOL
    )
    assert _state(hass, LIVING_ENTITY).state == HVACMode.COOL


async def test_set_temperature_with_unsupported_mode_is_rejected(
    hass, setup_integration, mock_client
):
    # HA's schema accepts "auto" (it is an HVACMode); the entity must refuse it.
    with pytest.raises(ServiceValidationError):
        await _call(hass, SERVICE_SET_TEMPERATURE, BEDROOM_ENTITY,
                    **{ATTR_HVAC_MODE: HVACMode.AUTO, ATTR_TEMPERATURE: 22.0})
    mock_client.set_setpoints.assert_not_called()
    mock_client.set_active.assert_not_called()
    # The rejected call didn't leak its temperature into the entity.
    state = _state(hass, BEDROOM_ENTITY)
    assert state.state == HVACMode.COOL
    assert state.attributes[ATTR_TEMPERATURE] == 24.0

    # A later plain setpoint change still starts from the old 24 degC target.
    await _call(hass, SERVICE_SET_TEMPERATURE, BEDROOM_ENTITY,
                **{ATTR_TARGET_TEMP_LOW: 17.0, ATTR_TARGET_TEMP_HIGH: 24.0})
    mock_client.set_setpoints.assert_called_once_with(BEDROOM, heat=17.0, cool=24.0)


async def test_set_temperature_with_unknown_mode_string_fails_schema(
    hass, setup_integration, mock_client
):
    # Not an HVACMode at all: HA's service schema rejects it before the entity.
    with pytest.raises(vol.Invalid):
        await _call(hass, SERVICE_SET_TEMPERATURE, BEDROOM_ENTITY,
                    **{ATTR_HVAC_MODE: "banana", ATTR_TEMPERATURE: 22.0})
    mock_client.set_setpoints.assert_not_called()


async def test_set_temperature_range_with_heat_cool_mode(hass, setup_integration, mock_client):
    await _call(hass, SERVICE_SET_TEMPERATURE, DINING_ENTITY,
                **{ATTR_HVAC_MODE: HVACMode.HEAT_COOL,
                   ATTR_TARGET_TEMP_LOW: 19.0, ATTR_TARGET_TEMP_HIGH: 25.0})
    mock_client.set_setpoints.assert_called_once_with(
        DINING, heat=19.0, cool=25.0, mode=api.MODE_HEAT_COOL
    )
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.HEAT_COOL
    assert state.attributes[ATTR_TEMPERATURE] is None
    assert state.attributes[ATTR_TARGET_TEMP_LOW] == 19.0
    assert state.attributes[ATTR_TARGET_TEMP_HIGH] == 25.0


# --- set_temperature without hvac_mode --------------------------------------


async def test_set_temperature_on_off_room_only_stores_setpoints(
    hass, setup_integration, cloud
):
    await _call(hass, SERVICE_SET_TEMPERATURE, DINING_ENTITY, **{ATTR_TEMPERATURE: 23.0})

    cloud.set_setpoints.assert_called_once_with(DINING, heat=ACTIVE_HEAT, cool=23.0)
    assert "mode" not in cloud.set_setpoints.call_args.kwargs
    cloud.set_active.assert_not_called()
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.OFF
    assert state.attributes[ATTR_HVAC_ACTION] == HVACAction.OFF

    # The stored target is what the room comes back on with.
    await _call(hass, SERVICE_TURN_ON, DINING_ENTITY)
    cloud.set_active.assert_called_once_with(DINING, True)
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.COOL
    assert state.attributes[ATTR_TEMPERATURE] == 23.0


async def test_set_temperature_on_cooling_room_keeps_its_mode(
    hass, setup_integration, mock_client
):
    await _call(hass, SERVICE_SET_TEMPERATURE, BEDROOM_ENTITY, **{ATTR_TEMPERATURE: 23.0})
    mock_client.set_setpoints.assert_called_once_with(BEDROOM, heat=16.0, cool=23.0)
    assert "mode" not in mock_client.set_setpoints.call_args.kwargs
    state = _state(hass, BEDROOM_ENTITY)
    assert state.state == HVACMode.COOL
    assert state.attributes[ATTR_TEMPERATURE] == 23.0


async def test_set_temperature_on_heating_room_sets_heat_target(
    hass, system, config_entry, mock_client, fake_stream
):
    system["rooms"][LIVING].update(
        mode=api.MODE_HEAT, on=True, heat_setpoint=20.0, cool_setpoint=26.0,
        active_comfort_id=f"{LIVING}-active",
    )
    await _setup(hass, config_entry)
    mock_client.set_setpoints.side_effect = lambda room_id, heat=None, cool=None, mode=None: (
        api.MODE_HEAT
    )

    await _call(hass, SERVICE_SET_TEMPERATURE, LIVING_ENTITY, **{ATTR_TEMPERATURE: 21.5})

    mock_client.set_setpoints.assert_called_once_with(LIVING, heat=21.5, cool=26.0)
    state = _state(hass, LIVING_ENTITY)
    assert state.state == HVACMode.HEAT
    assert state.attributes[ATTR_TEMPERATURE] == 21.5


async def test_set_temperature_range(hass, setup_integration, mock_client):
    await _call(hass, SERVICE_SET_TEMPERATURE, BEDROOM_ENTITY,
                **{ATTR_TARGET_TEMP_LOW: 18.0, ATTR_TARGET_TEMP_HIGH: 25.0})
    mock_client.set_setpoints.assert_called_once_with(BEDROOM, heat=18.0, cool=25.0)
    state = _state(hass, BEDROOM_ENTITY)
    assert state.attributes[ATTR_TARGET_TEMP_LOW] == 18.0
    assert state.attributes[ATTR_TARGET_TEMP_HIGH] == 25.0


# --- hvac mode / on / off ---------------------------------------------------


async def test_set_hvac_mode_off(hass, setup_integration, mock_client):
    await _call(hass, SERVICE_SET_HVAC_MODE, BEDROOM_ENTITY, **{ATTR_HVAC_MODE: HVACMode.OFF})
    mock_client.set_active.assert_called_once_with(BEDROOM, False)
    mock_client.set_setpoints.assert_not_called()
    state = _state(hass, BEDROOM_ENTITY)
    assert state.state == HVACMode.OFF
    assert state.attributes[ATTR_HVAC_ACTION] == HVACAction.OFF
    assert state.attributes[ATTR_PRESET_MODE] is None


async def test_turn_off(hass, setup_integration, mock_client):
    await _call(hass, SERVICE_TURN_OFF, BEDROOM_ENTITY)
    mock_client.set_active.assert_called_once_with(BEDROOM, False)
    assert _state(hass, BEDROOM_ENTITY).state == HVACMode.OFF


async def test_turn_on(hass, setup_integration, mock_client):
    await _call(hass, SERVICE_TURN_ON, DINING_ENTITY)
    mock_client.set_active.assert_called_once_with(DINING, True)
    mock_client.set_setpoints.assert_not_called()
    # The client reports which mode the room resumed in (Cool here).
    assert _state(hass, DINING_ENTITY).state == HVACMode.COOL


async def test_turn_on_shows_the_resumed_mode(hass, setup_integration, mock_client):
    mock_client.set_active.side_effect = lambda room_id, on: api.MODE_HEAT
    await _call(hass, SERVICE_TURN_ON, DINING_ENTITY)
    assert _state(hass, DINING_ENTITY).state == HVACMode.HEAT


@pytest.mark.parametrize(
    ("hvac_mode", "quilt_mode"),
    [
        (HVACMode.COOL, api.MODE_COOL),
        (HVACMode.HEAT, api.MODE_HEAT),
        (HVACMode.HEAT_COOL, api.MODE_HEAT_COOL),
        (HVACMode.FAN_ONLY, api.MODE_FAN),
        (HVACMode.DRY, api.MODE_DRY),
    ],
)
async def test_set_hvac_mode_writes_quilt_mode(
    hass, setup_integration, mock_client, hvac_mode, quilt_mode
):
    await _call(hass, SERVICE_SET_HVAC_MODE, DINING_ENTITY, **{ATTR_HVAC_MODE: hvac_mode})
    mock_client.set_setpoints.assert_called_once_with(
        DINING, heat=ACTIVE_HEAT, cool=ACTIVE_COOL, mode=quilt_mode
    )
    mock_client.set_active.assert_not_called()
    assert _state(hass, DINING_ENTITY).state == hvac_mode


async def test_set_hvac_mode_auto_is_rejected(hass, setup_integration, mock_client):
    with pytest.raises(ServiceValidationError):
        await _call(hass, SERVICE_SET_HVAC_MODE, DINING_ENTITY, **{ATTR_HVAC_MODE: HVACMode.AUTO})
    mock_client.set_setpoints.assert_not_called()
    mock_client.set_active.assert_not_called()


async def test_homekit_mode_and_temperature_writes_are_serialized(
    hass, setup_integration, mock_client
):
    """HomeKit fires set_hvac_mode and set_temperature(hvac_mode=...) as concurrent tasks."""
    active = 0
    peak = 0
    lock = threading.Lock()

    def slow_set_setpoints(room_id, heat=None, cool=None, mode=None):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return mode if mode is not None else api.MODE_OFF

    mock_client.set_setpoints.side_effect = slow_set_setpoints

    # Same shape as homekit.type_thermostats._set_chars -> async_call_service.
    t1 = hass.async_create_task(
        hass.services.async_call(
            CLIMATE_DOMAIN, SERVICE_SET_HVAC_MODE,
            {ATTR_ENTITY_ID: DINING_ENTITY, ATTR_HVAC_MODE: HVACMode.HEAT},
        ),
        eager_start=True,
    )
    t2 = hass.async_create_task(
        hass.services.async_call(
            CLIMATE_DOMAIN, SERVICE_SET_TEMPERATURE,
            {ATTR_ENTITY_ID: DINING_ENTITY, ATTR_HVAC_MODE: HVACMode.HEAT,
             ATTR_TEMPERATURE: 21.0},
        ),
        eager_start=True,
    )
    await asyncio.gather(t1, t2)
    await hass.async_block_till_done()

    assert peak == 1
    assert mock_client.set_setpoints.call_count == 2
    last = mock_client.set_setpoints.call_args
    assert last.args == (DINING,)
    assert last.kwargs == {"heat": 21.0, "cool": ACTIVE_COOL, "mode": api.MODE_HEAT}
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.HEAT
    assert state.attributes[ATTR_TEMPERATURE] == 21.0


@pytest.mark.parametrize(
    ("service", "data"),
    [
        (SERVICE_TURN_ON, {}),
        (SERVICE_SET_HVAC_MODE, {ATTR_HVAC_MODE: HVACMode.COOL}),
        (SERVICE_SET_TEMPERATURE, {ATTR_HVAC_MODE: HVACMode.COOL, ATTR_TEMPERATURE: 22.5}),
    ],
)
async def test_turning_a_room_on_never_shows_the_off_preset(
    hass, setup_integration, cloud, service, data
):
    """Every write that turns an off room on shows the Active preset straight away.

    Regression: the optimistic state used to set mode/on but keep the Off
    room's active_comfort_id, so the first state written showed the room
    cooling with preset_mode 'Off' (not in preset_modes) until a read or push
    brought the Active comfort id.
    """
    written: list[tuple[str, Any]] = []

    @callback
    def _record(event) -> None:
        if event.data["entity_id"] == DINING_ENTITY and event.data["new_state"]:
            new = event.data["new_state"]
            written.append((new.state, new.attributes.get(ATTR_PRESET_MODE)))

    hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    await _call(hass, service, DINING_ENTITY, **data)
    await hass.async_block_till_done()

    assert written, "the write should change the entity's state"
    assert written[-1] == (HVACMode.COOL, "Active")
    # No intermediate state claims the room is on with the Off preset.
    assert all(preset != "Off" for mode, preset in written if mode != HVACMode.OFF), written


async def test_turn_on_when_already_on_keeps_the_running_preset(
    hass, system, config_entry, mock_client, fake_stream, cloud
):
    """climate.turn_on on a room that is already on does nothing.

    Regression: it used to call set_active(room, True), which re-applies the
    Active preset, so a room running its Sleep preset jumped to the Active
    preset and setpoints.
    """
    system["rooms"][BEDROOM].update(
        active_comfort_id=f"{BEDROOM}-sleep", heat_setpoint=17.0, cool_setpoint=22.0,
    )
    await _setup(hass, config_entry)
    before = _state(hass, BEDROOM_ENTITY)
    assert (before.state, before.attributes[ATTR_PRESET_MODE]) == (HVACMode.COOL, "Sleep")
    assert before.attributes[ATTR_TEMPERATURE] == 22.0

    await _call(hass, SERVICE_TURN_ON, BEDROOM_ENTITY)

    mock_client.set_active.assert_not_called()
    mock_client.set_setpoints.assert_not_called()
    mock_client.set_preset.assert_not_called()
    after = _state(hass, BEDROOM_ENTITY)
    assert after.state == HVACMode.COOL
    assert after.attributes[ATTR_PRESET_MODE] == "Sleep"
    assert after.attributes[ATTR_TEMPERATURE] == 22.0


async def test_setpoint_change_on_running_room_shows_active_preset_at_once(
    hass, system, config_entry, mock_client, fake_stream, cloud
):
    """set_temperature on a room running Sleep writes the Active preset, and says so.

    set_setpoints stores the setpoints in the Active preset and applies it, so
    the first state written already shows preset_mode 'Active' rather than the
    stale Sleep preset with the new target.
    """
    system["rooms"][BEDROOM].update(
        active_comfort_id=f"{BEDROOM}-sleep", heat_setpoint=17.0, cool_setpoint=22.0,
    )
    await _setup(hass, config_entry)
    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_PRESET_MODE] == "Sleep"

    written: list[tuple[str, Any, Any]] = []

    @callback
    def _record(event) -> None:
        if event.data["entity_id"] == BEDROOM_ENTITY and event.data["new_state"]:
            new = event.data["new_state"]
            written.append((new.state, new.attributes.get(ATTR_PRESET_MODE),
                            new.attributes.get(ATTR_TEMPERATURE)))

    hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    await _call(hass, SERVICE_SET_TEMPERATURE, BEDROOM_ENTITY, **{ATTR_TEMPERATURE: 23.0})

    mock_client.set_setpoints.assert_called_once()
    assert written, "the write should change the entity's state"
    assert all(entry == (HVACMode.COOL, "Active", 23.0) for entry in written), written


# --- hvac_action --------------------------------------------------------------

# A cooling room where the temperature estimate says "idle" (22 °C, target 24),
# and one where it says "cooling" (27 °C): each case below uses the one that
# disagrees with the expected action, so only the unit's report can produce it.
ESTIMATE_IDLE = {"current_temp": 22.0}
ESTIMATE_COOLING = {"current_temp": 27.0}


@pytest.mark.parametrize(
    ("hvac_state", "expected"),
    [
        (api.HVAC_STATE_STANDBY, HVACAction.IDLE),
        (api.HVAC_STATE_COOL, HVACAction.COOLING),
        (api.HVAC_STATE_HEAT, HVACAction.HEATING),
        (api.HVAC_STATE_DRIFT, HVACAction.IDLE),
        (api.HVAC_STATE_FAN, HVACAction.FAN),
        (api.HVAC_STATE_COOL_DEFERRED, HVACAction.IDLE),
        (api.HVAC_STATE_HEAT_DEFERRED, HVACAction.IDLE),
        (api.HVAC_STATE_FAN_DEFERRED, HVACAction.IDLE),
        (api.HVAC_STATE_COOL_PREPARING, HVACAction.COOLING),
        (api.HVAC_STATE_HEAT_PREPARING, HVACAction.PREHEATING),
        (api.HVAC_STATE_DRY, HVACAction.DRYING),
        (api.HVAC_STATE_DRY_DEFERRED, HVACAction.IDLE),
        (api.HVAC_STATE_DRY_PREPARING, HVACAction.DRYING),
    ],
)
async def test_hvac_action_follows_unit_state(
    hass, system, config_entry, mock_client, fake_stream, hvac_state, expected
):
    room = system["rooms"][BEDROOM]
    room.update(ESTIMATE_COOLING if expected == HVACAction.IDLE else ESTIMATE_IDLE)
    room["hvac_state"] = hvac_state
    await _setup(hass, config_entry)

    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_HVAC_ACTION] == expected


@pytest.mark.parametrize("hvac_state", range(1, 14))
async def test_hvac_action_off_when_room_off(
    hass, system, config_entry, mock_client, fake_stream, hvac_state
):
    system["rooms"][DINING].update(hvac_state=hvac_state, current_temp=30.0)
    await _setup(hass, config_entry)
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.OFF
    assert state.attributes[ATTR_HVAC_ACTION] == HVACAction.OFF


async def test_hvac_action_follows_pushed_unit_state(hass, setup_integration, fake_stream):
    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_HVAC_ACTION] == HVACAction.COOLING

    await _push(hass, fake_stream,
                {"kind": "space", "space_id": BEDROOM, "hvac_state": api.HVAC_STATE_DRIFT})
    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_HVAC_ACTION] == HVACAction.IDLE

    await _push(hass, fake_stream,
                {"kind": "space", "space_id": BEDROOM,
                 "hvac_state": api.HVAC_STATE_COOL_PREPARING})
    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_HVAC_ACTION] == HVACAction.COOLING


@pytest.mark.parametrize(
    ("room", "expected"),
    [
        # Cool at 24: above target + 0.2 cools, otherwise idle.
        ({"mode": api.MODE_COOL, "current_temp": 24.5}, HVACAction.COOLING),
        ({"mode": api.MODE_COOL, "current_temp": 24.1}, HVACAction.IDLE),
        # Heat at 20: below target - 0.2 heats.
        ({"mode": api.MODE_HEAT, "heat_setpoint": 20.0, "current_temp": 19.0}, HVACAction.HEATING),
        ({"mode": api.MODE_HEAT, "heat_setpoint": 20.0, "current_temp": 21.0}, HVACAction.IDLE),
        # Heat/Cool 20-24.
        ({"mode": api.MODE_HEAT_COOL, "heat_setpoint": 20.0, "current_temp": 25.0},
         HVACAction.COOLING),
        ({"mode": api.MODE_HEAT_COOL, "heat_setpoint": 20.0, "current_temp": 19.0},
         HVACAction.HEATING),
        ({"mode": api.MODE_HEAT_COOL, "heat_setpoint": 20.0, "current_temp": 22.0},
         HVACAction.IDLE),
        ({"mode": api.MODE_FAN, "current_temp": 22.0}, HVACAction.FAN),
        ({"mode": api.MODE_DRY, "current_temp": 22.0}, HVACAction.DRYING),
        ({"mode": api.MODE_COOL, "current_temp": None}, HVACAction.IDLE),
    ],
)
@pytest.mark.parametrize("missing", ["absent", "unknown_value"])
async def test_hvac_action_estimated_without_unit_state(
    hass, system, config_entry, mock_client, fake_stream, room, expected, missing
):
    r = system["rooms"][BEDROOM]
    r.update(room)
    if missing == "absent":
        # get_system() leaves the key out when Space.sensor f4 isn't reported.
        del r["hvac_state"]
    else:
        r["hvac_state"] = 99  # a state this version doesn't know
    await _setup(hass, config_entry)

    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_HVAC_ACTION] == expected


async def test_hvac_action_uses_estimate_right_after_a_write(
    hass, setup_integration, cloud, fake_stream, freezer
):
    # Dining is off and its unit reports standby.
    assert _state(hass, DINING_ENTITY).attributes[ATTR_HVAC_ACTION] == HVACAction.OFF

    await _call(hass, SERVICE_SET_HVAC_MODE, DINING_ENTITY, **{ATTR_HVAC_MODE: HVACMode.FAN_ONLY})
    # The unit hasn't caught up (still standby -> idle); the estimate says fan.
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.FAN_ONLY
    assert state.attributes[ATTR_HVAC_ACTION] == HVACAction.FAN

    # A push of the (stale) standby during the hold doesn't override it either.
    await _push(hass, fake_stream,
                {"kind": "space", "space_id": DINING, "hvac_state": api.HVAC_STATE_STANDBY})
    assert _state(hass, DINING_ENTITY).attributes[ATTR_HVAC_ACTION] == HVACAction.FAN

    # Once the hold ends the unit's report is authoritative again.
    await _expire_hold(hass, freezer)
    state = _state(hass, DINING_ENTITY)
    assert state.state == HVACMode.FAN_ONLY
    assert state.attributes[ATTR_HVAC_ACTION] == HVACAction.IDLE

    await _push(hass, fake_stream,
                {"kind": "space", "space_id": DINING, "hvac_state": api.HVAC_STATE_FAN})
    assert _state(hass, DINING_ENTITY).attributes[ATTR_HVAC_ACTION] == HVACAction.FAN


async def test_hvac_action_estimate_after_setpoint_write_on_cooling_room(
    hass, setup_integration, mock_client
):
    # Unit reports cooling at 24.5 °C with a 24 °C target.
    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_HVAC_ACTION] == HVACAction.COOLING
    await _call(hass, SERVICE_SET_TEMPERATURE, BEDROOM_ENTITY, **{ATTR_TEMPERATURE: 26.0})
    # 24.5 °C is under the new 26 °C target, so the estimate says idle.
    assert _state(hass, BEDROOM_ENTITY).attributes[ATTR_HVAC_ACTION] == HVACAction.IDLE


# --- fallback modes -----------------------------------------------------------


async def test_fallback_auto_shows_heat_cool(
    hass, system, config_entry, mock_client, fake_stream
):
    system["rooms"][LIVING].update(
        mode=api.MODE_FALLBACK_AUTO, on=True, heat_setpoint=20.0, cool_setpoint=24.0,
        active_comfort_id=f"{LIVING}-active", hvac_state=api.HVAC_STATE_COOL,
        current_temp=22.0,
    )
    await _setup(hass, config_entry)
    state = _state(hass, LIVING_ENTITY)
    assert state.state == HVACMode.HEAT_COOL
    assert state.attributes[ATTR_HVAC_ACTION] == HVACAction.COOLING


@pytest.mark.parametrize("on", [False, True])
async def test_fallback_off_shows_off(
    hass, system, config_entry, mock_client, fake_stream, on
):
    # The parser reports on=False for mode 7; also check a stray on=True.
    system["rooms"][LIVING].update(
        mode=api.MODE_FALLBACK_OFF, on=on, hvac_state=api.HVAC_STATE_COOL, current_temp=30.0,
    )
    await _setup(hass, config_entry)
    state = _state(hass, LIVING_ENTITY)
    assert state.state == HVACMode.OFF
    assert state.attributes[ATTR_HVAC_ACTION] == HVACAction.OFF


async def test_pushed_fallback_modes(hass, setup_integration, system, fake_stream):
    """Quilt switching a room into a fallback mode on its own, seen over the stream."""
    bedroom = system["rooms"][BEDROOM]  # what the follow-up full read returns

    bedroom.update(mode=api.MODE_FALLBACK_AUTO, on=True)
    await _push(hass, fake_stream,
                {"kind": "space", "space_id": BEDROOM, "mode": api.MODE_FALLBACK_AUTO,
                 "on": True})
    assert _state(hass, BEDROOM_ENTITY).state == HVACMode.HEAT_COOL

    bedroom.update(mode=api.MODE_FALLBACK_OFF, on=False)
    await _push(hass, fake_stream,
                {"kind": "space", "space_id": BEDROOM, "mode": api.MODE_FALLBACK_OFF,
                 "on": False})
    state = _state(hass, BEDROOM_ENTITY)
    assert state.state == HVACMode.OFF
    assert state.attributes[ATTR_HVAC_ACTION] == HVACAction.OFF


# --- through the real client's write path -----------------------------------


async def test_ecobee_sync_call_reaches_quilt_as_cool(hass, setup_integration, wire):
    """set_temperature(hvac_mode=cool) on an off room, down to the gRPC requests."""
    await _call(hass, SERVICE_SET_TEMPERATURE, DINING_ENTITY,
                **{ATTR_HVAC_MODE: HVACMode.COOL, ATTR_TEMPERATURE: 22.5})

    (preset,) = _preset_writes(wire)
    assert preset.name == "Active"
    assert preset.f8 == api.MODE_COOL
    assert preset.cool_setpoint == pytest.approx(22.5)
    (space,) = _space_writes(wire)
    assert space.mode == api.MODE_COOL
    assert space.cool_setpoint == pytest.approx(22.5)
    assert space.comfort_id == f"{DINING}-active"
    assert _state(hass, DINING_ENTITY).state == HVACMode.COOL


async def test_setpoint_change_on_cool_room_keeps_cool_on_the_wire(
    hass, setup_integration, wire
):
    """Positive control for the fallback test below: same call, ordinary mode."""
    await _call(hass, SERVICE_SET_TEMPERATURE, BEDROOM_ENTITY,
                **{ATTR_TARGET_TEMP_LOW: 17.0, ATTR_TARGET_TEMP_HIGH: 23.0})
    assert [v.mode for v in _space_writes(wire)] == [api.MODE_COOL]
    assert [v.f8 for v in _preset_writes(wire)] == [api.MODE_COOL]


async def test_setpoint_change_on_fallback_auto_room_never_writes_mode_6(
    hass, system, config_entry, mock_client, fake_stream, wire
):
    """A setpoint change on a FALLBACK_AUTO room writes Heat/Cool, never mode 6.

    Fallback modes are Quilt-chosen and never written by us: the room is shown
    as Heat/Cool, so that is what set_setpoints keeps (via
    QuiltClient.current_writable_mode). Regression: set_setpoints(mode=None)
    used to reuse room['mode'] and write 6 into UpdateSpace.mode and the
    Active preset's f8.
    """
    system["rooms"][LIVING].update(
        mode=api.MODE_FALLBACK_AUTO, on=True, heat_setpoint=20.0, cool_setpoint=24.0,
        active_comfort_id=f"{LIVING}-active",
    )
    await _setup(hass, config_entry)
    assert _state(hass, LIVING_ENTITY).state == HVACMode.HEAT_COOL

    await _call(hass, SERVICE_SET_TEMPERATURE, LIVING_ENTITY,
                **{ATTR_TARGET_TEMP_LOW: 19.0, ATTR_TARGET_TEMP_HIGH: 25.0})

    space_modes = [v.mode for v in _space_writes(wire)]
    preset_modes = [v.f8 for v in _preset_writes(wire)]
    assert space_modes, "the change should apply to a room that is on"
    for written in space_modes + preset_modes:
        assert written in api.KNOWN_MODES, (space_modes, preset_modes)
    assert space_modes == [api.MODE_HEAT_COOL]
    assert preset_modes == [api.MODE_HEAT_COOL]
    assert _state(hass, LIVING_ENTITY).state == HVACMode.HEAT_COOL
