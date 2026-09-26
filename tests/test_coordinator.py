"""QuiltCoordinator: poll failures, reauth, pushes, stale-read guards, energy, catch-up, topics.

Everything runs through Home Assistant with the fake client and stream from
conftest. The coordinator's own clock (its `time` module) is replaced with a
FakeClock so push/poll ordering, the energy interval and the catch-up grace
are deterministic; wall-clock date changes use the freezer fixture.
"""
from __future__ import annotations

import asyncio
import copy
from datetime import timedelta
import logging
import threading
import time
import urllib.error
from unittest.mock import MagicMock, patch

import grpc
import pytest

from homeassistant.components.climate import HVACAction
from homeassistant.config_entries import SOURCE_REAUTH, ConfigEntryState
from homeassistant.const import EVENT_STATE_CHANGED, STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import UpdateFailed
from homeassistant.util import dt as dt_util
from homeassistant.util.async_ import run_callback_threadsafe

from custom_components.quilt import api, coordinator as coordinator_mod
from custom_components.quilt.const import DOMAIN, ENERGY_REFRESH_INTERVAL
from custom_components.quilt.coordinator import CATCH_UP_GRACE, QuiltCoordinator

from .conftest import BEDROOM, DIAL_ID, DINING, LIVING, make_room

ROOMS = (DINING, LIVING, BEDROOM)
# Entities are addressed by (platform, unique_id) and resolved through the entity
# registry, so these tests don't hinge on entity ids; the ids themselves are
# pinned by the test_room_sensor_entity_ids_* tests at the end.
CLIMATE = {r: ("climate", f"quilt_{r}") for r in ROOMS}
OCCUPANCY = {r: ("binary_sensor", f"quilt_{r}_occupancy") for r in ROOMS}
HUMIDITY = {r: ("sensor", f"quilt_{r}_humidity") for r in ROOMS}
ENERGY = {r: ("sensor", f"quilt_{r}_energy_today") for r in ROOMS}
DIAL_TEMP = ("sensor", "quilt_dial_temperature")
# Entities whose availability follows the poll / push stream (energy does not).
LIVE_ENTITIES = [*CLIMATE.values(), *OCCUPANCY.values(), *HUMIDITY.values(), DIAL_TEMP]

ENERGY_WARNING = "Couldn't read Quilt energy use"


# --- helpers ---------------------------------------------------------------
class FakeClock:
    """Stands in for coordinator.time: monotonic() only moves when a test says so."""

    def __init__(self) -> None:
        self.now = 10_000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return time.time()  # wall clock (frozen when the freezer fixture is active)

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture(autouse=True)
def clock():
    """Replace the coordinator's clock before the integration is set up."""
    fake = FakeClock()
    with patch.object(coordinator_mod, "time", fake):
        yield fake


class UnavailableRpcError(grpc.RpcError):
    """What grpc raises when Quilt's endpoint can't be reached."""

    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return "Socket closed"


def _coord(entry) -> QuiltCoordinator:
    return entry.runtime_data


def _serve(get_system: MagicMock, system: dict) -> None:
    """Back to normal: each poll returns a deep copy of `system` (as in conftest)."""
    get_system.side_effect = lambda: copy.deepcopy(system)


def _serve_energy(mock_client, energy: dict) -> None:
    """Back to normal: one hourly bucket per room starting "now" (as in conftest)."""
    mock_client.get_energy.side_effect = lambda since, until: {
        sid: [(int(until) - 3600, kwh)] for sid, kwh in energy.items()
    }


def _serve_buckets(mock_client, metered: dict[str, dict[int, float]]) -> None:
    """Serve Quilt's hourly buckets {room: {start epoch s: kWh}} for the window asked."""
    mock_client.get_energy.side_effect = lambda since, until: {
        sid: sorted((start, kwh) for start, kwh in hours.items() if since <= start < until)
        for sid, hours in metered.items()
    }


def _eid(hass: HomeAssistant, ref: tuple[str, str]) -> str:
    entity_id = er.async_get(hass).async_get_entity_id(ref[0], DOMAIN, ref[1])
    assert entity_id is not None, f"{ref} not registered"
    return entity_id


def _state(hass: HomeAssistant, ref: tuple[str, str]):
    state = hass.states.get(_eid(hass, ref))
    assert state is not None, f"{ref} has no state"
    return state


async def _push(hass: HomeAssistant, fake_stream, events: list[dict]) -> None:
    """Deliver events the way the stream thread does, then let HA settle."""
    fake_stream.instance.on_events(events)
    await hass.async_block_till_done()


async def _loop_turn(hass: HomeAssistant) -> None:
    """Run everything already queued with call_soon(_threadsafe), without waiting on tasks."""
    fut = hass.loop.create_future()
    hass.loop.call_soon(fut.set_result, None)
    await fut


async def _refresh(hass: HomeAssistant, coordinator: QuiltCoordinator) -> None:
    await coordinator.async_refresh()
    await hass.async_block_till_done()


async def _settle(hass: HomeAssistant) -> None:
    """Also wait for background work (the energy fetch runs as a background task)."""
    await hass.async_block_till_done(wait_background_tasks=True)


def _in_worker_thread(mock_client, name: str):
    """Run the client's `name` call on a real executor thread, as in production.

    The HA test harness runs Mock executor targets inline on the event loop, so
    a read could never be "in flight" while a push lands. A plain function that
    forwards to the mock goes through the real executor; the mock (returned)
    still records the calls.
    """
    mocked = getattr(mock_client, name)

    def call(*args):
        return mocked(*args)

    setattr(mock_client, name, call)
    return mocked


def _without_presets(data: dict) -> dict:
    """Coordinator data minus the presets (they hold MagicMocks that don't compare after a copy)."""
    return {
        "dial": dict(data["dial"] or {}),
        "rooms": {
            rid: {k: v for k, v in room.items() if k != "presets"}
            for rid, room in data["rooms"].items()
        },
    }


def _energy_warnings(caplog) -> list[logging.LogRecord]:
    return [
        r for r in caplog.records
        if r.levelno == logging.WARNING and ENERGY_WARNING in r.getMessage()
    ]


# --- (1) poll failures -----------------------------------------------------
POLL_ERRORS = [
    pytest.param(UnavailableRpcError(), "Quilt API error: StatusCode.UNAVAILABLE", id="grpc-unavailable"),
    pytest.param(grpc.RpcError(), "Quilt API error: RpcError", id="grpc-bare"),
    pytest.param(urllib.error.URLError("nodename nor servname provided"), "Can't reach Quilt", id="urlerror"),
    pytest.param(OSError(51, "Network is unreachable"), "Can't reach Quilt", id="oserror"),
    pytest.param(TimeoutError("timed out"), "Can't reach Quilt", id="timeout"),
    pytest.param(
        api.QuiltAuthError("Cognito HTTP 503: ServiceUnavailable"),
        "Quilt login refresh failed",
        id="auth-not-revoked",
    ),
]


@pytest.mark.parametrize(("error", "message"), POLL_ERRORS)
async def test_poll_failure_is_update_failed_and_entities_go_unavailable(
    hass, setup_integration, mock_client, fake_stream, caplog, error, message
):
    entry = setup_integration
    coordinator = _coord(entry)
    await _settle(hass)
    assert fake_stream.instance.healthy is False
    mock_client.get_system.side_effect = error
    caplog.clear()

    await _refresh(hass, coordinator)

    assert coordinator.last_update_success is False
    assert isinstance(coordinator.last_exception, UpdateFailed)
    assert message in str(coordinator.last_exception)
    assert coordinator.last_exception.__cause__ is error
    # One plain error line from HA's UpdateFailed path; no traceback, no "Unexpected error".
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors == [f"Error fetching quilt data: {coordinator.last_exception}"]
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING and r.exc_info]
    assert "Unexpected error" not in caplog.text

    for ref in LIVE_ENTITIES:
        assert _state(hass, ref).state == STATE_UNAVAILABLE, ref
    # Energy is cloud metering, not live telemetry: it stays readable through an outage.
    assert _state(hass, ENERGY[DINING]).state == "0.863"
    # A transient failure is not a login problem.
    assert hass.config_entries.flow.async_progress() == []
    assert entry.state is ConfigEntryState.LOADED


async def test_rpc_error_whose_code_raises_is_still_update_failed(
    hass, setup_integration, mock_client, caplog
):
    class BrokenCode(grpc.RpcError):
        def code(self):
            raise ValueError("no status")

    coordinator = _coord(setup_integration)
    mock_client.get_system.side_effect = BrokenCode()
    await _refresh(hass, coordinator)
    assert isinstance(coordinator.last_exception, UpdateFailed)
    assert "Quilt API error: BrokenCode" in str(coordinator.last_exception)
    assert "Unexpected error" not in caplog.text


async def test_poll_failure_keeps_entities_available_while_push_is_healthy(
    hass, setup_integration, mock_client, fake_stream
):
    coordinator = _coord(setup_integration)
    fake_stream.instance.healthy = True
    mock_client.get_system.side_effect = UnavailableRpcError()

    await _refresh(hass, coordinator)

    assert coordinator.last_update_success is False
    for ref in LIVE_ENTITIES:
        assert _state(hass, ref).state != STATE_UNAVAILABLE, ref
    assert _state(hass, CLIMATE[BEDROOM]).state == "cool"
    assert _state(hass, DIAL_TEMP).state == "25.2"

    # Pushes keep the entities current while the poll is down.
    await _push(hass, fake_stream, [
        {"kind": "space", "space_id": BEDROOM, "current_temp": 23.0},
        {"kind": "dial", "dial_id": DIAL_ID, "temperature": 24.1},
    ])
    assert _state(hass, CLIMATE[BEDROOM]).attributes["current_temperature"] == 23.0
    assert _state(hass, DIAL_TEMP).state == "24.1"


@pytest.mark.parametrize("healthy", [False, True])
async def test_entities_recover_when_the_next_poll_succeeds(
    hass, setup_integration, mock_client, fake_stream, system, caplog, healthy
):
    coordinator = _coord(setup_integration)
    fake_stream.instance.healthy = healthy
    mock_client.get_system.side_effect = OSError("Network is down")
    await _refresh(hass, coordinator)
    assert coordinator.last_update_success is False

    system["rooms"][DINING]["current_temp"] = 21.5
    system["dial"]["temperature"] = 22.0
    _serve(mock_client.get_system, system)
    caplog.clear()
    await _refresh(hass, coordinator)

    assert coordinator.last_update_success is True
    assert "Fetching quilt data recovered" in caplog.text
    for ref in LIVE_ENTITIES:
        assert _state(hass, ref).state != STATE_UNAVAILABLE, ref
    assert _state(hass, CLIMATE[DINING]).attributes["current_temperature"] == 21.5
    assert _state(hass, DIAL_TEMP).state == "22.0"


async def test_entities_go_unavailable_when_stream_dies_during_a_poll_outage(
    hass, setup_integration, mock_client, fake_stream
):
    """Availability is re-checked when the stream drops or returns while polls fail.

    Availability is only re-evaluated when the coordinator notifies listeners,
    and DataUpdateCoordinator doesn't on a second consecutive failed poll. So
    the stream reports its disconnects (NotifierStream's on_disconnect, which a
    silent stream also reaches once its watchdog cancels the call) and the
    coordinator re-notifies; otherwise entities would stay 'available' with
    stale values for the whole outage.
    """
    coordinator = _coord(setup_integration)
    stream = fake_stream.instance
    stream.healthy = True
    mock_client.get_system.side_effect = OSError("Network is down")
    await _refresh(hass, coordinator)
    assert coordinator.last_update_success is False
    assert _state(hass, CLIMATE[BEDROOM]).state == "cool"  # the stream keeps it available

    # The outage goes on: a second failed poll, which HA doesn't notify about.
    await _refresh(hass, coordinator)
    assert coordinator.last_update_success is False
    assert _state(hass, CLIMATE[BEDROOM]).state == "cool"

    # Then the stream drops too, reported from the stream thread.
    stream.healthy = False
    stream.on_disconnect()
    await hass.async_block_till_done()
    assert coordinator.push_healthy is False
    for ref in LIVE_ENTITIES:
        assert _state(hass, ref).state == STATE_UNAVAILABLE, ref

    # And the other way: the stream reconnects while polls are still failing.
    stream.healthy = True
    stream.on_connect()
    await hass.async_block_till_done()
    assert coordinator.last_update_success is False
    for ref in LIVE_ENTITIES:
        assert _state(hass, ref).state != STATE_UNAVAILABLE, ref
    assert _state(hass, CLIMATE[BEDROOM]).state == "cool"


# --- (2) reauth --------------------------------------------------------------
@pytest.mark.parametrize(
    "message",
    [
        "NotAuthorizedException",
        "NotAuthorizedException: Refresh Token has been revoked",
        "Refresh Token has been revoked",
    ],
)
async def test_revoked_login_starts_reauth(
    hass, setup_integration, mock_client, caplog, message
):
    entry = setup_integration
    coordinator = _coord(entry)
    mock_client.get_system.side_effect = api.QuiltAuthError(message)

    await _refresh(hass, coordinator)

    assert coordinator.last_update_success is False
    assert isinstance(coordinator.last_exception, ConfigEntryAuthFailed)
    flows = hass.config_entries.flow.async_progress()
    assert len(flows) == 1
    assert flows[0]["handler"] == "quilt"
    assert flows[0]["context"]["source"] == SOURCE_REAUTH
    assert flows[0]["context"]["entry_id"] == entry.entry_id
    assert flows[0]["step_id"] == "reauth_confirm"
    assert "Unexpected error" not in caplog.text

    # A second rejected poll doesn't pile up another reauth flow.
    await _refresh(hass, coordinator)
    assert len(hass.config_entries.flow.async_progress()) == 1


# --- (3) pushes --------------------------------------------------------------
async def test_space_push_changes_climate_then_requests_a_full_read(
    hass, setup_integration, mock_client, fake_stream, system
):
    polls = mock_client.get_system.call_count
    seen: list[tuple] = []

    @callback
    def _record(event) -> None:
        if event.data["entity_id"] == dining and event.data["new_state"]:
            new = event.data["new_state"]
            seen.append((new.state, new.attributes.get("temperature"),
                         mock_client.get_system.call_count))

    dining = _eid(hass, CLIMATE[DINING])
    unsub = hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    # Someone turned the Dining Room on to cool at 23 °C in the Quilt app; the
    # cloud reports the same thing to the follow-up read.
    change = {
        "mode": api.MODE_COOL, "on": True, "heat_setpoint": 16.0,
        "cool_setpoint": 23.0, "active_comfort_id": f"{DINING}-active",
    }
    system["rooms"][DINING].update(change)
    await _push(hass, fake_stream, [{"kind": "space", "space_id": DINING, **change}])
    unsub()

    # The pushed state is written before any read happens...
    assert seen and seen[0] == ("cool", 23.0, polls)
    # ...and a control change asks for one full read (preset details aren't pushed).
    assert mock_client.get_system.call_count == polls + 1
    state = _state(hass, CLIMATE[DINING])
    assert state.state == "cool"
    assert state.attributes["temperature"] == 23.0
    assert state.attributes["preset_mode"] == "Active"
    assert _state(hass, CLIMATE[LIVING]).state == "off"


async def test_unchanged_control_push_writes_nothing_and_requests_no_read(
    hass, setup_integration, mock_client, fake_stream, system
):
    polls = mock_client.get_system.call_count
    room = system["rooms"][BEDROOM]
    before = _state(hass, CLIMATE[BEDROOM])
    await _push(hass, fake_stream, [{
        "kind": "space", "space_id": BEDROOM, "mode": room["mode"], "on": room["on"],
        "heat_setpoint": room["heat_setpoint"], "cool_setpoint": room["cool_setpoint"],
        "active_comfort_id": room["active_comfort_id"],
    }])
    assert mock_client.get_system.call_count == polls
    assert _state(hass, CLIMATE[BEDROOM]).last_updated == before.last_updated


async def test_unit_push_with_only_unit_id_is_routed_to_its_room(
    hass, setup_integration, mock_client, fake_stream
):
    polls = mock_client.get_system.call_count
    await _push(hass, fake_stream, [
        {"kind": "unit", "space_id": None, "unit_id": f"unit-{LIVING}",
         "occupied": True, "humidity": 61},
    ])
    assert _state(hass, OCCUPANCY[LIVING]).state == "on"
    assert _state(hass, HUMIDITY[LIVING]).state == "61"
    assert _state(hass, CLIMATE[LIVING]).attributes["current_humidity"] == 61
    # Only that room changed.
    assert _state(hass, OCCUPANCY[DINING]).state == "off"
    assert _state(hass, HUMIDITY[DINING]).state == "52"
    assert _state(hass, HUMIDITY[BEDROOM]).state == "52"
    # Live-only changes don't need a full read.
    assert mock_client.get_system.call_count == polls


async def test_push_for_unknown_room_or_unit_is_ignored(
    hass, setup_integration, mock_client, fake_stream
):
    coordinator = _coord(setup_integration)
    polls = mock_client.get_system.call_count
    before = _without_presets(coordinator.data)
    await _push(hass, fake_stream, [
        {"kind": "unit", "space_id": None, "unit_id": "unit-elsewhere", "occupied": True},
        {"kind": "space", "space_id": "space-building", "mode": api.MODE_HEAT, "on": True},
        # A space event is never matched by unit id.
        {"kind": "space", "space_id": None, "unit_id": f"unit-{DINING}", "mode": api.MODE_HEAT},
    ])
    assert _without_presets(coordinator.data) == before
    assert mock_client.get_system.call_count == polls
    assert _state(hass, CLIMATE[DINING]).state == "off"


async def test_dial_push_updates_dial_sensor_and_other_dials_are_ignored(
    hass, setup_integration, mock_client, fake_stream
):
    coordinator = _coord(setup_integration)
    polls = mock_client.get_system.call_count
    assert _state(hass, DIAL_TEMP).state == "25.2"

    await _push(hass, fake_stream, [
        {"kind": "dial", "dial_id": DIAL_ID, "temperature": 23.4, "ambient_1": 15001},
    ])
    assert _state(hass, DIAL_TEMP).state == "23.4"
    assert coordinator.data["dial"]["ambient_1"] == 15001

    await _push(hass, fake_stream, [
        {"kind": "dial", "dial_id": "dial-someone-else", "temperature": 30.0, "ambient_1": 1},
    ])
    assert _state(hass, DIAL_TEMP).state == "23.4"
    assert coordinator.data["dial"]["temperature"] == 23.4
    assert coordinator.data["dial"]["ambient_1"] == 15001

    # A diff without the Dial's id comes from our own controller topic.
    await _push(hass, fake_stream, [{"kind": "dial", "dial_id": None, "temperature": 22.8}])
    assert _state(hass, DIAL_TEMP).state == "22.8"
    assert mock_client.get_system.call_count == polls


@pytest.mark.parametrize(
    ("hvac_state", "action"),
    [
        (api.HVAC_STATE_STANDBY, HVACAction.IDLE),
        (api.HVAC_STATE_DRIFT, HVACAction.IDLE),
        (api.HVAC_STATE_COOL_DEFERRED, HVACAction.IDLE),
        (api.HVAC_STATE_COOL_PREPARING, HVACAction.COOLING),
        (api.HVAC_STATE_FAN, HVACAction.FAN),
    ],
)
async def test_hvac_state_push_updates_hvac_action(
    hass, setup_integration, mock_client, fake_stream, hvac_state, action
):
    polls = mock_client.get_system.call_count
    assert _state(hass, CLIMATE[BEDROOM]).attributes["hvac_action"] == HVACAction.COOLING

    await _push(hass, fake_stream, [
        {"kind": "space", "space_id": BEDROOM, "current_temp": 24.5, "hvac_state": hvac_state},
    ])

    assert _state(hass, CLIMATE[BEDROOM]).attributes["hvac_action"] == action
    assert mock_client.get_system.call_count == polls

    # And back to cooling when the unit says so.
    await _push(hass, fake_stream, [
        {"kind": "space", "space_id": BEDROOM, "hvac_state": api.HVAC_STATE_COOL},
    ])
    assert _state(hass, CLIMATE[BEDROOM]).attributes["hvac_action"] == HVACAction.COOLING


async def test_hvac_state_push_for_an_off_room_stays_off(hass, setup_integration, fake_stream):
    await _push(hass, fake_stream, [
        {"kind": "space", "space_id": DINING, "hvac_state": api.HVAC_STATE_FAN},
    ])
    assert _state(hass, CLIMATE[DINING]).attributes["hvac_action"] == HVACAction.OFF


# --- (4) stale-read protection ----------------------------------------------
BEDROOM_HEAT_PUSH = [
    {"kind": "space", "space_id": BEDROOM, "mode": api.MODE_HEAT, "on": True,
     "heat_setpoint": 21.0, "cool_setpoint": 24.0, "active_comfort_id": f"{BEDROOM}-active",
     "current_temp": 22.5, "hvac_state": api.HVAC_STATE_HEAT},
    {"kind": "unit", "space_id": BEDROOM, "unit_id": f"unit-{BEDROOM}",
     "occupied": False, "humidity": 47},
]
BEDROOM_HEAT_STATE = {
    "mode": api.MODE_HEAT, "on": True, "heat_setpoint": 21.0, "cool_setpoint": 24.0,
    "current_temp": 22.5, "hvac_state": api.HVAC_STATE_HEAT, "occupied": False, "humidity": 47,
}


async def test_push_during_inflight_poll_is_not_rolled_back_for_rooms(
    hass, setup_integration, mock_client, fake_stream, system, clock
):
    coordinator = _coord(setup_integration)
    stale = copy.deepcopy(system)  # what Quilt had when the slow read began
    started, release = threading.Event(), threading.Event()

    def slow_read() -> dict:
        started.set()
        assert release.wait(5)
        return copy.deepcopy(stale)

    get_system = _in_worker_thread(mock_client, "get_system")
    get_system.side_effect = slow_read
    refresh = hass.async_create_task(coordinator.async_refresh())
    assert await hass.async_add_executor_job(started.wait, 5)

    clock.advance(1)  # the push lands after the read began
    fake_stream.instance.on_events(BEDROOM_HEAT_PUSH)
    await _loop_turn(hass)
    assert _state(hass, CLIMATE[BEDROOM]).state == "heat"

    system["rooms"][BEDROOM].update(BEDROOM_HEAT_STATE)  # the cloud agrees from here on
    _serve(get_system, system)
    release.set()
    await refresh
    await hass.async_block_till_done()

    room = coordinator.data["rooms"][BEDROOM]
    for key, value in BEDROOM_HEAT_STATE.items():
        assert room[key] == value, key
    state = _state(hass, CLIMATE[BEDROOM])
    assert state.state == "heat"
    assert state.attributes["hvac_action"] == HVACAction.HEATING
    assert state.attributes["current_temperature"] == 22.5
    assert state.attributes["temperature"] == 21.0
    assert _state(hass, OCCUPANCY[BEDROOM]).state == "off"
    assert _state(hass, HUMIDITY[BEDROOM]).state == "47"

    # A read that starts after the push wins: Quilt later reports the room cooling again.
    system["rooms"][BEDROOM].update(
        mode=api.MODE_COOL, current_temp=23.0, hvac_state=api.HVAC_STATE_COOL,
        occupied=True, humidity=50,
    )
    clock.advance(1)
    await _refresh(hass, coordinator)
    state = _state(hass, CLIMATE[BEDROOM])
    assert state.state == "cool"
    assert state.attributes["current_temperature"] == 23.0
    assert state.attributes["hvac_action"] == HVACAction.COOLING
    assert _state(hass, OCCUPANCY[BEDROOM]).state == "on"
    assert _state(hass, HUMIDITY[BEDROOM]).state == "50"


async def test_inflight_poll_still_applies_what_was_not_pushed(
    hass, setup_integration, mock_client, fake_stream, system, clock
):
    """Protection is per room and per group (control vs live), not all-or-nothing."""
    coordinator = _coord(setup_integration)
    # The slow read carries a cool-setpoint change for the Bedroom and a new
    # Living Room temperature; while it is in flight only a Bedroom temperature is pushed.
    stale = copy.deepcopy(system)
    stale["rooms"][BEDROOM]["cool_setpoint"] = 23.0
    stale["rooms"][LIVING]["current_temp"] = 22.0

    def read_with_push_midway() -> dict:
        clock.advance(1)
        fake_stream.instance.on_events([
            {"kind": "space", "space_id": BEDROOM, "current_temp": 24.8},
        ])  # from a worker thread, like the real stream
        run_callback_threadsafe(hass.loop, lambda: None).result(5)  # applied on the loop
        return copy.deepcopy(stale)

    get_system = _in_worker_thread(mock_client, "get_system")
    get_system.side_effect = read_with_push_midway
    await _refresh(hass, coordinator)
    assert get_system.call_count == 2

    bedroom = coordinator.data["rooms"][BEDROOM]
    assert bedroom["current_temp"] == 24.8  # pushed during the read: kept
    assert bedroom["cool_setpoint"] == 23.0  # no control push: the read's value applies
    assert coordinator.data["rooms"][LIVING]["current_temp"] == 22.0  # other room: read applies
    assert _state(hass, CLIMATE[BEDROOM]).attributes["temperature"] == 23.0
    assert _state(hass, CLIMATE[BEDROOM]).attributes["current_temperature"] == 24.8
    assert _state(hass, CLIMATE[LIVING]).attributes["current_temperature"] == 22.0


async def test_dial_push_during_inflight_poll_is_not_rolled_back(
    hass, setup_integration, mock_client, fake_stream, system, clock
):
    coordinator = _coord(setup_integration)
    stale = copy.deepcopy(system)  # Dial at 25.2 when the read began

    def read_with_push_midway() -> dict:
        clock.advance(1)
        fake_stream.instance.on_events([
            {"kind": "dial", "dial_id": DIAL_ID, "temperature": 22.2, "ambient_2": 5200},
        ])
        run_callback_threadsafe(hass.loop, lambda: None).result(5)
        return copy.deepcopy(stale)

    get_system = _in_worker_thread(mock_client, "get_system")
    get_system.side_effect = read_with_push_midway
    await _refresh(hass, coordinator)
    assert get_system.call_count == 2

    assert coordinator.data["dial"]["temperature"] == 22.2
    assert coordinator.data["dial"]["ambient_2"] == 5200
    assert _state(hass, DIAL_TEMP).state == "22.2"

    # A read that starts after the push wins.
    system["dial"]["temperature"] = 21.0
    _serve(get_system, system)
    clock.advance(1)
    await _refresh(hass, coordinator)
    assert coordinator.data["dial"]["temperature"] == 21.0
    assert coordinator.data["dial"]["ambient_2"] == 5119
    assert _state(hass, DIAL_TEMP).state == "21.0"


async def test_push_and_poll_at_the_same_instant_let_the_poll_win(
    hass, setup_integration, mock_client, fake_stream, system
):
    """With no clock movement at all (push not after the read began), the read applies."""
    coordinator = _coord(setup_integration)
    await _push(hass, fake_stream, [{"kind": "dial", "dial_id": DIAL_ID, "temperature": 20.0}])
    assert _state(hass, DIAL_TEMP).state == "20.0"
    await _refresh(hass, coordinator)
    assert _state(hass, DIAL_TEMP).state == "25.2"


# --- (5) energy --------------------------------------------------------------
async def test_energy_sensors_after_setup(hass, setup_integration, mock_client, energy):
    coordinator = _coord(setup_integration)
    await _settle(hass)
    midnight = dt_util.start_of_local_day()

    mock_client.get_energy.assert_called_once()
    since, until = mock_client.get_energy.call_args.args
    assert since == midnight.timestamp()
    assert until >= time.time() + 3000  # asks past "now" so the running hour is included
    assert coordinator.energy == energy
    assert coordinator.energy_last_reset == midnight
    for room, ref in ENERGY.items():
        state = _state(hass, ref)
        assert float(state.state) == pytest.approx(energy[room])
        assert state.attributes["last_reset"] == midnight.isoformat()
        assert state.attributes["state_class"] == "total"
        assert state.attributes["unit_of_measurement"] == "kWh"


async def test_energy_failure_warns_once_and_keeps_previous_values(
    hass, setup_integration, mock_client, energy, clock, caplog
):
    coordinator = _coord(setup_integration)
    await _settle(hass)
    midnight = dt_util.start_of_local_day().isoformat()
    mock_client.get_energy.side_effect = UnavailableRpcError()
    caplog.clear()

    for attempt in (2, 3, 4):
        clock.advance(ENERGY_REFRESH_INTERVAL + 1)
        await coordinator.async_refresh()
        await _settle(hass)
        assert mock_client.get_energy.call_count == attempt
        assert coordinator.last_update_success is True  # the poll itself is fine

    assert len(_energy_warnings(caplog)) == 1
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    for room, ref in ENERGY.items():
        state = _state(hass, ref)
        assert float(state.state) == pytest.approx(energy[room])
        assert state.attributes["last_reset"] == midnight

    # Recovery picks up new totals, and a later failure warns again.
    energy[DINING] = 1.5
    _serve_energy(mock_client, energy)
    clock.advance(ENERGY_REFRESH_INTERVAL + 1)
    await coordinator.async_refresh()
    await _settle(hass)
    assert _state(hass, ENERGY[DINING]).state == "1.5"

    mock_client.get_energy.side_effect = OSError("Network is down")
    caplog.clear()
    clock.advance(ENERGY_REFRESH_INTERVAL + 1)
    await coordinator.async_refresh()
    await _settle(hass)
    assert len(_energy_warnings(caplog)) == 1
    assert _state(hass, ENERGY[DINING]).state == "1.5"


async def test_energy_is_refetched_only_after_the_refresh_interval(
    hass, setup_integration, mock_client, energy, clock
):
    coordinator = _coord(setup_integration)
    await _settle(hass)
    assert mock_client.get_energy.call_count == 1

    energy[BEDROOM] = 3.5
    clock.advance(ENERGY_REFRESH_INTERVAL - 1)
    await coordinator.async_refresh()
    await _settle(hass)
    assert mock_client.get_energy.call_count == 1
    assert _state(hass, ENERGY[BEDROOM]).state == "3.31"

    clock.advance(2)
    await coordinator.async_refresh()
    await _settle(hass)
    assert mock_client.get_energy.call_count == 2
    assert _state(hass, ENERGY[BEDROOM]).state == "3.5"


@pytest.mark.freeze_time("2026-09-25 23:58:00-07:00")
async def test_energy_is_refetched_as_soon_as_the_local_day_changes(
    hass, freezer, config_entry, mock_client, fake_stream, clock
):
    """The first poll of a new day reads energy at once, closes out yesterday, then resets.

    The read starts at yesterday's reset so the finished day's final total is
    reported under the old last_reset before the sensors restart for the new day.
    """
    yesterday = dt_util.start_of_local_day()
    assert yesterday.isoformat() == "2026-09-25T00:00:00-07:00"
    today = dt_util.start_of_local_day(yesterday + timedelta(hours=26))
    noon, eleven_pm = (int((yesterday + timedelta(hours=h)).timestamp()) for h in (12, 23))
    # Quilt's hourly metering as of 23:58; the 23:00 bucket is still growing.
    metered = {
        DINING: {noon: 0.8, eleven_pm: 0.063},
        LIVING: {noon: 1.2, eleven_pm: 0.066},
        BEDROOM: {noon: 3.0, eleven_pm: 0.31},
    }
    _serve_buckets(mock_client, metered)
    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await _settle(hass)
    coordinator = _coord(config_entry)
    assert mock_client.get_energy.call_count == 1
    as_of_2358 = {DINING: 0.863, LIVING: 1.266, BEDROOM: 3.31}
    for room, ref in ENERGY.items():
        assert float(_state(hass, ref).state) == pytest.approx(as_of_2358[room])

    clock.advance(60)
    await coordinator.async_refresh()
    await _settle(hass)
    assert mock_client.get_energy.call_count == 1  # same day, interval not up

    # Quilt meters the last minutes of the day and the first of the next.
    final_yesterday = {DINING: 0.9, LIVING: 1.3, BEDROOM: 3.4}
    early_today = {DINING: 0.004, LIVING: 0.0, BEDROOM: 0.021}
    for room in ROOMS:
        metered[room][eleven_pm] = round(final_yesterday[room] - metered[room][noon], 3)
        metered[room][int(today.timestamp())] = early_today[room]
    reported: dict[str, list[tuple[float, str]]] = {room: [] for room in ROOMS}
    rooms_by_entity = {_eid(hass, ref): room for room, ref in ENERGY.items()}

    @callback
    def _record(event) -> None:
        room = rooms_by_entity.get(event.data["entity_id"])
        if room and event.data["new_state"]:
            new = event.data["new_state"]
            reported[room].append((float(new.state), new.attributes.get("last_reset")))

    unsub = hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    freezer.move_to("2026-09-26 00:00:30-07:00")
    clock.advance(60)  # still well inside ENERGY_REFRESH_INTERVAL
    await coordinator.async_refresh()
    await _settle(hass)
    unsub()

    assert dt_util.start_of_local_day() == today
    assert today.isoformat() == "2026-09-26T00:00:00-07:00"
    assert mock_client.get_energy.call_count == 2
    assert mock_client.get_energy.call_args.args[0] == yesterday.timestamp()
    assert coordinator.energy_last_reset == today
    for room, ref in ENERGY.items():
        # Yesterday's cycle ends on its final total, then today's starts.
        assert reported[room] == [
            (pytest.approx(final_yesterday[room]), yesterday.isoformat()),
            (pytest.approx(early_today[room]), today.isoformat()),
        ], room
        state = _state(hass, ref)
        assert float(state.state) == pytest.approx(early_today[room])
        assert state.attributes["last_reset"] == today.isoformat()

    # The new day doesn't keep re-fetching every poll.
    clock.advance(60)
    await coordinator.async_refresh()
    await _settle(hass)
    assert mock_client.get_energy.call_count == 2


@pytest.mark.freeze_time("2026-09-25 23:58:00-07:00")
async def test_failed_new_day_energy_read_waits_the_interval_then_closes_out_the_day(
    hass, freezer, setup_integration, mock_client, energy, clock
):
    coordinator = _coord(setup_integration)
    await _settle(hass)
    yesterday = dt_util.start_of_local_day()
    assert mock_client.get_energy.call_count == 1

    freezer.move_to("2026-09-26 00:00:30-07:00")
    today = dt_util.start_of_local_day()
    mock_client.get_energy.side_effect = UnavailableRpcError()
    clock.advance(60)
    await coordinator.async_refresh()
    await _settle(hass)
    assert mock_client.get_energy.call_count == 2  # the new day is due at once
    # The sensors keep yesterday's cycle until a read succeeds.
    for room, ref in ENERGY.items():
        state = _state(hass, ref)
        assert float(state.state) == pytest.approx(energy[room])
        assert state.attributes["last_reset"] == yesterday.isoformat()

    # The failed attempt is retried on the interval, not on every poll.
    for _ in range(3):
        clock.advance(60)
        await coordinator.async_refresh()
        await _settle(hass)
    assert mock_client.get_energy.call_count == 2

    # The retry still reads from yesterday's reset, so the day is closed out.
    final_yesterday = {DINING: 0.9, LIVING: 1.3, BEDROOM: 3.4}
    early_today = {DINING: 0.004, LIVING: 0.0, BEDROOM: 0.021}
    eleven_pm = int((yesterday + timedelta(hours=23)).timestamp())
    _serve_buckets(mock_client, {
        room: {eleven_pm: final_yesterday[room], int(today.timestamp()): early_today[room]}
        for room in ROOMS
    })
    clock.advance(ENERGY_REFRESH_INTERVAL)
    await coordinator.async_refresh()
    await _settle(hass)
    assert mock_client.get_energy.call_count == 3
    assert mock_client.get_energy.call_args.args[0] == yesterday.timestamp()
    for room, ref in ENERGY.items():
        state = _state(hass, ref)
        assert float(state.state) == pytest.approx(early_today[room])
        assert state.attributes["last_reset"] == today.isoformat()


async def test_energy_fetch_never_delays_or_fails_the_poll(
    hass, setup_integration, mock_client, energy, system, clock
):
    coordinator = _coord(setup_integration)
    await _settle(hass)
    started, release = threading.Event(), threading.Event()

    def slow_energy(since: float, until: float) -> dict:
        started.set()
        assert release.wait(5)
        return {
            sid: [(int(until) - 3600, kwh)]
            for sid, kwh in {DINING: 2.0, LIVING: 2.5, BEDROOM: 4.0}.items()
        }

    get_energy = _in_worker_thread(mock_client, "get_energy")
    get_energy.side_effect = slow_energy
    system["rooms"][DINING]["current_temp"] = 21.0
    clock.advance(ENERGY_REFRESH_INTERVAL + 1)
    try:
        # The poll finishes while the energy call is still blocked.
        await asyncio.wait_for(coordinator.async_refresh(), timeout=3)
        assert await hass.async_add_executor_job(started.wait, 5)
        assert coordinator.last_update_success is True
        await hass.async_block_till_done()  # foreground work only
        assert _state(hass, CLIMATE[DINING]).attributes["current_temperature"] == 21.0
        assert _state(hass, ENERGY[DINING]).state == "0.863"

        # While that fetch is in flight, a due poll doesn't start a second one.
        clock.advance(ENERGY_REFRESH_INTERVAL + 1)
        await asyncio.wait_for(coordinator.async_refresh(), timeout=3)
        assert get_energy.call_count == 2
    finally:
        release.set()
    await _settle(hass)
    assert _state(hass, ENERGY[DINING]).state == "2.0"
    assert _state(hass, ENERGY[BEDROOM]).state == "4.0"


async def test_energy_failure_at_setup_does_not_block_setup(
    hass, config_entry, mock_client, fake_stream, caplog
):
    mock_client.get_energy.side_effect = UnavailableRpcError()
    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await _settle(hass)

    assert config_entry.state is ConfigEntryState.LOADED
    assert _coord(config_entry).last_update_success is True
    for ref in ENERGY.values():
        assert _state(hass, ref).state == STATE_UNAVAILABLE
    for ref in LIVE_ENTITIES:
        assert _state(hass, ref).state != STATE_UNAVAILABLE, ref
    assert len(_energy_warnings(caplog)) == 1


# --- (6) catch-up on (re)connect --------------------------------------------
async def test_connect_catch_up_is_skipped_within_grace_and_requested_after(
    hass, setup_integration, mock_client, fake_stream, clock
):
    polls = mock_client.get_system.call_count

    clock.advance(CATCH_UP_GRACE - 1)
    fake_stream.instance.on_connect()
    await hass.async_block_till_done()
    assert mock_client.get_system.call_count == polls  # a read just finished

    clock.advance(2)  # now past the grace
    fake_stream.instance.on_connect()
    await hass.async_block_till_done()
    assert mock_client.get_system.call_count == polls + 1

    # That catch-up read restarts the grace.
    clock.advance(1)
    fake_stream.instance.on_connect()
    await hass.async_block_till_done()
    assert mock_client.get_system.call_count == polls + 1


async def test_connect_catch_up_counts_only_successful_reads(
    hass, setup_integration, mock_client, fake_stream, system, clock
):
    coordinator = _coord(setup_integration)
    clock.advance(CATCH_UP_GRACE + 5)
    mock_client.get_system.side_effect = OSError("Network is down")
    await _refresh(hass, coordinator)  # a failed read doesn't count as "just read"
    _serve(mock_client.get_system, system)
    polls = mock_client.get_system.call_count

    clock.advance(1)
    fake_stream.instance.on_connect()
    await hass.async_block_till_done()
    assert mock_client.get_system.call_count == polls + 1
    assert coordinator.last_update_success is True


# --- (7) topics and resubscription ------------------------------------------
async def test_topics_cover_rooms_units_and_the_dial(hass, setup_integration, fake_stream):
    topics = fake_stream.instance.topics_fn()
    assert sorted(topics) == sorted([
        *(f"hds/space/{r}" for r in ROOMS),
        *(f"hds/indoor_unit/unit-{r}" for r in ROOMS),
        f"hds/controller/{DIAL_ID}",
    ])


@pytest.mark.parametrize("dial", [None, {"id": None, "name": "Dial", "temperature": 20.0}])
async def test_no_controller_topic_without_a_dial_id(
    hass, config_entry, mock_client, fake_stream, system, dial
):
    system["dial"] = dial
    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done()
    topics = fake_stream.instance.topics_fn()
    assert not [t for t in topics if t.startswith("hds/controller/")]
    assert len(topics) == 6


async def test_resubscribe_is_checked_after_polls_and_pushes(
    hass, setup_integration, mock_client, fake_stream, system
):
    coordinator = _coord(setup_integration)
    stream = fake_stream.instance
    await _settle(hass)

    count = len(stream.resubscribed)
    await _refresh(hass, coordinator)
    assert len(stream.resubscribed) == count + 1
    assert stream.resubscribed[-1] == stream.topics_fn()
    assert f"hds/controller/{DIAL_ID}" in stream.resubscribed[-1]

    # A room added in the Quilt app is followed after the next read.
    system["rooms"]["space-office"] = make_room("space-office", "Office")
    await _refresh(hass, coordinator)
    assert "hds/space/space-office" in stream.resubscribed[-1]
    assert "hds/indoor_unit/unit-space-office" in stream.resubscribed[-1]

    # Pushed changes also re-check the topic list.
    count = len(stream.resubscribed)
    await _push(hass, fake_stream, [{"kind": "dial", "dial_id": DIAL_ID, "temperature": 19.5}])
    assert len(stream.resubscribed) == count + 1

    # After unload the stream is stopped and the topic listener is gone.
    assert await hass.config_entries.async_unload(setup_integration.entry_id)
    await hass.async_block_till_done()
    assert stream.stopped
    count = len(stream.resubscribed)
    coordinator.async_update_listeners()
    assert len(stream.resubscribed) == count


# --- entity ids vs platform setup order (found while writing these tests) ----
async def _setup_with_climate_order(hass, config_entry, *, climate_first: bool) -> None:
    """Set up with the climate platform forced to add its entities first or last.

    async_forward_entry_setups sets the platforms up concurrently, so either
    order can happen on a real install.
    """
    from custom_components.quilt import binary_sensor as bs_mod
    from custom_components.quilt import climate as climate_mod
    from custom_components.quilt import sensor as sensor_mod

    registry = er.async_get(hass)

    async def wait_for(refs: list[tuple[str, str]]) -> None:
        for _ in range(500):
            if all(registry.async_get_entity_id(p, DOMAIN, u) for p, u in refs):
                return
            await asyncio.sleep(0)
        pytest.fail(f"precondition failed: {refs} never registered")

    def after(refs, orig):
        async def setup_entry(hass, entry, add_entities):
            await wait_for(refs)
            await orig(hass, entry, add_entities)
        return setup_entry

    room_refs = [OCCUPANCY[DINING], HUMIDITY[DINING], ENERGY[DINING]]
    if climate_first:
        patches = [
            patch.object(bs_mod, "async_setup_entry",
                         after([CLIMATE[DINING]], bs_mod.async_setup_entry)),
            patch.object(sensor_mod, "async_setup_entry",
                         after([CLIMATE[DINING]], sensor_mod.async_setup_entry)),
        ]
    else:
        patches = [patch.object(climate_mod, "async_setup_entry",
                                after(room_refs, climate_mod.async_setup_entry))]
    for p in patches:
        p.start()
    try:
        config_entry.add_to_hass(hass)
        assert await hass.config_entries.async_setup(config_entry.entry_id)
        await hass.async_block_till_done()
    finally:
        for p in patches:
            p.stop()


async def test_room_sensor_entity_ids_when_climate_is_set_up_first(
    hass, config_entry, mock_client, fake_stream
):
    await _setup_with_climate_order(hass, config_entry, climate_first=True)
    assert _eid(hass, CLIMATE[DINING]) == "climate.dining_room"
    assert _eid(hass, OCCUPANCY[DINING]) == "binary_sensor.dining_room_occupancy"
    assert _eid(hass, HUMIDITY[DINING]) == "sensor.dining_room_humidity"
    assert _eid(hass, ENERGY[DINING]) == "sensor.dining_room_energy_today"


async def test_room_sensor_entity_ids_do_not_depend_on_platform_setup_order(
    hass, config_entry, mock_client, fake_stream
):
    """Sensors set up before climate still get room-named entity ids.

    Every room entity passes the room device's full DeviceInfo (name,
    manufacturer, model). With only identifiers (a 'link'), a sensor platform
    that registered the device first created it nameless, and the ids became
    binary_sensor.occupancy, sensor.humidity, sensor.energy_today (then _2, _3).
    """
    await _setup_with_climate_order(hass, config_entry, climate_first=False)
    assert _eid(hass, CLIMATE[DINING]) == "climate.dining_room"
    assert {
        "occupancy": _eid(hass, OCCUPANCY[DINING]),
        "humidity": _eid(hass, HUMIDITY[DINING]),
        "energy": _eid(hass, ENERGY[DINING]),
    } == {
        "occupancy": "binary_sensor.dining_room_occupancy",
        "humidity": "sensor.dining_room_humidity",
        "energy": "sensor.dining_room_energy_today",
    }
