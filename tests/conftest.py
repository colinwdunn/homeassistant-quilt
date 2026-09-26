"""Shared fixtures: a fake Quilt system shaped like a real 3-room install.

Nothing here touches the network. The cloud client (QuiltClient) and the push
stream (NotifierStream) are replaced where the coordinator imports them, so
tests drive the integration through Home Assistant exactly as a user would and
feed it pushes by calling the coordinator's thread callbacks.
"""
from __future__ import annotations

import copy
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.quilt import api
from custom_components.quilt.const import (
    CONF_EMAIL,
    CONF_REFRESH_TOKEN,
    CONF_SYSTEM_ID,
    DOMAIN,
)

pytest_plugins = "pytest_homeassistant_custom_component"

SYSTEM_ID = "sys-1"
DINING, LIVING, BEDROOM = "space-dining", "space-living", "space-bedroom"
DIAL_ID = "dial-1"


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    yield


def _presets(room_id: str, active_mode: int = api.MODE_COOL) -> dict:
    def preset(name, heat, cool, mode, ptype):
        return {
            "id": f"{room_id}-{name.lower()}",
            "meta_updated": None,
            "value": MagicMock(name=f"ComfortValue {name}"),
            "heat": heat,
            "cool": cool,
            "mode": mode,
        }
    return {
        "Active": preset("Active", 16.0, 24.0, active_mode, 1),
        "Sleep": preset("Sleep", 17.0, 22.0, active_mode, 2),
        "Eco": preset("Eco", 16.1, 31.1, api.MODE_HEAT_COOL, 3),
        "Off": preset("Off", api.HEAT_DISABLED, api.COOL_DISABLED, api.MODE_OFF, 4),
    }


def make_room(room_id: str, name: str, **over: Any) -> dict:
    room = {
        "id": room_id,
        "name": name,
        "space_updated": None,
        "current_temp": 24.0,
        "humidity": 52,
        "mode": api.MODE_OFF,
        "on": False,
        "heat_setpoint": api.HEAT_DISABLED,
        "cool_setpoint": api.COOL_DISABLED,
        "active_comfort_id": f"{room_id}-off",
        "presets": _presets(room_id),
        "occupied": False,
        "unit_serial": f"QS1-{name[:3].upper()}",
        "unit_id": f"unit-{room_id}",
        "hvac_state": api.HVAC_STATE_STANDBY,
    }
    room.update(over)
    return room


def make_system() -> dict:
    """Dining + Living off, Primary Bedroom cooling at 24 °C (like the live probe)."""
    return {
        "rooms": {
            DINING: make_room(DINING, "Dining Room"),
            LIVING: make_room(LIVING, "Living Room"),
            BEDROOM: make_room(
                BEDROOM, "Primary Bedroom", mode=api.MODE_COOL, on=True,
                heat_setpoint=16.0, cool_setpoint=24.0, current_temp=24.5,
                active_comfort_id=f"{BEDROOM}-active", occupied=True,
                hvac_state=api.HVAC_STATE_COOL,
            ),
        },
        "dial": {
            "id": DIAL_ID,
            "name": "Dial QD1-0B000VG2S",
            "temperature": 25.2,
            "ambient_1": 15298,
            "ambient_2": 5119,
            "ambient_3": 0,
        },
    }


class FakeStream:
    """Stands in for api.NotifierStream; tests push through the coordinator callbacks."""

    def __init__(self, auth, topics, on_events, on_connect, on_disconnect=None) -> None:
        self.topics_fn = topics
        self.on_events = on_events
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.healthy = False
        self.started = False
        self.stopped = False
        self.resubscribed: list[list[str]] = []

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def resubscribe_if_changed(self, topics: list[str]) -> None:
        self.resubscribed.append(topics)


@pytest.fixture
def system() -> dict:
    """The data the fake client returns; mutate it to change what the next poll sees."""
    return make_system()


@pytest.fixture
def energy() -> dict[str, float]:
    return {DINING: 0.863, LIVING: 1.266, BEDROOM: 3.31}


@pytest.fixture
def mock_client(system, energy):
    """Patch QuiltClient where the coordinator builds it. Each poll returns a deep copy."""
    client = MagicMock(name="QuiltClient")
    client.get_system.side_effect = lambda: copy.deepcopy(system)
    # One bucket per room starting "now" (the coordinator asks until now + 1 h),
    # so the whole value lands in the current local day.
    client.get_energy.side_effect = lambda since, until: {
        sid: [(int(until) - 3600, kwh)] for sid, kwh in energy.items()
    }
    client.get_energy_today.side_effect = lambda since, until: dict(energy)
    client.set_active.side_effect = lambda room_id, on: api.MODE_COOL if on else api.MODE_OFF
    client.set_setpoints.side_effect = (
        lambda room_id, heat=None, cool=None, mode=None: mode if mode is not None else api.MODE_COOL
    )
    client.set_preset.return_value = api.MODE_COOL
    client.resume_mode = api.QuiltClient.resume_mode
    with patch("custom_components.quilt.coordinator.QuiltClient", return_value=client), \
         patch("custom_components.quilt.coordinator.CognitoAuth"):
        yield client


@pytest.fixture
def fake_stream():
    """Patch NotifierStream; the created instance is available as fake_stream.instance."""
    holder = MagicMock()

    def factory(*args, **kwargs):
        holder.instance = FakeStream(*args, **kwargs)
        return holder.instance

    with patch("custom_components.quilt.coordinator.NotifierStream", side_effect=factory):
        yield holder


@pytest.fixture
def config_entry() -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        title="Quilt",
        unique_id=SYSTEM_ID,
        data={
            CONF_REFRESH_TOKEN: "refresh-token",
            CONF_SYSTEM_ID: SYSTEM_ID,
            CONF_EMAIL: "owner@example.com",
        },
    )


@pytest.fixture
async def setup_integration(hass, config_entry, mock_client, fake_stream):
    """Set the integration up; returns the config entry."""
    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done()
    return config_entry
