"""Protocol/data-correctness review checks (fallback-mode writes, energy decoding).

Pure client tests: no Home Assistant, no network. The gRPC stub is a MagicMock
and rooms are handed to the write paths directly, so the assertions read the
exact UpdateSpace / UpdateComfortSetting messages that would go to Quilt.
"""
from __future__ import annotations

import struct
from unittest.mock import MagicMock

import pytest

from custom_components.quilt import api
from custom_components.quilt import quilt_pb2 as pb

SYSTEM_ID = "sys-1"
ROOM = "3f2b8c1e-9a4d-4e7b-8f10-2c6d5e4a1b90"  # UUID-shaped, like real space ids
L = api._len_delimited
V = api._varint


def _client() -> api.QuiltClient:
    client = object.__new__(api.QuiltClient)
    client._system_id = SYSTEM_ID
    client._stub = MagicMock(name="HomeDatastoreServiceStub")
    client._meta = lambda: ()
    return client


def _room(mode: int, *, active_f8: int = api.MODE_HEAT_COOL) -> dict:
    def preset(name, heat, cool, f8):
        return {
            "id": f"{ROOM}-{name.lower()}",
            "meta_updated": pb.Timestamp(seconds=1),
            "value": pb.ComfortValue(name=name, heat_setpoint=heat, cool_setpoint=cool, f8=f8),
            "heat": heat, "cool": cool, "mode": f8,
        }
    return {
        "id": ROOM, "name": "Living Room", "space_updated": pb.Timestamp(seconds=1),
        "mode": mode, "on": mode not in api.OFF_MODES,
        "presets": {
            "Active": preset("Active", 19.0, 25.0, active_f8),
            "Off": preset("Off", api.HEAT_DISABLED, api.COOL_DISABLED, api.MODE_OFF),
        },
    }


def _writes(stub):
    spaces = [c.args[0].update.value for c in stub.UpdateSpace.call_args_list]
    presets = [c.args[0].update.value for c in stub.UpdateComfortSetting.call_args_list]
    return spaces, presets


# --- positive control: an ordinary Heat/Cool room --------------------------------
def test_setpoints_on_heat_cool_room_write_heat_cool():
    client = _client()
    client._fresh_room = lambda room_id: _room(api.MODE_HEAT_COOL)
    assert client.set_setpoints(ROOM, heat=18.0, cool=26.0) == api.MODE_HEAT_COOL
    spaces, presets = _writes(client._stub)
    assert [s.mode for s in spaces] == [api.MODE_HEAT_COOL]
    assert [p.f8 for p in presets] == [api.MODE_HEAT_COOL]


def test_setpoints_on_fallback_auto_room_never_write_mode_6_and_resume_keeps_heat_cool():
    """Regression: a room Quilt has put in FALLBACK_AUTO (6) is written as Heat/Cool.

    set_setpoints(mode=None) goes through current_writable_mode(), which maps 6 to
    Heat/Cool (4), so neither UpdateSpace.mode nor the Active preset's f8 ever gets 6,
    and a later turn-on (resume_mode) still resumes in Heat/Cool rather than Cool.
    """
    client = _client()
    client._fresh_room = lambda room_id: _room(api.MODE_FALLBACK_AUTO)
    client.set_setpoints(ROOM, heat=18.0, cool=26.0)
    spaces, presets = _writes(client._stub)
    written = [s.mode for s in spaces] + [p.f8 for p in presets]

    # What the next turn-on would do with the preset as it was just written.
    stored_f8 = presets[0].f8
    after = _room(api.MODE_OFF, active_f8=stored_f8)
    resumed = api.QuiltClient.resume_mode(after)

    assert all(m in api.KNOWN_MODES for m in written), written
    assert [s.mode for s in spaces] == [api.MODE_HEAT_COOL]
    assert stored_f8 == api.MODE_HEAT_COOL
    assert resumed == api.MODE_HEAT_COOL, (stored_f8, resumed)


@pytest.mark.parametrize("preset_name", ["Active", "Off"])
def test_preset_on_fallback_auto_room_never_writes_mode_6(preset_name):
    """set_preset keeps a FALLBACK_AUTO room's mode as Heat/Cool (Off preset -> Off)."""
    client = _client()
    client._fresh_room = lambda room_id: _room(api.MODE_FALLBACK_AUTO)
    written = client.set_preset(ROOM, preset_name)
    spaces, presets = _writes(client._stub)
    expected = api.MODE_OFF if preset_name == "Off" else api.MODE_HEAT_COOL
    assert written == expected
    assert [s.mode for s in spaces] == [expected]
    assert presets == []


def test_current_writable_mode_maps_fallback_modes():
    """FALLBACK_AUTO (6) is written as Heat/Cool; FALLBACK_OFF (7) counts as off."""
    assert api.QuiltClient.current_writable_mode(_room(api.MODE_FALLBACK_AUTO)) == api.MODE_HEAT_COOL
    assert api.QuiltClient.current_writable_mode(_room(api.MODE_FALLBACK_OFF)) is None
    assert api.QuiltClient.current_writable_mode(_room(api.MODE_COOL)) == api.MODE_COOL


# --- energy: _decode's text heuristic on a SpaceEnergy entry ------------------------
def _f32(field: int, value: float) -> bytes:
    return V((field << 3) | 5) + struct.pack("<f", value)


def _bucket(start: int, status: int, kwh: float) -> bytes:
    body = L(1, V(1 << 3) + V(start)) + V(2 << 3) + V(status) + _f32(3, kwh)
    return L(3, body)


def _entry(space_id: str, *buckets: bytes) -> bytes:
    return L(1, L(1, space_id.encode()) + b"".join(buckets))


MIDNIGHT = 1_760_252_400


def test_energy_room_with_only_yesterdays_buckets_reads_zero():
    """Control: a listed room with no bucket since midnight totals 0.0 kWh."""
    client = _client()
    client._raw_energy = MagicMock(return_value=_entry(ROOM, _bucket(MIDNIGHT - 3600, 1, 2.0)))
    assert client.get_energy_today(float(MIDNIGHT), MIDNIGHT + 7200.0) == {ROOM: 0.0}


def test_energy_room_listed_without_buckets_reads_zero():
    """Regression: a SpaceEnergy entry with no buckets ('\\n$' + a 36-char UUID) still
    counts as a room.

    parse_energy decodes by wire type, so the room maps to [] and get_energy_today
    reports it as 0.0 kWh instead of dropping it (which left the sensor unavailable).
    _decode's text heuristic also reads the leading field-1 tag as a message now.
    """
    raw = _entry(ROOM)
    assert api.parse_energy(raw) == {ROOM: []}
    assert isinstance(api._decode(raw)[1][0], dict)

    client = _client()
    client._raw_energy = MagicMock(return_value=raw)
    assert client.get_energy(float(MIDNIGHT), MIDNIGHT + 7200.0) == {ROOM: []}
    assert client.get_energy_today(float(MIDNIGHT), MIDNIGHT + 7200.0) == {ROOM: 0.0}
