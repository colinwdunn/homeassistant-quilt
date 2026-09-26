"""Unit tests for the pure cloud client (custom_components/quilt/api.py).

No Home Assistant and no network: inputs are built with the generated protobuf
module (quilt_pb2) where it declares the message, and hand-encoded with
api._len_delimited / api._varint (plus struct for 32-bit floats) where it
doesn't (indoor units, the Dial, notifier frames, energy metrics). Where the
client hand-encodes a request, the bytes are checked against an independently
built protobuf schema (a private DescriptorPool) rather than api's own reader.
"""
from __future__ import annotations

import math
import random
import struct
from unittest.mock import MagicMock, patch

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
import pytest

from custom_components.quilt import api
from custom_components.quilt import quilt_pb2 as pb

SYSTEM_ID = "sys-1"
HOME = "space-home"
DINING, LIVING, BEDROOM = "space-dining", "space-living", "space-bedroom"
DIAL_ID = "dial-1"
NAN = float("nan")
INF = float("inf")

# --------------------------------------------------------------------------
# wire helpers
# --------------------------------------------------------------------------
L = api._len_delimited
V = api._varint


def vint(field: int, value: int) -> bytes:
    """A varint field (wire type 0)."""
    return V(field << 3) + V(value)


def f32(field: int, value: float) -> bytes:
    """A 32-bit float field (wire type 5)."""
    return V((field << 3) | 5) + struct.pack("<f", value)


def text(field: int, value: str) -> bytes:
    return L(field, value.encode())


def msg(field: int, *parts: bytes) -> bytes:
    return L(field, b"".join(parts))


def ts(field: int, seconds: int) -> bytes:
    """A Timestamp{1: seconds} sub-message, hand-encoded."""
    return msg(field, vint(1, seconds))


def meta_bytes(obj_id: str, seconds: int = 1_760_000_000) -> bytes:
    """A realistic Meta (id + created/updated timestamps + system id)."""
    return pb.Meta(
        id=obj_id,
        created=pb.Timestamp(seconds=seconds - 1000),
        updated=pb.Timestamp(seconds=seconds, nanos=7),
        system_id=SYSTEM_ID,
    ).SerializeToString()


def unit_bytes(unit_id: str | None = "unit-1", *, space_id: str | None = DINING,
               serial: str | None = "QS1-DIN", humidity: float | None = 52.4,
               presence: int | None = 2) -> bytes:
    """One indoor unit (head) message body, shaped like field 9 of the system."""
    parts = []
    if unit_id is not None:
        parts.append(L(1, meta_bytes(unit_id)))
    if space_id is not None:
        parts.append(msg(2, text(2, space_id)))  # relationships: f2 = space id
    if serial is not None:
        parts.append(msg(3, text(1, serial)))
    if humidity is not None:
        # f11 is the head's humidity; give the block some other channels too.
        parts.append(msg(5, f32(1, 22.5), f32(11, humidity), vint(12, 3)))
    if presence is not None:
        parts.append(msg(7, vint(1, 1), vint(2, presence)))
    return b"".join(parts)


def dial_bytes(dial_id: str | None = DIAL_ID, *, name: str | None = "Dial QD1-0B000VG2S",
               temperature: float | None = 25.25, board: float | None = 31.5,
               ambient: tuple = (15298, 5119, 7)) -> bytes:
    """The Dial (controller) message body, shaped like field 11 of the system."""
    parts = []
    if dial_id is not None:
        parts.append(L(1, meta_bytes(dial_id)))
    if name is not None:
        parts.append(msg(3, text(1, name)))
    state = []
    if board is not None:
        state.append(f32(3, board))  # PCB temperature, not humidity
    if temperature is not None:
        state.append(f32(5, temperature))
    for field, value in zip((8, 9, 10), ambient):
        if value is None:
            continue
        state.append(f32(field, value) if isinstance(value, float) else vint(field, value))
    if state:
        parts.append(msg(4, *state))
    return b"".join(parts)


def make_space(space_id: str, *, parent: str | None = HOME, name: str | None = None,
               space_type: int = 2, control: dict | None = None,
               sensor: dict | None = None) -> pb.Space:
    s = pb.Space(meta=pb.Meta(id=space_id, updated=pb.Timestamp(seconds=1_760_000_000, nanos=9),
                              system_id=SYSTEM_ID))
    if parent is not None:
        s.parent.parent_id = parent
    s.info.name = name or space_id
    s.info.space_type = space_type
    if control is not None:
        s.control.CopyFrom(pb.SpaceControl(**control))
    if sensor is not None:
        s.sensor.CopyFrom(pb.SpaceSensor(**sensor))
    return s


def data_frame(diff: bytes, topic: str = "hds/space/x") -> bytes:
    """A Subscribe response carrying one data event whose diff is `diff`.

    frame{1: wrapper{1: event{2: Any{1: type_url, 2: Notification{1: topic, 2: diff}}}}}
    """
    notification = text(1, topic) + L(2, diff)
    any_msg = text(1, "type.googleapis.com/core.protos.notifier.Notification") + L(2, notification)
    event = L(2, any_msg)
    return msg(1, L(1, event))


def control_frame(ctype: int, topics: list[str]) -> bytes:
    """frame{1: wrapper{2: Control{1: topic..., 2: type}}}."""
    control = b"".join(text(1, t) for t in topics) + vint(2, ctype)
    return msg(1, L(2, control))


# --------------------------------------------------------------------------
# An independently built schema for the hand-encoded RPCs (private pool, so
# nothing clashes with the integration's generated descriptors).
# --------------------------------------------------------------------------
_F = descriptor_pb2.FieldDescriptorProto


def _build_test_schema():
    fdp = descriptor_pb2.FileDescriptorProto(
        name="quilt_api_test_schema.proto", package="quilttest", syntax="proto3")

    def add(message, name, number, ftype, *, repeated=False, type_name=None):
        f = message.field.add(name=name, number=number, type=ftype,
                              label=_F.LABEL_REPEATED if repeated else _F.LABEL_OPTIONAL)
        if type_name:
            f.type_name = type_name

    t = fdp.message_type.add(name="Ts")
    add(t, "seconds", 1, _F.TYPE_INT64)
    add(t, "nanos", 2, _F.TYPE_INT32)

    req = fdp.message_type.add(name="EnergyRequest")
    add(req, "system_id", 1, _F.TYPE_STRING)
    add(req, "start", 2, _F.TYPE_MESSAGE, type_name=".quilttest.Ts")
    add(req, "end", 3, _F.TYPE_MESSAGE, type_name=".quilttest.Ts")
    add(req, "resolution", 4, _F.TYPE_UINT32)

    bucket = fdp.message_type.add(name="Bucket")
    add(bucket, "start", 1, _F.TYPE_MESSAGE, type_name=".quilttest.Ts")
    add(bucket, "status", 2, _F.TYPE_UINT32)
    add(bucket, "kwh", 3, _F.TYPE_FLOAT)

    space = fdp.message_type.add(name="SpaceEnergy")
    add(space, "space_id", 1, _F.TYPE_STRING)
    add(space, "buckets", 3, _F.TYPE_MESSAGE, repeated=True, type_name=".quilttest.Bucket")

    resp = fdp.message_type.add(name="EnergyResponse")
    add(resp, "spaces", 1, _F.TYPE_MESSAGE, repeated=True, type_name=".quilttest.SpaceEnergy")

    sub = fdp.message_type.add(name="Subscription")
    add(sub, "topic", 1, _F.TYPE_STRING)
    append = fdp.message_type.add(name="Append")
    add(append, "subscriptions", 1, _F.TYPE_MESSAGE, repeated=True,
        type_name=".quilttest.Subscription")
    sreq = fdp.message_type.add(name="SubscribeRequest")
    add(sreq, "append", 2, _F.TYPE_MESSAGE, type_name=".quilttest.Append")

    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return {name: message_factory.GetMessageClass(pool.FindMessageTypeByName(f"quilttest.{name}"))
            for name in ("EnergyRequest", "EnergyResponse", "SubscribeRequest")}


SCHEMA = _build_test_schema()


# --------------------------------------------------------------------------
# client without network
# --------------------------------------------------------------------------
@pytest.fixture
def client():
    """A QuiltClient whose gRPC channel is a mock; nothing is dialled."""
    with patch.object(api.grpc, "secure_channel") as secure_channel, \
            patch.object(api.grpc, "ssl_channel_credentials"):
        c = api.QuiltClient(MagicMock(name="CognitoAuth"), SYSTEM_ID)
    secure_channel.assert_called_once()
    assert secure_channel.call_args.args[0] == api.GRPC_HOST
    c._meta = lambda: (("authorization", "id-token"),)
    return c


# ==========================================================================
# constants
# ==========================================================================
def test_mode_constants_match_quilt():
    assert (api.MODE_OFF, api.MODE_COOL, api.MODE_HEAT, api.MODE_HEAT_COOL,
            api.MODE_FAN, api.MODE_DRY) == (1, 2, 3, 4, 5, 8)
    assert (api.MODE_FALLBACK_AUTO, api.MODE_FALLBACK_OFF) == (6, 7)
    assert api.OFF_MODES == {1, 7}
    # The fallbacks are read-only: never among the modes we write.
    assert 6 not in api.KNOWN_MODES and 7 not in api.KNOWN_MODES


def test_hvac_state_constants_match_quilt():
    assert [
        api.HVAC_STATE_STANDBY, api.HVAC_STATE_COOL, api.HVAC_STATE_HEAT,
        api.HVAC_STATE_DRIFT, api.HVAC_STATE_FAN, api.HVAC_STATE_COOL_DEFERRED,
        api.HVAC_STATE_HEAT_DEFERRED, api.HVAC_STATE_FAN_DEFERRED,
        api.HVAC_STATE_COOL_PREPARING, api.HVAC_STATE_HEAT_PREPARING,
        api.HVAC_STATE_DRY, api.HVAC_STATE_DRY_DEFERRED, api.HVAC_STATE_DRY_PREPARING,
    ] == list(range(1, 14))


# ==========================================================================
# low-level wire helpers
# ==========================================================================
@pytest.mark.parametrize("n", [0, 1, 127, 128, 300, 16_383, 16_384, 2**31 - 1,
                               1_760_000_000, 2**35 + 5, 2**63 - 1])
def test_varint_and_timestamp_match_protobuf(n):
    assert api._read_varint(V(n), 0) == (n, len(V(n)))
    assert pb.Timestamp.FromString(api._timestamp(n)).seconds == n
    if n:  # protobuf leaves a zero off the wire; ours writes it explicitly (same value)
        # Timestamp{seconds} hand-encoded == what protobuf itself serializes.
        assert api._timestamp(n) == pb.Timestamp(seconds=n).SerializeToString()


def test_varint_known_encodings():
    assert V(0) == b"\x00"
    assert V(127) == b"\x7f"
    assert V(128) == b"\x80\x01"
    assert V(300) == b"\xac\x02"


def test_len_delimited_matches_protobuf():
    assert L(1, b"sys-1") == pb.GetHomeDatastoreSystemRequest(system_id="sys-1").SerializeToString()
    long_id = "x" * 300  # length needs a two-byte varint
    assert L(1, long_id.encode()) == pb.GetHomeDatastoreSystemRequest(
        system_id=long_id).SerializeToString()


def test_decode_reads_every_wire_type():
    buf = (vint(1, 150) + f32(2, 1.5) + V((3 << 3) | 1) + struct.pack("<d", 2.25)
           + text(4, "hello") + msg(5, vint(1, 9)) + vint(1, 151))
    d = api._decode(buf)
    assert d[1] == [150, 151]
    assert d[2] == [1.5]
    assert d[3] == [2.25]
    assert d[4] == ["hello"]
    assert d[5] == [{1: [9]}]
    assert api._first(d, 5, 1) == 9
    assert api._first(d, 6) is None
    assert api._first(d, 4, 1) is None  # can't walk into a string


@pytest.mark.parametrize("buf", [b"\x08", b"\x15\x00\x00", b"\x0a\x05ab", b"\xff\xff\xff",
                                 b"\x0b\x00"])
def test_decode_is_best_effort_on_truncated_input(buf):
    assert isinstance(api._decode(buf), dict)


# ==========================================================================
# _space_state
# ==========================================================================
@pytest.mark.parametrize(("mode", "on"), [
    (api.MODE_OFF, False),
    (api.MODE_COOL, True),
    (api.MODE_HEAT, True),
    (api.MODE_HEAT_COOL, True),
    (api.MODE_FAN, True),
    (api.MODE_FALLBACK_AUTO, True),   # 6: Quilt-chosen auto fallback is running
    (api.MODE_FALLBACK_OFF, False),   # 7: Quilt-chosen off fallback
    (api.MODE_DRY, True),
])
def test_space_state_mode_and_on(mode, on):
    state = api._space_state(make_space(DINING, control={"mode": mode, "comfort_id": "c-1"}))
    assert state["mode"] == mode
    assert state["on"] is on
    assert state["active_comfort_id"] == "c-1"


@pytest.mark.parametrize("hvac_state", list(range(1, 14)))
def test_space_state_hvac_state_from_sensor_valid(hvac_state):
    state = api._space_state(make_space(DINING, sensor={"current_temp": 24.5, "valid": hvac_state}))
    assert state["hvac_state"] == hvac_state
    assert state["current_temp"] == 24.5


def test_space_state_live_example_cooling_room():
    """The live probe: a 76 F room cooling to 75 F reports mode 2 and state 2."""
    room_c, target_c = (76 - 32) / 1.8, (75 - 32) / 1.8
    s = make_space(BEDROOM, control={"mode": api.MODE_COOL, "heat_setpoint": target_c,
                                     "heat_setpoint2": 16.0, "cool_setpoint": target_c},
                   sensor={"current_temp": room_c, "valid": api.HVAC_STATE_COOL})
    state = api._space_state(s)
    assert state["on"] is True
    assert state["hvac_state"] == api.HVAC_STATE_COOL
    assert state["current_temp"] == pytest.approx(room_c, abs=1e-5)
    assert state["cool_setpoint"] == pytest.approx(target_c, abs=1e-5)
    assert state["heat_setpoint"] == 16.0  # field 5, not field 2


def test_space_state_hvac_state_zero_is_not_reported():
    state = api._space_state(make_space(DINING, sensor={"current_temp": 21.0, "valid": 0}))
    assert "hvac_state" not in state
    assert state["current_temp"] == 21.0


def test_space_state_heat_prefers_field_5_and_falls_back_to_field_2():
    both = api._space_state(make_space(DINING, control={
        "mode": api.MODE_COOL, "heat_setpoint": 24.0, "heat_setpoint2": 17.0,
        "cool_setpoint": 24.0}))
    assert both["heat_setpoint"] == 17.0
    assert both["cool_setpoint"] == 24.0

    only2 = api._space_state(make_space(DINING, control={
        "mode": api.MODE_HEAT, "heat_setpoint": 19.5, "cool_setpoint": 26.0}))
    assert only2["heat_setpoint"] == 19.5


@pytest.mark.parametrize("bad", [NAN, INF, -INF])
def test_space_state_drops_non_finite_setpoints_and_temperature(bad):
    s = make_space(DINING, control={"mode": api.MODE_HEAT_COOL, "heat_setpoint": 18.0,
                                    "heat_setpoint2": bad, "cool_setpoint": bad},
                   sensor={"current_temp": bad, "valid": api.HVAC_STATE_STANDBY})
    state = api._space_state(s)
    assert "heat_setpoint" not in state
    assert "cool_setpoint" not in state
    assert "current_temp" not in state
    # The rest of the message still comes through.
    assert state["mode"] == api.MODE_HEAT_COOL
    assert state["hvac_state"] == api.HVAC_STATE_STANDBY


def test_space_state_nan_survives_the_wire_and_is_dropped():
    s = make_space(DINING, control={"mode": api.MODE_COOL, "heat_setpoint2": 16.0,
                                    "cool_setpoint": NAN},
                   sensor={"current_temp": NAN})
    state = api._space_state(pb.Space.FromString(s.SerializeToString()))
    assert state["heat_setpoint"] == 16.0
    assert "cool_setpoint" not in state
    assert "current_temp" not in state


def test_space_state_without_control_or_sensor_is_empty():
    s = pb.Space(meta=pb.Meta(id=DINING))
    assert api._space_state(s) == {}
    only_sensor = api._space_state(make_space(DINING, sensor={"current_temp": 20.0}))
    assert set(only_sensor) == {"current_temp"}


# ==========================================================================
# _unit_state
# ==========================================================================
def test_unit_state_occupied_with_humidity():
    space_id, state = api._unit_state(api._decode(unit_bytes(humidity=52.4, presence=2)))
    assert space_id == DINING
    assert state == {"occupied": True, "humidity": 52}


def test_unit_state_vacant_and_humidity_rounds():
    space_id, state = api._unit_state(api._decode(unit_bytes(humidity=47.6, presence=1)))
    assert space_id == DINING
    assert state == {"occupied": False, "humidity": 48}


def test_unit_state_missing_or_nan_values_are_left_out():
    _, state = api._unit_state(api._decode(unit_bytes(humidity=NAN, presence=None)))
    assert state == {}
    _, state = api._unit_state(api._decode(unit_bytes(humidity=None, presence=None)))
    assert state == {}


def test_unit_state_without_space_link():
    space_id, state = api._unit_state(api._decode(unit_bytes(space_id=None)))
    assert space_id is None
    assert state["occupied"] is True


# ==========================================================================
# _dial_state
# ==========================================================================
def test_dial_state_temperature_and_ambient_channels():
    state = api._dial_state(api._decode(dial_bytes()))
    assert state == {"temperature": 25.25, "ambient_1": 15298, "ambient_2": 5119,
                     "ambient_3": 7}


def test_dial_state_does_not_expose_field_3():
    state = api._dial_state(api._decode(dial_bytes(board=31.5, temperature=25.25)))
    assert "humidity" not in state
    assert 31.5 not in state.values()
    # f3 alone is never promoted to a reading.
    assert api._dial_state(api._decode(dial_bytes(temperature=None, ambient=()))) == {}


def test_dial_state_drops_nan_and_keeps_float_channels():
    state = api._dial_state(api._decode(dial_bytes(temperature=NAN, ambient=(NAN, 2.5, None))))
    assert state == {"ambient_2": 2.5}


def test_dial_state_without_state_block():
    assert api._dial_state(api._decode(dial_bytes(temperature=None, board=None, ambient=()))) == {}
    assert api._dial_state({}) == {}


# ==========================================================================
# parse_notifier_frame
# ==========================================================================
def _system_diff(*spaces: pb.Space, units: tuple[bytes, ...] = (),
                 dials: tuple[bytes, ...] = ()) -> bytes:
    diff = pb.HomeDatastoreSystem(spaces=list(spaces)).SerializeToString()
    diff += b"".join(L(9, u) for u in units)
    diff += b"".join(L(11, d) for d in dials)
    return diff


def test_frame_space_event():
    s = make_space(BEDROOM, control={"mode": api.MODE_COOL, "heat_setpoint2": 16.0,
                                     "cool_setpoint": 24.0, "comfort_id": "bed-active"},
                   sensor={"current_temp": 24.5, "valid": api.HVAC_STATE_COOL})
    events = api.parse_notifier_frame(data_frame(_system_diff(s)))
    assert events == [{
        "kind": "space", "space_id": BEDROOM, "mode": api.MODE_COOL, "on": True,
        "active_comfort_id": "bed-active", "heat_setpoint": 16.0, "cool_setpoint": 24.0,
        "current_temp": 24.5, "hvac_state": api.HVAC_STATE_COOL,
    }]


def test_frame_space_event_fallback_off():
    s = make_space(LIVING, control={"mode": api.MODE_FALLBACK_OFF})
    (event,) = api.parse_notifier_frame(data_frame(_system_diff(s)))
    assert event["mode"] == 7
    assert event["on"] is False


def test_frame_unit_event_with_space_link():
    events = api.parse_notifier_frame(data_frame(
        _system_diff(units=(unit_bytes("unit-dining", space_id=DINING, humidity=55.2),)),
        topic="hds/indoor_unit/unit-dining"))
    assert events == [{"kind": "unit", "space_id": DINING, "unit_id": "unit-dining",
                       "occupied": True, "humidity": 55}]


def test_frame_unit_event_without_space_link_keeps_unit_id():
    events = api.parse_notifier_frame(data_frame(
        _system_diff(units=(unit_bytes("unit-living", space_id=None, presence=1),))))
    assert events == [{"kind": "unit", "space_id": None, "unit_id": "unit-living",
                       "occupied": False, "humidity": 52}]


def test_frame_unit_without_space_link_or_id_is_dropped():
    events = api.parse_notifier_frame(data_frame(
        _system_diff(units=(unit_bytes(None, space_id=None),))))
    assert events == []


def test_frame_unit_without_space_link_and_id_only_meta_keeps_unit_id():
    """Regression: a Meta carrying only a UUID-length id is decoded as a message.

    Its bytes ('\\n$<id>') are all printable, which used to make _decode() read the
    whole Meta as a string, so the unit id was lost and a unit diff without its space
    link was dropped. Bytes starting with a tag byte (\\n, \\t, \\r) are now treated as
    a message, so the unit-id fallback still works.
    """
    unit_id = "3f2b8c1e-9a4d-4e7b-8f10-2c6d5e4a1b90"
    unit = msg(1, text(1, unit_id)) + msg(7, vint(2, 2))  # Meta{id} only, no relationships
    events = api.parse_notifier_frame(data_frame(_system_diff(units=(unit,))))
    assert events == [{"kind": "unit", "space_id": None, "unit_id": unit_id, "occupied": True}]


def test_frame_unit_id_with_full_meta_survives_uuid_length_ids():
    """The same UUID-length id is read fine when Meta also carries its timestamps."""
    unit_id = "3f2b8c1e-9a4d-4e7b-8f10-2c6d5e4a1b90"
    events = api.parse_notifier_frame(data_frame(
        _system_diff(units=(unit_bytes(unit_id, space_id=None, humidity=None),))))
    assert events == [{"kind": "unit", "space_id": None, "unit_id": unit_id, "occupied": True}]


def test_frame_dial_event():
    events = api.parse_notifier_frame(data_frame(
        _system_diff(dials=(dial_bytes(),)), topic=f"hds/controller/{DIAL_ID}"))
    assert events == [{"kind": "dial", "dial_id": DIAL_ID, "temperature": 25.25,
                       "ambient_1": 15298, "ambient_2": 5119, "ambient_3": 7}]
    assert "humidity" not in events[0]


def test_frame_dial_event_without_readings_still_names_the_dial():
    events = api.parse_notifier_frame(data_frame(
        _system_diff(dials=(dial_bytes(temperature=None, board=31.0, ambient=()),))))
    assert events == [{"kind": "dial", "dial_id": DIAL_ID}]


def test_frame_mixed_diff_yields_every_event_in_order():
    s = make_space(DINING, control={"mode": api.MODE_OFF})
    events = api.parse_notifier_frame(data_frame(_system_diff(
        s, units=(unit_bytes("unit-dining"),), dials=(dial_bytes(),))))
    assert [e["kind"] for e in events] == ["space", "unit", "dial"]
    assert events[0]["on"] is False


def test_frame_with_two_wrappers():
    s1 = make_space(DINING, control={"mode": api.MODE_HEAT})
    s2 = make_space(LIVING, control={"mode": api.MODE_DRY})
    raw = data_frame(_system_diff(s1)) + data_frame(_system_diff(s2))
    events = api.parse_notifier_frame(raw)
    assert [(e["space_id"], e["mode"]) for e in events] == [(DINING, 3), (LIVING, 8)]


def test_frame_control_topic_appended():
    topics = [f"hds/space/{DINING}", f"hds/controller/{DIAL_ID}"]
    events = api.parse_notifier_frame(control_frame(api.CONTROL_TOPIC_APPENDED, topics))
    assert api.CONTROL_TOPIC_APPENDED == 1
    assert events == [{"kind": "control", "type": 1, "topics": topics}]


def test_frame_control_reconnect_request():
    events = api.parse_notifier_frame(control_frame(api.CONTROL_RECONNECT_REQUEST, []))
    assert api.CONTROL_RECONNECT_REQUEST == 5
    assert events == [{"kind": "control", "type": 5, "topics": []}]


def test_frame_heartbeat_yields_nothing():
    assert api.parse_notifier_frame(b"\n\x00") == []
    assert api.parse_notifier_frame(b"") == []


def test_frame_data_event_without_diff_yields_nothing():
    event = L(2, text(1, "type.googleapis.com/x"))  # Any with no value
    assert api.parse_notifier_frame(msg(1, L(1, event))) == []


@pytest.mark.parametrize("raw", [
    b"\xff\xff\xff\xff",
    b"\x0a\x05ab",                     # truncated wrapper
    b"\x0b\x00",                       # group wire type
    b"\x0a\x03\x0a\x10\x00",           # length past the end, nested
    b"\x0a\x04\x0a\x02\x12\xff",       # truncated event
    b"not a protobuf frame at all",
    b"\x0a" + b"\x80" * 12,            # runaway varint length
])
def test_frame_garbage_never_raises(raw):
    assert api.parse_notifier_frame(raw) == []


def test_frame_random_bytes_never_raise():
    rng = random.Random(1234)
    for _ in range(500):
        raw = bytes(rng.randrange(256) for _ in range(rng.randrange(1, 64)))
        assert isinstance(api.parse_notifier_frame(raw), list)


def test_frame_truncations_never_raise():
    full = data_frame(_system_diff(
        make_space(DINING, control={"mode": 2}), units=(unit_bytes(),), dials=(dial_bytes(),)))
    for cut in range(len(full)):
        assert isinstance(api.parse_notifier_frame(full[:cut]), list)


def test_frame_with_undecodable_diff_yields_nothing():
    # The diff is not a HomeDatastoreSystem (a space field that isn't a message).
    assert api.parse_notifier_frame(data_frame(L(3, b"\xff\xff"))) == []


# ==========================================================================
# rooms: spaces with a parent space id
# ==========================================================================
def _home(*spaces: pb.Space, comfort: list | None = None) -> pb.HomeDatastoreSystem:
    return pb.HomeDatastoreSystem(spaces=list(spaces), comfort_settings=comfort or [])


def _comfort(space_id: str, name: str, heat: float, cool: float, mode: int) -> pb.ComfortSetting:
    return pb.ComfortSetting(
        meta=pb.Meta(id=f"{space_id}-{name.lower()}",
                     updated=pb.Timestamp(seconds=1_760_000_100, nanos=3)),
        value=pb.ComfortValue(name=name, heat_setpoint=heat, cool_setpoint=cool, f8=mode),
        space_ref=pb.ComfortSpaceRef(space_id=space_id),
    )


def test_rooms_are_spaces_with_a_parent(client):
    home = _home(
        # The building: no parent, even though its space_type is 2.
        make_space(HOME, parent=None, name="Home", space_type=2),
        make_space(DINING, name="Dining Room", space_type=2),
        # Auto-away off: space_type 0, still a room because it has a parent.
        make_space(LIVING, name="Living Room", space_type=0),
        # Parent message present but no parent id: not a room.
        make_space("space-orphan", parent="", name="Orphan", space_type=2),
    )
    home.spaces[3].parent.SetInParent()
    rooms = client._rooms_from_home(pb.HomeDatastoreSystem.FromString(home.SerializeToString()))
    assert [r["id"] for r in rooms] == [DINING, LIVING]
    assert [r["name"] for r in rooms] == ["Dining Room", "Living Room"]


def test_room_space_type_zero_is_a_room(client):
    rooms = client._rooms_from_home(_home(make_space(LIVING, space_type=0)))
    assert [r["id"] for r in rooms] == [LIVING]


def test_building_space_type_two_is_not_a_room(client):
    assert client._rooms_from_home(_home(make_space(HOME, parent=None, space_type=2))) == []


def test_room_fields_and_presets(client):
    home = _home(
        make_space(HOME, parent=None, space_type=0),
        make_space(BEDROOM, name="Primary Bedroom",
                   control={"mode": api.MODE_COOL, "heat_setpoint2": 16.0,
                            "cool_setpoint": 24.0, "comfort_id": f"{BEDROOM}-active"},
                   sensor={"current_temp": 24.5, "humidity": 0.0, "valid": 2}),
        make_space(DINING, name="Dining Room"),  # no control / sensor
        comfort=[
            _comfort(BEDROOM, "Active", 16.0, 24.0, api.MODE_COOL),
            _comfort(BEDROOM, "Off", api.HEAT_DISABLED, api.COOL_DISABLED, api.MODE_OFF),
            _comfort(HOME, "Active", 18.0, 25.0, api.MODE_HEAT),  # the building's
        ],
    )
    rooms = {r["id"]: r for r in client._rooms_from_home(home)}
    assert set(rooms) == {BEDROOM, DINING}

    bed = rooms[BEDROOM]
    assert bed["name"] == "Primary Bedroom"
    assert bed["mode"] == api.MODE_COOL and bed["on"] is True
    assert bed["heat_setpoint"] == 16.0 and bed["cool_setpoint"] == 24.0
    assert bed["current_temp"] == 24.5
    assert bed["hvac_state"] == api.HVAC_STATE_COOL
    assert bed["active_comfort_id"] == f"{BEDROOM}-active"
    assert bed["humidity"] is None  # the space sensor's f2 is not humidity
    assert bed["occupied"] is None and bed["unit_id"] is None and bed["unit_serial"] is None
    assert bed["space_updated"] == pb.Timestamp(seconds=1_760_000_000, nanos=9)
    assert set(bed["presets"]) == {"Active", "Off"}
    active = bed["presets"]["Active"]
    assert active["id"] == f"{BEDROOM}-active"
    assert (active["heat"], active["cool"], active["mode"]) == (16.0, 24.0, api.MODE_COOL)
    assert active["meta_updated"] == pb.Timestamp(seconds=1_760_000_100, nanos=3)
    assert active["value"].name == "Active"

    dining = rooms[DINING]
    assert dining["mode"] == api.MODE_OFF and dining["on"] is False
    assert dining["heat_setpoint"] is None and dining["cool_setpoint"] is None
    assert dining["current_temp"] is None
    assert dining["presets"] == {}


def test_get_rooms_reads_home_by_system_id(client):
    home = _home(make_space(HOME, parent=None), make_space(DINING))
    client._raw_get_home = MagicMock(return_value=home.SerializeToString())
    rooms = client.get_rooms()
    assert [r["id"] for r in rooms] == [DINING]
    req = client._raw_get_home.call_args.args[0]
    assert pb.GetHomeDatastoreSystemRequest.FromString(req).system_id == SYSTEM_ID
    assert client._raw_get_home.call_args.kwargs["metadata"] == (("authorization", "id-token"),)


# ==========================================================================
# get_system: rooms + indoor units + the Dial
# ==========================================================================
def _system_raw(*, units=(), dials=()) -> bytes:
    home = _home(
        make_space(HOME, parent=None, name="Home", space_type=2),
        make_space(DINING, name="Dining Room", control={"mode": api.MODE_OFF}),
        make_space(BEDROOM, name="Primary Bedroom", space_type=0,
                   control={"mode": api.MODE_COOL, "cool_setpoint": 24.0}),
    )
    return _system_diff(*home.spaces, units=units, dials=dials)


def test_get_system_dial_dict(client):
    client._get_home_raw = lambda: _system_raw(dials=(dial_bytes(),))
    dial = client.get_system()["dial"]
    assert dial == {"id": DIAL_ID, "name": "Dial QD1-0B000VG2S", "temperature": 25.25,
                    "ambient_1": 15298, "ambient_2": 5119, "ambient_3": 7}
    assert "humidity" not in dial


def test_get_system_dial_defaults(client):
    client._get_home_raw = lambda: _system_raw(dials=(
        dial_bytes(name=None, temperature=None, ambient=(15298,)),))
    dial = client.get_system()["dial"]
    assert dial == {"id": DIAL_ID, "name": "Quilt Dial", "temperature": None,
                    "ambient_1": 15298, "ambient_2": None, "ambient_3": None}
    assert "humidity" not in dial


def test_get_system_without_dial(client):
    client._get_home_raw = lambda: _system_raw()
    assert client.get_system()["dial"] is None


def test_get_system_enriches_rooms_from_units(client):
    client._get_home_raw = lambda: _system_raw(units=(
        unit_bytes("unit-dining", space_id=DINING, serial="QS1-DIN", humidity=50.4, presence=1),
        unit_bytes("unit-bed", space_id=BEDROOM, serial="QS1-PRI", humidity=58.5, presence=2),
        unit_bytes("unit-ghost", space_id="space-unknown", serial="QS1-GHO"),
        unit_bytes("unit-home", space_id=HOME),  # the building is not a room
    ))
    system = client.get_system()
    rooms = system["rooms"]
    assert set(rooms) == {DINING, BEDROOM}
    assert rooms[DINING]["unit_id"] == "unit-dining"
    assert rooms[DINING]["unit_serial"] == "QS1-DIN"
    assert rooms[DINING]["occupied"] is False
    assert rooms[DINING]["humidity"] == 50
    assert rooms[BEDROOM]["unit_id"] == "unit-bed"
    assert rooms[BEDROOM]["occupied"] is True
    assert rooms[BEDROOM]["humidity"] == pytest.approx(58, abs=1)
    assert rooms[BEDROOM]["on"] is True


# ==========================================================================
# get_energy_today
# ==========================================================================
HOUR = 3600
MIDNIGHT = 1_760_252_400  # a local midnight (07:00 UTC), on an hour boundary


def _bucket(start: int, status: int, kwh: float) -> bytes:
    return msg(3, ts(1, start), vint(2, status), f32(3, kwh))


def _space_energy(space_id: str, *buckets: bytes) -> bytes:
    return msg(1, text(1, space_id), *buckets)


def _energy_client(client, response: bytes):
    client._raw_energy = MagicMock(return_value=response)
    return client


def test_energy_counts_buckets_since_midnight_including_the_current_hour(client):
    now_hour = MIDNIGHT + 10 * HOUR
    response = (
        _space_energy(
            DINING,
            _bucket(MIDNIGHT - 2 * HOUR, 1, 5.0),     # yesterday: excluded
            _bucket(MIDNIGHT - HOUR, 1, 7.0),         # yesterday: excluded
            _bucket(MIDNIGHT, 1, 0.5),                # first hour of today
            _bucket(MIDNIGHT + HOUR, 1, 0.25),
            _bucket(now_hour, 2, 0.125),              # current hour, incomplete: included
        )
        + _space_energy(
            LIVING,
            _bucket(MIDNIGHT, 1, NAN),                # NaN: skipped
            _bucket(MIDNIGHT + HOUR, 1, 1.5),
            _bucket(now_hour, 2, INF),                # non-finite: skipped
        )
        + _space_energy(BEDROOM, _bucket(MIDNIGHT - HOUR, 1, 3.0))  # nothing today
    )
    totals = _energy_client(client, response).get_energy_today(
        float(MIDNIGHT), float(now_hour + 2 * HOUR))
    assert totals == {DINING: 0.875, LIVING: 1.5, BEDROOM: 0.0}


def test_energy_request_encoding(client):
    _energy_client(client, b"")
    until = MIDNIGHT + 13 * HOUR + 0.75
    assert client.get_energy_today(float(MIDNIGHT), until) == {}

    call = client._raw_energy.call_args
    req_bytes = call.args[0]
    req = SCHEMA["EnergyRequest"].FromString(req_bytes)
    assert req.system_id == SYSTEM_ID
    assert req.start.seconds == MIDNIGHT and req.start.nanos == 0
    assert req.end.seconds == int(until) and req.end.nanos == 0
    assert req.resolution == 1  # hourly
    # Byte-exact against protobuf's own serialization of the same request.
    assert req_bytes == req.SerializeToString()
    assert call.kwargs["metadata"] == (("authorization", "id-token"),)
    assert call.kwargs["timeout"] == 20


def test_energy_decodes_a_protobuf_encoded_response(client):
    """Cross-check the hand decoder against protobuf's own encoder."""
    resp = SCHEMA["EnergyResponse"]()
    for space_id, rows in ((DINING, [(MIDNIGHT - HOUR, 1, 9.0), (MIDNIGHT, 1, 0.863),
                                     (MIDNIGHT + HOUR, 2, 0.4)]),
                           (LIVING, [(MIDNIGHT, 1, 1.266)])):
        entry = resp.spaces.add(space_id=space_id)
        for start, status, kwh in rows:
            b = entry.buckets.add(status=status, kwh=kwh)
            b.start.seconds = start
    totals = _energy_client(client, resp.SerializeToString()).get_energy_today(
        float(MIDNIGHT), float(MIDNIGHT + 2 * HOUR))
    assert totals[DINING] == pytest.approx(0.863 + 0.4, abs=1e-6)
    assert totals[LIVING] == pytest.approx(1.266, abs=1e-6)


def test_energy_hand_encoding_matches_protobuf(client):
    """The hand-built fixture is the same wire format protobuf produces."""
    hand = _space_energy(DINING, _bucket(MIDNIGHT, 2, 0.5))
    resp = SCHEMA["EnergyResponse"]()
    entry = resp.spaces.add(space_id=DINING)
    b = entry.buckets.add(status=2, kwh=0.5)
    b.start.seconds = MIDNIGHT
    assert hand == resp.SerializeToString()


def test_energy_zero_kwh_bucket_and_missing_start(client):
    # proto3 leaves 0.0 kWh off the wire; a bucket with no start can't be placed.
    response = _space_energy(DINING, msg(3, ts(1, MIDNIGHT), vint(2, 1)),
                             msg(3, vint(2, 1), f32(3, 4.0)),
                             _bucket(MIDNIGHT + HOUR, 1, 0.75))
    totals = _energy_client(client, response).get_energy_today(float(MIDNIGHT), MIDNIGHT + 1e4)
    assert totals == {DINING: 0.75}


# ==========================================================================
# subscribe_request
# ==========================================================================
def test_subscribe_request_exact_bytes():
    assert api.subscribe_request(["a"]) == b"\x12\x05\x0a\x03\x0a\x01a"
    assert api.subscribe_request([]) == b"\x12\x00"


def test_subscribe_request_round_trips_through_protobuf():
    topics = [f"hds/space/{DINING}", f"hds/indoor_unit/unit-{BEDROOM}",
              f"hds/controller/{DIAL_ID}", "hds/space/" + "r" * 200]
    raw = api.subscribe_request(topics)
    parsed = SCHEMA["SubscribeRequest"].FromString(raw)
    assert [s.topic for s in parsed.append.subscriptions] == topics
    assert raw == parsed.SerializeToString()


# ==========================================================================
# _space_update (write encoding)
# ==========================================================================
@pytest.mark.parametrize(("mode", "field2"), [
    (api.MODE_OFF, 0.0),      # Off clears field 2
    (api.MODE_HEAT, 19.0),    # Heat: field 2 is the heat target
    (api.MODE_COOL, 24.0),    # Cool: field 2 is the cool target
])
def test_space_update_encoding(client, mode, field2):
    room = {"id": DINING, "space_updated": pb.Timestamp(seconds=1_760_000_000, nanos=9)}
    upd = client._space_update(room, mode=mode, heat=19.0, cool=24.0, comfort_id="c-9")
    wire = pb.UpdateSpaceRequest.FromString(
        pb.UpdateSpaceRequest(update=upd).SerializeToString()).update
    assert wire.ref.id == DINING
    assert wire.ref.system_id == SYSTEM_ID
    assert wire.ref.updated == pb.Timestamp(seconds=1_760_000_000, nanos=9)
    assert wire.value.mode == mode
    assert wire.value.heat_setpoint == field2
    assert wire.value.heat_setpoint2 == 19.0
    assert wire.value.cool_setpoint == 24.0
    assert wire.value.f8 == 2
    assert wire.value.comfort_id == "c-9"
    assert wire.value.updated.seconds > 1_700_000_000
