"""Quilt cloud client: Cognito auth + HomeDatastoreService gRPC.

Pure (no Home Assistant deps) so it can be exercised standalone. All calls are
blocking; the HA layer wraps them with async_add_executor_job.

Ported from the homebridge-quilt plugin (auth.js / quiltClient.js / accessory.js)
and reauth.py (passwordless CUSTOM_AUTH email-code login).
"""
from __future__ import annotations

import base64
from collections.abc import Callable
import json
import logging
import math
import random
import struct
import threading
import time
import urllib.error
import urllib.request

import grpc

from . import quilt_pb2 as pb
from . import quilt_pb2_grpc as pbg

# --- Quilt Cognito identifiers (captured from the app) ---
REGION = "us-west-2"
CLIENT_ID = "6lef74vtc8p7pgu47nmqubd9vn"
USER_POOL_ID = "us-west-2_mP0zkCEzn"
GRPC_HOST = "api.prod.quilt.cloud:443"

# Space.control.mode is the room's HVAC mode.
MODE_OFF = 1
MODE_COOL = 2
MODE_HEAT = 3
MODE_HEAT_COOL = 4
MODE_FAN = 5
MODE_DRY = 8
# The modes we write. Quilt can also report 6 and 7, fallbacks it picks on its
# own (named FALLBACK_AUTO / FALLBACK_OFF in the Android app); we only read them.
KNOWN_MODES = frozenset({MODE_OFF, MODE_COOL, MODE_HEAT, MODE_HEAT_COOL, MODE_FAN, MODE_DRY})
MODE_FALLBACK_AUTO = 6
MODE_FALLBACK_OFF = 7
OFF_MODES = frozenset({MODE_OFF, MODE_FALLBACK_OFF})

# Space.state field 4 (our proto calls it SpaceSensor.valid): what the unit is
# doing right now, as reported by the unit. Values from the Android app's
# HVACState enum; verified live that a cooling room reports 2 and an off room 1.
HVAC_STATE_STANDBY = 1
HVAC_STATE_COOL = 2
HVAC_STATE_HEAT = 3
HVAC_STATE_DRIFT = 4
HVAC_STATE_FAN = 5
HVAC_STATE_COOL_DEFERRED = 6
HVAC_STATE_HEAT_DEFERRED = 7
HVAC_STATE_FAN_DEFERRED = 8
HVAC_STATE_COOL_PREPARING = 9
HVAC_STATE_HEAT_PREPARING = 10
HVAC_STATE_DRY = 11
HVAC_STATE_DRY_DEFERRED = 12
HVAC_STATE_DRY_PREPARING = 13

# Quilt "Off" preset sentinels: a very low heat / very high cool threshold
# disables that side, which is how we express heat-only / cool-only.
HEAT_DISABLED = 8.0
COOL_DISABLED = 40.0

_COGNITO_URL = f"https://cognito-idp.{REGION}.amazonaws.com/"


class QuiltAuthError(Exception):
    """Raised when Cognito auth fails (bad/expired/revoked token or code)."""


def is_revoked(err: Exception) -> bool:
    """True when the refresh token itself was rejected (the user must log in again)."""
    return "revoked" in str(err).lower() or "NotAuthorized" in str(err)


def _cognito(target: str, body: dict, *, region: str = REGION) -> dict:
    """POST to the Cognito IDP JSON API."""
    req = urllib.request.Request(
        f"https://cognito-idp.{region}.amazonaws.com/",
        data=json.dumps(body).encode(),
        headers={
            "content-type": "application/x-amz-json-1.1",
            "x-amz-target": "AWSCognitoIdentityProviderService." + target,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as err:
        detail = err.read().decode()[:400]
        raise QuiltAuthError(f"Cognito {target} {err.code}: {detail}") from err


def _jwt_exp(id_token: str) -> int:
    """Return the `exp` epoch claim from a JWT (no signature check)."""
    part = id_token.split(".")[1]
    part += "=" * (-len(part) % 4)
    claims = json.loads(base64.urlsafe_b64decode(part))
    return int(claims.get("exp", 0))


def _now_ts() -> pb.Timestamp:
    return pb.Timestamp(seconds=int(time.time()), nanos=0)


# ----------------------------------------------------------------------------
# Minimal protobuf wire reader
# ----------------------------------------------------------------------------
# The generated stubs only declare spaces (field 3) and comfort_settings
# (field 13). The richer per-room telemetry lives in collections the proto
# discards — indoor units (field 9, one head per room) and the dial (field 11).
# We read those straight from the raw response bytes. Extraction is read-only
# and best-effort: a decode hiccup yields None, never an exception upstream.

def _read_varint(buf: bytes, i: int) -> tuple[int, int]:
    shift = result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def _looks_text(bs: bytes) -> bool:
    try:
        s = bs.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return len(s) > 0 and all(31 < ord(c) < 127 or c in "\t\n\r" for c in s)


def _decode(buf: bytes) -> dict:
    """Decode protobuf bytes into {field_number: [values, ...]}.

    Nested messages -> dict, length-delimited text -> str, 32-bit -> float,
    varint -> int. Unknown/odd wire types stop the scan (best-effort).
    """
    out: dict = {}
    i, n = 0, len(buf)
    while i < n:
        try:
            tag, i = _read_varint(buf, i)
            field, wt = tag >> 3, tag & 7
            if wt == 0:
                val, i = _read_varint(buf, i)
            elif wt == 5:
                val = struct.unpack("<f", buf[i:i + 4])[0]
                i += 4
            elif wt == 1:
                val = struct.unpack("<d", buf[i:i + 8])[0]
                i += 8
            elif wt == 2:
                ln, i = _read_varint(buf, i)
                sub = buf[i:i + ln]
                i += ln
                val = sub.decode("utf-8") if _looks_text(sub) else _decode(sub)
            else:
                break
        except (IndexError, struct.error):
            break
        out.setdefault(field, []).append(val)
    return out


def _first(node, *path):
    """Walk nested _decode() dicts by field number, taking the first of each."""
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key][0]
    return node


def _finite(value):
    return value if isinstance(value, float) and math.isfinite(value) else None


def _space_state(s) -> dict:
    """The live fields of a Space message, shared by full reads and pushes."""
    state: dict = {}
    if s.HasField("control"):
        state["mode"] = s.control.mode
        state["on"] = s.control.mode not in OFF_MODES
        state["active_comfort_id"] = s.control.comfort_id
        # Field 5 holds the heat setpoint in every mode; field 2 tracks
        # whichever setpoint the current mode targets.
        heat = _finite(s.control.heat_setpoint2 or s.control.heat_setpoint)
        cool = _finite(s.control.cool_setpoint)
        if heat is not None:
            state["heat_setpoint"] = heat
        if cool is not None:
            state["cool_setpoint"] = cool
    if s.HasField("sensor"):
        temp = _finite(s.sensor.current_temp)
        if temp is not None:
            state["current_temp"] = temp
        if s.sensor.valid:  # 0 = not reported
            state["hvac_state"] = s.sensor.valid
    return state


def _unit_state(unit: dict) -> tuple[str | None, dict]:
    """(space id, live fields) from one decoded indoor unit (head)."""
    state: dict = {}
    occ = _first(unit, 7, 2)  # presence flag: 2 occupied, 1 vacant
    if occ is not None:
        state["occupied"] = occ == 2
    hum = _finite(_first(unit, 5, 11))  # head humidity (% RH)
    if hum is not None:
        state["humidity"] = round(hum)
    return _first(unit, 2, 2), state  # first ref = space id


def _dial_state(dial: dict) -> dict:
    """Live fields of the Dial (controller) from a full read or a push.

    Its field 4 block holds the room temperature the Dial reports (f5, °C) and
    unidentified channels f8-f10. f3 is not humidity: over four days it tracked
    temperature (r=+0.85) and ran opposite to room humidity, matching the
    Android app's "PCB temperature" label, so it isn't exposed.
    """
    state: dict = {}
    for key, field in (("temperature", 5), ("ambient_1", 8),
                       ("ambient_2", 9), ("ambient_3", 10)):
        value = _first(dial, 4, field)
        if isinstance(value, float):
            value = _finite(value)
        if isinstance(value, (int, float)):
            state[key] = value
    return state


def _iter_fields(buf: bytes):
    """Yield (field, wire_type, value) with length-delimited values as raw bytes."""
    i, n = 0, len(buf)
    while i < n:
        tag, i = _read_varint(buf, i)
        field, wt = tag >> 3, tag & 7
        if wt == 0:
            val, i = _read_varint(buf, i)
        elif wt == 2:
            ln, i = _read_varint(buf, i)
            val = buf[i:i + ln]
            i += ln
        elif wt == 5:
            val = buf[i:i + 4]
            i += 4
        elif wt == 1:
            val = buf[i:i + 8]
            i += 8
        else:
            return
        yield field, wt, val


def _raw_field(buf: bytes, field: int) -> bytes | None:
    for f, wt, val in _iter_fields(buf):
        if f == field and wt == 2:
            return val
    return None


# ----------------------------------------------------------------------------
# Passwordless email-code login (used by the config flow to mint a refresh token)
# ----------------------------------------------------------------------------

def begin_email_login(email: str, *, client_id: str = CLIENT_ID,
                      region: str = REGION) -> tuple[str, str]:
    """Start CUSTOM_AUTH; Quilt emails a 6-digit code. Returns (session, username)."""
    res = _cognito(
        "InitiateAuth",
        {"AuthFlow": "CUSTOM_AUTH", "ClientId": client_id,
         "AuthParameters": {"USERNAME": email}},
        region=region,
    )
    return res["Session"], res.get("ChallengeParameters", {}).get("USERNAME", email)


def complete_email_login(session: str, username: str, code: str, *,
                         client_id: str = CLIENT_ID, region: str = REGION) -> str:
    """Answer the emailed code; returns a long-lived refresh token."""
    res = _cognito(
        "RespondToAuthChallenge",
        {"ChallengeName": "CUSTOM_CHALLENGE", "ClientId": client_id,
         "Session": session, "ChallengeResponses": {"USERNAME": username, "ANSWER": code}},
        region=region,
    )
    ar = res.get("AuthenticationResult")
    if not ar or "RefreshToken" not in ar:
        raise QuiltAuthError(f"login failed: {json.dumps(res)[:300]}")
    return ar["RefreshToken"]


class CognitoAuth:
    """Trades a refresh token for short-lived IdTokens, cached until ~2min before expiry."""

    def __init__(self, refresh_token: str, *, client_id: str = CLIENT_ID,
                 region: str = REGION) -> None:
        self._refresh_token = refresh_token
        self._client_id = client_id
        self._region = region
        self._id_token: str | None = None
        self._exp = 0
        self._lock = threading.Lock()

    def id_token(self) -> str:
        with self._lock:
            return self._fresh_id_token()

    def _fresh_id_token(self) -> str:
        now = time.time()
        if self._id_token and now < self._exp - 120:
            return self._id_token
        res = _cognito(
            "InitiateAuth",
            {"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": self._client_id,
             "AuthParameters": {"REFRESH_TOKEN": self._refresh_token}},
            region=self._region,
        )
        ar = res.get("AuthenticationResult")
        if not ar or "IdToken" not in ar:
            raise QuiltAuthError("no IdToken in refresh response (token revoked?)")
        self._id_token = ar["IdToken"]
        self._exp = _jwt_exp(self._id_token)
        return self._id_token


# ----------------------------------------------------------------------------
# gRPC client over HomeDatastoreService
# ----------------------------------------------------------------------------

class QuiltClient:
    def __init__(self, auth: CognitoAuth, system_id: str) -> None:
        self._auth = auth
        self._system_id = system_id
        self._channel = grpc.secure_channel(GRPC_HOST, grpc.ssl_channel_credentials())
        self._stub = pbg.HomeDatastoreServiceStub(self._channel)
        # Raw passthrough of the read RPC so we can decode collections the
        # generated proto discards (indoor units, dial) from the wire bytes.
        self._raw_get_home = self._channel.unary_unary(
            "/core.protos.home_datastore.HomeDatastoreService/GetHomeDatastoreSystem",
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b,
        )
        # Hand-encoded like the notifier, so no second generated proto (and no
        # descriptor-pool name clashes) is needed.
        self._raw_energy = self._channel.unary_unary(
            "/core.protos.app.SystemInformationService/GetEnergyMetrics",
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b,
        )

    def close(self) -> None:
        self._channel.close()

    def _meta(self):
        # Quilt sends the raw IdToken JWT as the `authorization` metadata value.
        return (("authorization", self._auth.id_token()),)

    def _get_home_raw(self) -> bytes:
        req = pb.GetHomeDatastoreSystemRequest(
            system_id=self._system_id).SerializeToString()
        return self._raw_get_home(req, metadata=self._meta(), timeout=20)

    def get_home(self):
        # Parse from raw bytes; unknown fields are ignored by FromString.
        return pb.HomeDatastoreSystem.FromString(self._get_home_raw())

    def get_rooms(self) -> list[dict]:
        """Per-room model mirroring the homebridge plugin's getRooms()."""
        return self._rooms_from_home(self.get_home())

    def _rooms_from_home(self, home) -> list[dict]:
        comfort_by_space: dict[str, list] = {}
        for cs in home.comfort_settings:
            sid = cs.space_ref.space_id if cs.HasField("space_ref") else None
            if sid:
                comfort_by_space.setdefault(sid, []).append(cs)

        rooms = []
        for s in home.spaces:
            # Rooms are the spaces under the home. SpaceSettings f5 (our
            # "space_type") is 2 for every room only because auto-away is on;
            # the Android app names it the occupancy mode, so switching
            # auto-away off would have hidden the room.
            if not s.HasField("parent") or not s.parent.parent_id:
                continue
            presets = {}
            for c in comfort_by_space.get(s.meta.id, []):
                presets[c.value.name] = {
                    "id": c.meta.id,
                    "meta_updated": pb.Timestamp(seconds=c.meta.updated.seconds,
                                                 nanos=c.meta.updated.nanos),
                    "value": c.value,  # keep the ComfortValue message for writes
                    "heat": c.value.heat_setpoint,
                    "cool": c.value.cool_setpoint,
                    "mode": c.value.f8,
                }
            rooms.append({
                "id": s.meta.id,
                "name": s.info.name,
                "space_updated": pb.Timestamp(seconds=s.meta.updated.seconds,
                                              nanos=s.meta.updated.nanos),
                "current_temp": None,
                # Only the indoor unit reports humidity; the space sensor's
                # second field is not a humidity reading.
                "humidity": None,
                "mode": MODE_OFF,
                "on": False,
                "heat_setpoint": None,
                "cool_setpoint": None,
                "active_comfort_id": None,
                **_space_state(s),
                "presets": presets,
                # Filled in by get_system() from the indoor-unit (head) telemetry.
                "occupied": None,
                "unit_serial": None,
                "unit_id": None,
            })
        return rooms

    def get_system(self) -> dict:
        """One read -> {"rooms": {id: room}, "dial": {...} | None}.

        Rooms are enriched from each indoor head's telemetry: per-room
        occupancy (presence sensor) and humidity (the space-level humidity
        field reads 0; the real value is on the head unit).
        """
        raw = self._get_home_raw()
        rooms = {r["id"]: r for r in self._rooms_from_home(
            pb.HomeDatastoreSystem.FromString(raw))}
        extras = _decode(raw)

        for unit in extras.get(9, []):  # indoor units (one head per room)
            space_id, state = _unit_state(unit)
            room = rooms.get(space_id)
            if not room:
                continue
            room["unit_id"] = _first(unit, 1, 1)
            room["unit_serial"] = _first(unit, 3, 1)
            room.update(state)

        dial = None
        draw = (extras.get(11) or [None])[0]
        if isinstance(draw, dict):
            name = _first(draw, 3, 1)
            dial = {
                "id": _first(draw, 1, 1),
                "name": name if isinstance(name, str) else "Quilt Dial",
                "temperature": None,
                "ambient_1": None,
                "ambient_2": None,
                "ambient_3": None,
                **_dial_state(draw),
            }

        return {"rooms": rooms, "dial": dial}

    def get_energy_today(self, since: float, until: float) -> dict[str, float]:
        """kWh per room since `since` (epoch s), from Quilt's hourly energy buckets.

        The bucket for the current hour is marked incomplete and grows until
        the hour ends; it is included so the total tracks the day as it goes.
        """
        req = (_len_delimited(1, self._system_id.encode())
               + _len_delimited(2, _timestamp(int(since)))
               + _len_delimited(3, _timestamp(int(until)))
               + b"\x20\x01")  # preferred_resolution = HOURLY
        raw = self._raw_energy(req, metadata=self._meta(), timeout=20)
        totals: dict[str, float] = {}
        for metrics in _decode(raw).get(1, []):
            if not isinstance(metrics, dict):
                continue
            space_id = _first(metrics, 1)
            if not isinstance(space_id, str):
                continue
            total = 0.0
            for bucket in metrics.get(3, []):
                if not isinstance(bucket, dict):
                    continue
                start = _first(bucket, 1, 1)
                kwh = _finite(_first(bucket, 3))
                if isinstance(start, int) and start >= since and kwh is not None:
                    total += kwh
            totals[space_id] = total
        return totals

    def _fresh_room(self, room_id: str) -> dict:
        for room in self.get_rooms():
            if room["id"] == room_id:
                return room
        raise QuiltAuthError(f"room not found: {room_id}")

    def _space_update(self, room: dict, *, mode: int, heat: float, cool: float,
                      comfort_id: str) -> pb.SpaceUpdate:
        return pb.SpaceUpdate(
            ref=pb.Ref(id=room["id"], updated=room["space_updated"],
                       system_id=self._system_id),
            value=pb.SpaceUpdateValue(
                mode=mode,
                heat_setpoint=0.0 if mode == MODE_OFF else heat if mode == MODE_HEAT else cool,
                cool_setpoint=cool, heat_setpoint2=heat, f8=2,
                comfort_id=comfort_id, updated=_now_ts()),
        )

    @staticmethod
    def resume_mode(room: dict) -> int:
        """The mode to use when turning a room on: its last active mode, else Cool."""
        ap = room["presets"].get("Active")
        mode = ap["mode"] if ap else None
        return mode if mode in KNOWN_MODES and mode != MODE_OFF else MODE_COOL

    def set_active(self, room_id: str, on: bool) -> int:
        """Turn a room off (Off preset) or back on in its last mode. Returns the mode."""
        room = self._fresh_room(room_id)
        preset = room["presets"].get("Active" if on else "Off")
        if not preset:
            raise QuiltAuthError(f"no {'Active' if on else 'Off'} preset for {room['name']}")
        mode = self.resume_mode(room) if on else MODE_OFF
        upd = self._space_update(room, mode=mode, heat=preset["heat"], cool=preset["cool"],
                                 comfort_id=preset["id"])
        self._stub.UpdateSpace(pb.UpdateSpaceRequest(update=upd),
                               metadata=self._meta(), timeout=20)
        return mode

    def set_preset(self, room_id: str, preset_name: str) -> int:
        """Apply a named comfort preset (Active/Eco/Sleep/Off), keeping the room's mode.

        Returns the mode written.
        """
        room = self._fresh_room(room_id)
        preset = room["presets"].get(preset_name)
        if not preset:
            raise QuiltAuthError(f"no {preset_name} preset for {room['name']}")
        if preset_name.lower() == "off":
            mode = MODE_OFF
        elif room["on"] and room["mode"] in KNOWN_MODES:
            mode = room["mode"]
        else:
            mode = self.resume_mode(room)
        upd = self._space_update(room, mode=mode, heat=preset["heat"], cool=preset["cool"],
                                 comfort_id=preset["id"])
        self._stub.UpdateSpace(pb.UpdateSpaceRequest(update=upd),
                               metadata=self._meta(), timeout=20)
        return mode

    def set_setpoints(self, room_id: str, *, heat: float | None = None,
                      cool: float | None = None, mode: int | None = None) -> int:
        """Store setpoints (and mode) in the Active preset and apply them.

        With no mode given, a room that is on keeps its current mode and a room
        that is off stays off (the setpoints apply on the next turn-on). Returns
        the room's mode after the write.
        """
        room = self._fresh_room(room_id)
        ap = room["presets"].get("Active")
        if not ap:
            raise QuiltAuthError(f"no Active preset for {room['name']}")
        if mode is not None and (mode not in KNOWN_MODES or mode == MODE_OFF):
            raise ValueError(f"not an active Quilt mode: {mode}")
        apply = mode is not None or room["on"]
        if mode is None:
            mode = room["mode"] if room["on"] else MODE_OFF
        new_heat = heat if heat is not None else ap["heat"]
        new_cool = cool if cool is not None else ap["cool"]

        # The Quilt app keeps the Active preset's f8 equal to the room mode (and
        # f9 at 3 for Fan, 4 otherwise); schedules re-apply the preset, so a
        # stale f8 would switch the mode back.
        value = pb.ComfortValue()
        value.CopyFrom(ap["value"])
        value.heat_setpoint = new_heat
        value.cool_setpoint = new_cool
        if mode != MODE_OFF:
            value.f8 = mode
            value.f9 = 3 if mode == MODE_FAN else 4
        value.ts.CopyFrom(_now_ts())
        comfort_upd = pb.ComfortUpdate(
            ref=pb.Ref(id=ap["id"], updated=ap["meta_updated"], system_id=self._system_id),
            value=value)
        self._stub.UpdateComfortSetting(
            pb.UpdateComfortSettingRequest(update=comfort_upd),
            metadata=self._meta(), timeout=20)
        if not apply:
            return MODE_OFF

        room2 = self._fresh_room(room_id)  # fresh concurrency token
        upd = self._space_update(room2, mode=mode, heat=new_heat, cool=new_cool,
                                 comfort_id=ap["id"])
        try:
            self._stub.UpdateSpace(pb.UpdateSpaceRequest(update=upd),
                                   metadata=self._meta(), timeout=20)
        except Exception:
            self._restore_preset(room2, ap["value"])
            raise
        return mode

    def _restore_preset(self, room: dict, original: pb.ComfortValue) -> None:
        """Best effort: put the Active preset back after a failed UpdateSpace."""
        ap = room["presets"].get("Active")
        if not ap:
            return
        value = pb.ComfortValue()
        value.CopyFrom(original)
        value.ts.CopyFrom(_now_ts())
        try:
            self._stub.UpdateComfortSetting(
                pb.UpdateComfortSettingRequest(update=pb.ComfortUpdate(
                    ref=pb.Ref(id=ap["id"], updated=ap["meta_updated"],
                               system_id=self._system_id),
                    value=value)),
                metadata=self._meta(), timeout=20)
        except Exception:  # noqa: BLE001 - the caller re-raises the original error
            pass


# ----------------------------------------------------------------------------
# NotifierService push stream
# ----------------------------------------------------------------------------
#
# The Quilt app keeps a bidirectional NotifierService.Subscribe stream open and
# the cloud pushes each subscribed object (topic "hds/<type>/<id>") whenever it
# changes, plus a steady trickle of telemetry. Each data event carries the whole
# object in the same shape as GetHomeDatastoreSystem, so it goes through the
# same parsers.

NOTIFIER_SUBSCRIBE = "/core.protos.notifier.NotifierService/Subscribe"
CONTROL_TOPIC_APPENDED = 1
CONTROL_RECONNECT_REQUEST = 5
_LOGGER = logging.getLogger(__name__)


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _len_delimited(field: int, payload: bytes) -> bytes:
    return _varint((field << 3) | 2) + _varint(len(payload)) + payload


def _timestamp(seconds: int) -> bytes:
    """google.protobuf.Timestamp{seconds}."""
    return b"\x08" + _varint(seconds)


def subscribe_request(topics: list[str]) -> bytes:
    """SubscribeRequest{append: {subscriptions: [{topic}, ...]}}."""
    subs = b"".join(_len_delimited(1, _len_delimited(1, t.encode())) for t in topics)
    return _len_delimited(2, subs)


def parse_notifier_frame(raw: bytes) -> list[dict]:
    """Decode one Subscribe response into events.

    Returns dicts of kind "space" (space_id + _space_state fields), "unit"
    (space_id, unit_id + _unit_state fields), "dial" (dial_id + _dial_state
    fields) or "control" (type + topics). Heartbeats and anything
    unrecognised yield nothing.
    """
    events: list[dict] = []
    try:
        for f, wt, wrapper in _iter_fields(raw):
            if f != 1 or wt != 2:
                continue
            for kind, kwt, body in _iter_fields(wrapper):
                if kwt != 2:
                    continue
                if kind == 2:
                    d = _decode(body)
                    events.append({"kind": "control", "type": _first(d, 2),
                                   "topics": d.get(1, [])})
                elif kind == 1:
                    events.extend(_data_events(body))
    except Exception:  # noqa: BLE001 - a bad frame must never kill the stream
        _LOGGER.debug("Unparseable Quilt notifier frame (%d bytes)", len(raw))
    return events


def _data_events(event: bytes) -> list[dict]:
    any_msg = _raw_field(event, 2)
    notification = _raw_field(any_msg, 2) if any_msg else None
    diff = _raw_field(notification, 2) if notification else None
    if not diff:
        return []
    events = []
    obj = pb.HomeDatastoreSystem.FromString(diff)
    for s in obj.spaces:
        events.append({"kind": "space", "space_id": s.meta.id, **_space_state(s)})
    decoded = _decode(diff)
    for unit in decoded.get(9, []):
        if not isinstance(unit, dict):
            continue
        space_id, state = _unit_state(unit)
        # Diffs have always carried the room link so far; the unit id lets the
        # coordinator place one that doesn't.
        unit_id = _first(unit, 1, 1)
        if space_id or unit_id:
            events.append({"kind": "unit", "space_id": space_id, "unit_id": unit_id, **state})
    for dial in decoded.get(11, []):
        if isinstance(dial, dict):
            events.append({"kind": "dial", "dial_id": _first(dial, 1, 1), **_dial_state(dial)})
    return events


class NotifierStream:
    """Keeps one NotifierService.Subscribe stream open on a background thread.

    Reconnects with jittered backoff, on a Quilt RECONNECT_REQUEST, when the
    stream goes silent (the cloud normally sends something every few seconds),
    and when the topic list changes. Callbacks run on the stream thread.
    """

    SILENCE_TIMEOUT = 90.0
    MAX_BACKOFF = 60.0
    HEALTHY_AFTER = 30.0  # a stream this old counts as healthy, resetting backoff

    def __init__(self, auth: CognitoAuth, topics: Callable[[], list[str]],
                 on_events: Callable[[list[dict]], None],
                 on_connect: Callable[[], None]) -> None:
        self._auth = auth
        self._topics = topics
        self._on_events = on_events
        self._on_connect = on_connect
        self._stop = threading.Event()
        self._call = None
        self._subscribed: frozenset[str] = frozenset()
        self._last_frame = 0.0
        self._connected = False
        self._auth_warned = False
        self._error_logged = False
        self._revoked = False
        self._thread: threading.Thread | None = None

    @property
    def healthy(self) -> bool:
        """Subscribed and hearing from Quilt (it sends something every few seconds)."""
        return self._connected and time.monotonic() - self._last_frame < self.SILENCE_TIMEOUT

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="quilt-notifier", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        call = self._call
        if call is not None:
            call.cancel()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def resubscribe_if_changed(self, topics: list[str]) -> None:
        """Restart the stream when the rooms/units to follow have changed."""
        call = self._call
        if call is not None and frozenset(topics) != self._subscribed:
            call.cancel()

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                connected = self._stream_once()
            except Exception:  # noqa: BLE001 - the thread must outlive any one failure
                connected = False
                if not self._error_logged:
                    _LOGGER.exception("Quilt notifier failed; retrying")
                    self._error_logged = True
            if self._stop.is_set():
                break
            if self._revoked:
                # The poll raises ConfigEntryAuthFailed; wait for the reload.
                self._stop.wait()
                break
            if connected and time.monotonic() - started >= self.HEALTHY_AFTER:
                backoff = 1.0
            self._stop.wait(backoff * random.uniform(0.5, 1.5))
            backoff = min(backoff * 2, self.MAX_BACKOFF)

    def _stream_once(self) -> bool:
        """Run one stream until it ends. Returns True if it connected."""
        topics = self._topics()
        if not topics:
            return False
        try:
            token = self._auth.id_token()
        except QuiltAuthError as err:
            if is_revoked(err):
                _LOGGER.warning("Quilt notifier stopped: Quilt rejected the login (%s)", err)
                self._revoked = True
            elif not self._auth_warned:
                _LOGGER.warning("Quilt notifier can't refresh its login; retrying: %s", err)
                self._auth_warned = True
            return False
        except (urllib.error.URLError, OSError) as err:
            _LOGGER.debug("Quilt notifier can't reach Cognito: %s", err)
            return False
        except Exception as err:  # noqa: BLE001 - e.g. a truncated or non-JSON reply
            if not self._auth_warned:
                _LOGGER.warning("Quilt notifier can't refresh its login; retrying: %r", err)
                self._auth_warned = True
            return False
        self._auth_warned = False
        if self._stop.is_set():
            return False
        done = threading.Event()
        connected = False

        def requests():
            yield subscribe_request(topics)
            # Hold the request side open; the stream ends only by cancel.
            while not done.wait(5):
                pass

        channel = grpc.secure_channel(
            GRPC_HOST, grpc.ssl_channel_credentials(),
            options=[("grpc.keepalive_time_ms", 30_000),
                     ("grpc.keepalive_timeout_ms", 10_000),
                     ("grpc.keepalive_permit_without_calls", 1)],
        )
        watchdog = threading.Thread(target=self._watch, args=(done,), daemon=True)
        call = None
        try:
            call = channel.stream_stream(NOTIFIER_SUBSCRIBE,
                                         request_serializer=lambda b: b,
                                         response_deserializer=lambda b: b)(
                requests(), metadata=(("authorization", token),))
            self._call = call
            self._subscribed = frozenset(topics)
            # stop() may have run before self._call was set; it can't see this call.
            if self._stop.is_set():
                return False
            self._last_frame = time.monotonic()
            watchdog.start()
            for frame in call:
                self._last_frame = time.monotonic()
                data = []
                reconnect = False
                for ev in parse_notifier_frame(frame):
                    if ev["kind"] != "control":
                        data.append(ev)
                    elif ev["type"] == CONTROL_TOPIC_APPENDED:
                        pass
                    elif ev["type"] == CONTROL_RECONNECT_REQUEST:
                        reconnect = True
                    elif ev["type"] is not None:
                        _LOGGER.warning("Quilt notifier control event %s for %s",
                                        ev["type"], ev["topics"])
                    if not connected and (ev["kind"] != "control"
                                          or ev["type"] == CONTROL_TOPIC_APPENDED):
                        connected = self._connected = True
                        self._error_logged = False
                        _LOGGER.debug("Quilt notifier subscribed to %d topics", len(topics))
                        self._on_connect()
                if data:
                    self._on_events(data)
                if reconnect:
                    _LOGGER.debug("Quilt notifier asked us to reconnect")
                    return connected
        except grpc.RpcError as err:
            if not self._stop.is_set():
                _LOGGER.debug("Quilt notifier stream ended: %s", err.code())
        except Exception:  # noqa: BLE001 - keep the thread alive; the poll still runs
            if not self._error_logged:
                _LOGGER.exception("Quilt notifier stream failed")
                self._error_logged = True
        finally:
            done.set()
            self._connected = False
            self._call = None
            if call is not None:
                call.cancel()
            channel.close()
        return connected

    def _watch(self, done: threading.Event) -> None:
        while not done.wait(15):
            if time.monotonic() - self._last_frame > self.SILENCE_TIMEOUT:
                _LOGGER.debug("Quilt notifier silent for %.0fs; reconnecting",
                              self.SILENCE_TIMEOUT)
                call = self._call
                if call is not None:
                    call.cancel()
                return
