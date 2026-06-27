"""Quilt cloud client: Cognito auth + HomeDatastoreService gRPC.

Pure (no Home Assistant deps) so it can be exercised standalone. All calls are
blocking; the HA layer wraps them with async_add_executor_job.

Ported from the homebridge-quilt plugin (auth.js / quiltClient.js / accessory.js)
and reauth.py (passwordless CUSTOM_AUTH email-code login).
"""
from __future__ import annotations

import base64
import json
import struct
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

# Space.control.mode values.
MODE_OFF = 1
MODE_ACTIVE = 2

# Quilt "Off" preset sentinels: a very low heat / very high cool threshold
# disables that side, which is how we express heat-only / cool-only.
HEAT_DISABLED = 8.0
COOL_DISABLED = 40.0

_COGNITO_URL = f"https://cognito-idp.{REGION}.amazonaws.com/"


class QuiltAuthError(Exception):
    """Raised when Cognito auth fails (bad/expired/revoked token or code)."""


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

    def id_token(self) -> str:
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
            if not s.HasField("info") or s.info.space_type != 2:
                continue  # only rooms
            presets = {}
            for c in comfort_by_space.get(s.meta.id, []):
                presets[c.value.name] = {
                    "id": c.meta.id,
                    "meta_updated": pb.Timestamp(seconds=c.meta.updated.seconds,
                                                 nanos=c.meta.updated.nanos),
                    "value": c.value,  # keep the ComfortValue message for writes
                    "heat": c.value.heat_setpoint,
                    "cool": c.value.cool_setpoint,
                }
            mode = s.control.mode if s.HasField("control") else MODE_OFF
            rooms.append({
                "id": s.meta.id,
                "name": s.info.name,
                "space_updated": pb.Timestamp(seconds=s.meta.updated.seconds,
                                              nanos=s.meta.updated.nanos),
                "current_temp": s.sensor.current_temp if s.HasField("sensor") else None,
                "humidity": s.sensor.humidity if s.HasField("sensor") else None,
                "mode": mode,
                "on": mode != MODE_OFF,
                "heat_setpoint": s.control.heat_setpoint if s.HasField("control") else None,
                "cool_setpoint": s.control.cool_setpoint if s.HasField("control") else None,
                "active_comfort_id": s.control.comfort_id if s.HasField("control") else None,
                "presets": presets,
                # Filled in by get_system() from the indoor-unit (head) telemetry.
                "occupied": None,
                "unit_serial": None,
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
            try:
                room = rooms.get(_first(unit, 2, 2))  # first ref = space id
                if not room:
                    continue
                occ = _first(unit, 7, 2)  # presence flag: 2 occupied, 1 vacant
                room["occupied"] = None if occ is None else occ == 2
                hum = _first(unit, 5, 11)  # head humidity (% RH)
                if hum is not None:
                    room["humidity"] = round(hum)
                room["unit_serial"] = _first(unit, 3, 1)
            except (TypeError, ValueError, IndexError):
                continue

        dial = None
        draw = (extras.get(11) or [None])[0]
        if isinstance(draw, dict):
            try:
                dial = {
                    "name": _first(draw, 3, 1) or "Quilt Dial",
                    "temperature": _first(draw, 4, 5),
                    "humidity": _first(draw, 4, 3),
                    "ambient_1": _first(draw, 4, 8),
                    "ambient_2": _first(draw, 4, 9),
                    "ambient_3": _first(draw, 4, 10),
                }
            except (TypeError, ValueError, IndexError):
                dial = None

        return {"rooms": rooms, "dial": dial}

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
                mode=mode, heat_setpoint=heat, cool_setpoint=cool,
                heat_setpoint2=heat, f8=2, comfort_id=comfort_id, updated=_now_ts()),
        )

    def set_active(self, room_id: str, on: bool):
        """Turn a room on (Active preset) or off (Off preset)."""
        room = self._fresh_room(room_id)
        preset = room["presets"].get("Active" if on else "Off")
        if not preset:
            raise QuiltAuthError(f"no {'Active' if on else 'Off'} preset for {room['name']}")
        upd = self._space_update(room, mode=MODE_ACTIVE if on else MODE_OFF,
                                 heat=preset["heat"], cool=preset["cool"],
                                 comfort_id=preset["id"])
        return self._stub.UpdateSpace(pb.UpdateSpaceRequest(update=upd),
                                      metadata=self._meta(), timeout=20)

    def set_preset(self, room_id: str, preset_name: str):
        """Apply a named comfort preset (Active/Eco/Sleep/Off) to a room."""
        room = self._fresh_room(room_id)
        preset = room["presets"].get(preset_name)
        if not preset:
            raise QuiltAuthError(f"no {preset_name} preset for {room['name']}")
        on = preset_name.lower() != "off"
        upd = self._space_update(room, mode=MODE_ACTIVE if on else MODE_OFF,
                                 heat=preset["heat"], cool=preset["cool"],
                                 comfort_id=preset["id"])
        return self._stub.UpdateSpace(pb.UpdateSpaceRequest(update=upd),
                                      metadata=self._meta(), timeout=20)

    def set_setpoints(self, room_id: str, *, heat: float | None = None,
                      cool: float | None = None):
        """Update the room's Active preset setpoints, then apply (activating it)."""
        room = self._fresh_room(room_id)
        ap = room["presets"].get("Active")
        if not ap:
            raise QuiltAuthError(f"no Active preset for {room['name']}")
        new_heat = heat if heat is not None else ap["heat"]
        new_cool = cool if cool is not None else ap["cool"]

        value = pb.ComfortValue()
        value.CopyFrom(ap["value"])
        value.heat_setpoint = new_heat
        value.cool_setpoint = new_cool
        value.ts.CopyFrom(_now_ts())
        comfort_upd = pb.ComfortUpdate(
            ref=pb.Ref(id=ap["id"], updated=ap["meta_updated"], system_id=self._system_id),
            value=value)
        self._stub.UpdateComfortSetting(
            pb.UpdateComfortSettingRequest(update=comfort_upd),
            metadata=self._meta(), timeout=20)

        room2 = self._fresh_room(room_id)  # fresh concurrency token
        upd = self._space_update(room2, mode=MODE_ACTIVE, heat=new_heat, cool=new_cool,
                                 comfort_id=ap["id"])
        return self._stub.UpdateSpace(pb.UpdateSpaceRequest(update=upd),
                                      metadata=self._meta(), timeout=20)
