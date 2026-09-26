"""Data update coordinator for Quilt."""
from __future__ import annotations

from datetime import datetime, timedelta
import logging
import time
import urllib.error

import grpc

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.debounce import Debouncer
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .api import CognitoAuth, NotifierStream, QuiltAuthError, QuiltClient, is_revoked
from .const import (
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_SYSTEM_ID,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    ENERGY_REFRESH_INTERVAL,
    REFRESH_COOLDOWN,
)

_LOGGER = logging.getLogger(__name__)

# Pushed fields that change what a room is doing (mode, targets, preset); a
# change here also triggers a full read to pick up the preset details the
# stream doesn't carry.
CONTROL_KEYS = ("mode", "on", "heat_setpoint", "cool_setpoint", "active_comfort_id")
LIVE_KEYS = ("current_temp", "occupied", "humidity", "hvac_state")
DIAL_KEYS = ("temperature", "ambient_1", "ambient_2", "ambient_3")
# A (re)connect skips its catch-up read when a full read finished this recently.
CATCH_UP_GRACE = 10.0


def scan_interval(entry: ConfigEntry) -> timedelta:
    return timedelta(seconds=entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL))


def _rpc_code(err: grpc.RpcError) -> str:
    code = getattr(err, "code", None)
    try:
        return str(code()) if callable(code) else type(err).__name__
    except Exception:  # noqa: BLE001 - only used for a log message
        return type(err).__name__


class QuiltCoordinator(DataUpdateCoordinator[dict]):
    """Polls Quilt's cloud and applies its push stream.

    Data is {"rooms": {id: room}, "dial": {...} | None}. Energy lives beside
    it (energy / energy_last_reset) because it comes from a separate, slower call.
    """

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=scan_interval(entry),
            request_refresh_debouncer=Debouncer(
                hass, _LOGGER, cooldown=REFRESH_COOLDOWN, immediate=True
            ),
        )
        self.entry = entry
        auth = CognitoAuth(entry.data[CONF_REFRESH_TOKEN])
        self.client = QuiltClient(auth, entry.data[CONF_SYSTEM_ID])
        self._stream = NotifierStream(
            auth, self._topics, self._events_from_thread, self._connected_from_thread
        )
        # When each room last had a pushed value applied (time.monotonic()), so
        # a full read that started earlier can't roll it back.
        self._control_pushed_at: dict[str, float] = {}
        self._live_pushed_at: dict[str, float] = {}
        self._dial_pushed_at = 0.0
        self._last_read_done = 0.0
        self._remove_topic_listener = None
        # kWh per room since local midnight, and that midnight (the sensors' last_reset).
        self.energy: dict[str, float] = {}
        self.energy_last_reset: datetime | None = None
        self._energy_attempt = float("-inf")
        self._energy_inflight = False
        self._energy_warned = False

    @property
    def push_healthy(self) -> bool:
        """The push stream is connected and Quilt is still talking on it."""
        return self._stream.healthy

    async def _async_update_data(self) -> dict:
        started = time.monotonic()
        try:
            data = await self.hass.async_add_executor_job(self.client.get_system)
        except QuiltAuthError as err:
            # A revoked/expired refresh token needs the user to re-login.
            if is_revoked(err):
                raise ConfigEntryAuthFailed(str(err)) from err
            raise UpdateFailed(f"Quilt login refresh failed: {err}") from err
        except grpc.RpcError as err:
            raise UpdateFailed(f"Quilt API error: {_rpc_code(err)}") from err
        except (urllib.error.URLError, OSError) as err:
            raise UpdateFailed(f"Can't reach Quilt: {err}") from err
        self._last_read_done = time.monotonic()
        old = self.data or {}
        old_rooms = old.get("rooms", {})
        for room_id, room in data["rooms"].items():
            prev = old_rooms.get(room_id)
            if prev is None:
                continue
            if self._control_pushed_at.get(room_id, 0.0) > started:
                room.update({k: prev[k] for k in CONTROL_KEYS if k in prev})
            if self._live_pushed_at.get(room_id, 0.0) > started:
                room.update({k: prev[k] for k in LIVE_KEYS if prev.get(k) is not None})
        old_dial, dial = old.get("dial"), data.get("dial")
        if old_dial and dial and self._dial_pushed_at > started:
            dial.update({k: old_dial[k] for k in DIAL_KEYS if old_dial.get(k) is not None})
        self._schedule_energy_refresh()
        return data

    # --- energy ----------------------------------------------------------
    @callback
    def _schedule_energy_refresh(self) -> None:
        """Fetch today's energy in the background when due; never delays the poll."""
        midnight = dt_util.start_of_local_day()
        new_day = midnight != self.energy_last_reset
        due = time.monotonic() - self._energy_attempt >= ENERGY_REFRESH_INTERVAL
        if self._energy_inflight or not (new_day or due):
            return
        self._energy_inflight = True
        self._energy_attempt = time.monotonic()
        self.entry.async_create_background_task(
            self.hass, self._async_refresh_energy(midnight), "quilt energy refresh"
        )

    async def _async_refresh_energy(self, midnight: datetime) -> None:
        try:
            totals = await self.hass.async_add_executor_job(
                self.client.get_energy_today, midnight.timestamp(), time.time() + 3600
            )
        except Exception as err:  # noqa: BLE001 - energy is best-effort
            if not self._energy_warned:
                _LOGGER.warning("Couldn't read Quilt energy use; will retry: %s", err)
                self._energy_warned = True
            return
        finally:
            self._energy_inflight = False
        self._energy_warned = False
        self.energy = totals
        self.energy_last_reset = midnight
        self.async_update_listeners()

    # --- push stream -----------------------------------------------------
    def start_push(self) -> None:
        self._remove_topic_listener = self.async_add_listener(self._resubscribe_if_changed)
        self._stream.start()

    async def async_stop_push(self, *_: object) -> None:
        if self._remove_topic_listener is not None:
            self._remove_topic_listener()
            self._remove_topic_listener = None
        await self.hass.async_add_executor_job(self._stream.stop)

    @callback
    def _resubscribe_if_changed(self) -> None:
        self._stream.resubscribe_if_changed(self._topics())

    def _topics(self) -> list[str]:
        data = self.data or {}
        rooms = data.get("rooms", {})
        topics = [f"hds/space/{room_id}" for room_id in rooms]
        topics += [f"hds/indoor_unit/{r['unit_id']}" for r in rooms.values() if r.get("unit_id")]
        dial = data.get("dial")
        if dial and dial.get("id"):
            topics.append(f"hds/controller/{dial['id']}")
        return topics

    def _call_soon(self, func, *args) -> None:
        try:
            self.hass.loop.call_soon_threadsafe(func, *args)
        except RuntimeError:
            pass  # event loop already closed (shutdown)

    def _events_from_thread(self, events: list[dict]) -> None:
        self._call_soon(self._apply_push, events)

    def _connected_from_thread(self) -> None:
        self._call_soon(self._catch_up)

    @callback
    def _catch_up(self) -> None:
        # Pick up anything that changed while the stream was down, unless a
        # full read just finished.
        if time.monotonic() - self._last_read_done > CATCH_UP_GRACE:
            self._request_refresh_soon()

    @callback
    def _request_refresh_soon(self) -> None:
        self.hass.async_create_task(self.async_request_refresh())

    def _room_for(self, ev: dict) -> tuple[str | None, dict | None]:
        rooms = self.data["rooms"]
        room_id = ev.get("space_id")
        if room_id in rooms:
            return room_id, rooms[room_id]
        unit_id = ev.get("unit_id")
        if ev.get("kind") == "unit" and unit_id:
            for rid, room in rooms.items():
                if room.get("unit_id") == unit_id:
                    return rid, room
        return None, None

    @callback
    def _apply_push(self, events: list[dict]) -> None:
        if not self.data:
            return
        now = time.monotonic()
        controls_changed = live_changed = False
        for ev in events:
            if ev.get("kind") == "dial":
                dial = self.data.get("dial")
                if not dial or (ev.get("dial_id") and dial.get("id")
                                and ev["dial_id"] != dial["id"]):
                    continue
                for key in DIAL_KEYS:
                    if ev.get(key) is not None:
                        self._dial_pushed_at = now
                        if ev[key] != dial.get(key):
                            dial[key] = ev[key]
                            live_changed = True
                continue
            room_id, room = self._room_for(ev)
            if room is None:
                continue
            for key in CONTROL_KEYS:
                if key in ev:
                    self._control_pushed_at[room_id] = now
                    if ev[key] != room.get(key):
                        room[key] = ev[key]
                        controls_changed = True
            for key in LIVE_KEYS:
                if ev.get(key) is not None:
                    self._live_pushed_at[room_id] = now
                    if ev[key] != room.get(key):
                        room[key] = ev[key]
                        live_changed = True
        # async_update_listeners (not async_set_updated_data) so a steady push
        # stream doesn't keep postponing the fallback poll.
        if controls_changed or live_changed:
            self.async_update_listeners()
        if controls_changed:
            self._request_refresh_soon()
