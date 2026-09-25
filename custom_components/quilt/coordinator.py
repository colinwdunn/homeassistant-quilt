"""Data update coordinator for Quilt."""
from __future__ import annotations

from datetime import timedelta
import logging
import time

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.debounce import Debouncer
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import CognitoAuth, NotifierStream, QuiltAuthError, QuiltClient, is_revoked
from .const import (
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_SYSTEM_ID,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    REFRESH_COOLDOWN,
)

_LOGGER = logging.getLogger(__name__)

# Pushed fields that change what a room is doing (mode, targets, preset); a
# change here also triggers a full read to pick up the preset details the
# stream doesn't carry.
CONTROL_KEYS = ("mode", "on", "heat_setpoint", "cool_setpoint", "active_comfort_id")
LIVE_KEYS = ("current_temp", "occupied", "humidity")
# A (re)connect skips its catch-up read when a full read finished this recently.
CATCH_UP_GRACE = 10.0


def scan_interval(entry: ConfigEntry) -> timedelta:
    return timedelta(seconds=entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL))


class QuiltCoordinator(DataUpdateCoordinator[dict]):
    """Polls Quilt's cloud and applies its push stream.

    Data is {"rooms": {id: room}, "dial": {...}}.
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
        self._last_read_done = 0.0
        self._remove_topic_listener = None

    async def _async_update_data(self) -> dict:
        started = time.monotonic()
        try:
            data = await self.hass.async_add_executor_job(self.client.get_system)
        except QuiltAuthError as err:
            # A revoked/expired refresh token needs the user to re-login.
            if is_revoked(err):
                raise ConfigEntryAuthFailed(str(err)) from err
            raise UpdateFailed(str(err)) from err
        self._last_read_done = time.monotonic()
        old_rooms = (self.data or {}).get("rooms", {})
        for room_id, room in data["rooms"].items():
            old = old_rooms.get(room_id)
            if old is None:
                continue
            if self._control_pushed_at.get(room_id, 0.0) > started:
                room.update({k: old[k] for k in CONTROL_KEYS if k in old})
            if self._live_pushed_at.get(room_id, 0.0) > started:
                room.update({k: old[k] for k in LIVE_KEYS if old.get(k) is not None})
        return data

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
        rooms = (self.data or {}).get("rooms", {})
        topics = [f"hds/space/{room_id}" for room_id in rooms]
        topics += [f"hds/indoor_unit/{r['unit_id']}" for r in rooms.values() if r.get("unit_id")]
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

    @callback
    def _apply_push(self, events: list[dict]) -> None:
        if not self.data:
            return
        rooms = self.data["rooms"]
        now = time.monotonic()
        controls_changed = live_changed = False
        for ev in events:
            room_id = ev.get("space_id")
            room = rooms.get(room_id)
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
