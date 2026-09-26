"""api.NotifierStream threading behaviour, with a fake gRPC channel and no network.

grpc.secure_channel is patched (where api.py looks it up) to hand back a fake
channel whose Subscribe call is an iterator the test feeds frame by frame: it
can deliver frames, end cleanly, raise, or block until cancel() (which then
raises CANCELLED, like a real grpc call). CognitoAuth is replaced by a scripted
fake. Backoff waits are recorded and shortened, so every test runs in well under
a second, and a fixture stops every stream and joins every thread it started.
"""
from __future__ import annotations

import collections
import http.client
import logging
import threading
import time
import types
import urllib.error

import grpc
import pytest

from custom_components.quilt import api

LOGGER = api._LOGGER.name
TOPICS = ["hds/space/space-1", "hds/indoor_unit/unit-1", "hds/controller/dial-1"]
WAIT = 3.0  # upper bound for any cross-thread handoff; normally a few ms


def wait_until(predicate, timeout: float = WAIT, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.002)


# ---------------------------------------------------------------------------
# Frame builders (the SubscribeResponse wire shape parse_notifier_frame reads)
# ---------------------------------------------------------------------------

def _tag(field: int, wire: int) -> bytes:
    return api._varint((field << 3) | wire)


def data_item(unit_id: str = "unit-1", space_id: str = "space-1",
              occupied: bool = True) -> bytes:
    """A data event carrying one indoor-unit diff (collection field 9)."""
    unit = (api._len_delimited(1, api._len_delimited(1, unit_id.encode()))
            + api._len_delimited(2, api._len_delimited(2, space_id.encode()))
            + api._len_delimited(7, _tag(2, 0) + api._varint(2 if occupied else 1)))
    diff = api._len_delimited(9, unit)
    notification = api._len_delimited(2, diff)
    any_msg = (api._len_delimited(1, b"type.googleapis.com/core.protos.notifier.Notification")
               + api._len_delimited(2, notification))
    return api._len_delimited(1, api._len_delimited(2, any_msg))


def control_item(control_type: int, topics=()) -> bytes:
    body = b"".join(api._len_delimited(1, t.encode()) for t in topics)
    body += _tag(2, 0) + api._varint(control_type)
    return api._len_delimited(2, body)


def frame(*items: bytes) -> bytes:
    return b"".join(api._len_delimited(1, item) for item in items)


# A frame with nothing recognisable in it (keep-alive traffic).
HEARTBEAT = _tag(2, 0) + api._varint(1)


def unit_event(occupied: bool = True) -> dict:
    return {"kind": "unit", "space_id": "space-1", "unit_id": "unit-1", "occupied": occupied}


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        super().__init__(code)
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


_END = object()


class FakeCall:
    """The Subscribe call: an iterator of frames the test controls."""

    def __init__(self, index: int, channel, method: str, requests, metadata) -> None:
        self.index = index
        self.channel = channel
        self.method = method
        self.metadata = metadata
        # The stream's request generator yields the subscribe request first.
        self.first_request = next(requests)
        self._items: collections.deque = collections.deque()
        self._cond = threading.Condition()
        self.cancelled = False
        self.cancel_calls = 0
        self.reads = 0  # times the stream thread asked for the next frame

    def __iter__(self):
        return self

    def __next__(self):
        with self._cond:
            self.reads += 1
            self._cond.notify_all()
            while not self._items and not self.cancelled:
                self._cond.wait()
            if self.cancelled:
                raise FakeRpcError(grpc.StatusCode.CANCELLED)
            item = self._items.popleft()
        if item is _END:
            raise StopIteration
        if isinstance(item, BaseException):
            raise item
        return item

    def _put(self, item) -> None:
        with self._cond:
            self._items.append(item)
            self._cond.notify_all()

    def push(self, *frames: bytes) -> None:
        for f in frames:
            self._put(f)

    def end(self) -> None:
        """The server closes the stream cleanly."""
        self._put(_END)

    def fail(self, exc: BaseException) -> None:
        self._put(exc)

    def cancel(self) -> bool:
        with self._cond:
            self.cancel_calls += 1
            self.cancelled = True
            self._cond.notify_all()
        return True

    def wait_processed(self, count: int) -> None:
        """Wait until the thread has handled `count` frames and asked for the next."""
        wait_until(lambda: self.reads > count, what=f"call {self.index} to process {count} frames")


class FakeChannel:
    def __init__(self, hub: FakeGrpc, target: str, options) -> None:
        self.hub = hub
        self.target = target
        self.options = options
        self.closed = False

    def stream_stream(self, method, request_serializer=None, response_deserializer=None):
        def invoke(requests, metadata=None):
            return self.hub._open(self, method, requests, metadata)
        return invoke

    def close(self) -> None:
        self.closed = True


class FakeGrpc:
    """Replaces grpc.secure_channel; records every channel and call opened."""

    def __init__(self) -> None:
        self.channels: list[FakeChannel] = []
        self.calls: list[FakeCall] = []
        self.channel_gate: threading.Event | None = None  # block secure_channel until set
        self.in_secure_channel = threading.Event()
        self.channel_error: BaseException | None = None
        self._lock = threading.Lock()

    def secure_channel(self, target, credentials, options=None):
        self.in_secure_channel.set()
        if self.channel_gate is not None:
            self.channel_gate.wait(WAIT)
        if self.channel_error is not None:
            raise self.channel_error
        channel = FakeChannel(self, target, options)
        with self._lock:
            self.channels.append(channel)
        return channel

    def _open(self, channel, method, requests, metadata) -> FakeCall:
        with self._lock:
            call = FakeCall(len(self.calls) + 1, channel, method, requests, metadata)
            self.calls.append(call)
        return call

    def call(self, n: int) -> FakeCall:
        """The n-th Subscribe call (1-based), waiting for it to open."""
        wait_until(lambda: len(self.calls) >= n, what=f"stream #{n} to open")
        return self.calls[n - 1]


class FakeAuth:
    """Stands in for CognitoAuth: plays a script of results; the last one repeats.

    Each entry is a token string, an exception to raise, or a callable.
    """

    def __init__(self, *script) -> None:
        self.calls = 0
        self.script(*(script or ("id-token",)))

    def script(self, *effects) -> None:
        self._effects = list(effects)
        self._start = self.calls

    def id_token(self) -> str:
        i = self.calls - self._start
        self.calls += 1
        effect = self._effects[min(i, len(self._effects) - 1)]
        if isinstance(effect, BaseException):
            raise effect
        if callable(effect):
            return effect()
        return effect


class Harness:
    """A NotifierStream wired to recording callbacks, with shortened backoff."""

    def __init__(self, auth: FakeAuth, topics) -> None:
        self.auth = auth
        self.topics = list(topics)
        self.topic_calls = 0
        self.topics_error: BaseException | None = None
        self.events: list[list[dict]] = []
        self.on_events_error: BaseException | None = None
        self.connects = 0
        self.backoffs: list[float | None] = []  # what _run asked _stop.wait for
        self.hold_backoff = threading.Event()  # set: park the thread in its backoff
        self.stream = api.NotifierStream(auth, self._topics, self._on_events, self._on_connect)

        stop = self.stream._stop
        real_wait = stop.wait

        def fast_wait(timeout=None):
            self.backoffs.append(timeout)
            if timeout is None:  # the revoked-login park: really wait for stop()
                return real_wait()
            while self.hold_backoff.is_set() and not stop.is_set():
                real_wait(0.002)
            return real_wait(min(timeout, 0.002))

        stop.wait = fast_wait

    def _topics(self) -> list[str]:
        self.topic_calls += 1
        if self.topics_error is not None:
            raise self.topics_error
        return list(self.topics)

    def _on_events(self, events: list[dict]) -> None:
        self.events.append(events)
        if self.on_events_error is not None:
            raise self.on_events_error

    def _on_connect(self) -> None:
        self.connects += 1

    @property
    def alive(self) -> bool:
        return self.stream._thread is not None and self.stream._thread.is_alive()


class Clock:
    """api.time stand-in whose monotonic() can be pushed forward."""

    def __init__(self) -> None:
        self.offset = 0.0

    def monotonic(self) -> float:
        return time.monotonic() + self.offset

    def __getattr__(self, name):
        return getattr(time, name)


class _FastEvent(threading.Event):
    """Event whose timed waits poll every 20 ms (shrinks the watchdog's 15 s tick)."""

    def wait(self, timeout=None):
        return super().wait(None if timeout is None else min(timeout, 0.02))


class _FastThreading:
    Event = _FastEvent

    def __getattr__(self, name):
        return getattr(threading, name)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def debug_logs(caplog):
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    return caplog


@pytest.fixture(autouse=True)
def no_jitter(monkeypatch):
    """Backoff jitter off, so recorded waits are exactly 1, 2, 4, ... seconds."""
    monkeypatch.setattr(api, "random", types.SimpleNamespace(uniform=lambda a, b: 1.0))


@pytest.fixture
def fake_grpc(monkeypatch) -> FakeGrpc:
    hub = FakeGrpc()
    monkeypatch.setattr(api.grpc, "secure_channel", hub.secure_channel)
    monkeypatch.setattr(api.grpc, "ssl_channel_credentials", lambda *a, **k: "fake-credentials")
    return hub


@pytest.fixture
def clock(monkeypatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(api, "time", c)
    return c


def _stream_threads() -> set[threading.Thread]:
    return {t for t in threading.enumerate()
            if t.name == "quilt-notifier" or "(_watch)" in t.name}


@pytest.fixture
def make_stream(fake_grpc):
    made: list[Harness] = []
    before = _stream_threads()

    def factory(auth: FakeAuth | None = None, topics=TOPICS, start: bool = True,
                **constants) -> Harness:
        h = Harness(auth or FakeAuth(), topics)
        for name, value in constants.items():
            setattr(h.stream, name, value)
        made.append(h)
        if start:
            h.stream.start()
        return h

    yield factory

    if fake_grpc.channel_gate is not None:
        fake_grpc.channel_gate.set()
    for h in made:
        h.hold_backoff.clear()
        h.stream.stop()
    for h in made:
        assert not h.alive, "notifier thread still running after stop()"
    for thread in _stream_threads() - before:
        thread.join(WAIT)
        assert not thread.is_alive(), f"{thread.name} left running"


def api_records(caplog, level: int) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == LOGGER and r.levelno == level]


# ---------------------------------------------------------------------------
# Positive control: the frames these tests build parse the way Quilt's do
# ---------------------------------------------------------------------------

def test_frame_builders_parse_as_expected():
    assert api.parse_notifier_frame(frame(data_item())) == [unit_event()]
    assert api.parse_notifier_frame(frame(control_item(api.CONTROL_RECONNECT_REQUEST))) == [
        {"kind": "control", "type": api.CONTROL_RECONNECT_REQUEST, "topics": []}]
    assert api.parse_notifier_frame(
        frame(control_item(api.CONTROL_TOPIC_APPENDED, TOPICS[:1]))) == [
        {"kind": "control", "type": api.CONTROL_TOPIC_APPENDED, "topics": TOPICS[:1]}]
    assert api.parse_notifier_frame(HEARTBEAT) == []


def test_opens_subscribe_call_with_token_and_topics(make_stream, fake_grpc):
    h = make_stream()
    call = fake_grpc.call(1)
    assert call.method == api.NOTIFIER_SUBSCRIBE
    assert call.metadata == (("authorization", "id-token"),)
    assert call.first_request == api.subscribe_request(TOPICS)
    assert call.channel.target == api.GRPC_HOST
    assert h.stream._subscribed == frozenset(TOPICS)


# ---------------------------------------------------------------------------
# (1) Token refresh failures
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("error", "warns"),
    [
        pytest.param(ValueError("Expecting value: line 1 column 1 (char 0)"), 1, id="ValueError"),
        pytest.param(http.client.IncompleteRead(b'{"Authentication'), 1, id="IncompleteRead"),
        pytest.param(KeyError("IdToken"), 1, id="KeyError"),
        pytest.param(api.QuiltAuthError("Cognito InitiateAuth 500: InternalErrorException"), 1,
                     id="QuiltAuthError-not-revoked"),
        pytest.param(urllib.error.URLError("temporary failure in name resolution"), 0,
                     id="URLError"),
        pytest.param(TimeoutError("timed out"), 0, id="TimeoutError"),
    ],
)
def test_token_errors_retry_without_killing_the_thread(make_stream, fake_grpc, caplog,
                                                       error, warns):
    h = make_stream(auth=FakeAuth(error, error, error, "id-token"))

    call = fake_grpc.call(1)  # the fourth attempt got a token and opened the stream
    assert h.auth.calls == 4
    assert h.alive
    assert h.backoffs == [1.0, 2.0, 4.0]
    assert call.metadata == (("authorization", "id-token"),)
    warnings = api_records(caplog, logging.WARNING)
    assert len(warnings) == warns, [r.getMessage() for r in warnings]
    if warns:
        assert "can't refresh its login; retrying" in warnings[0].getMessage()
    assert api_records(caplog, logging.ERROR) == []


def test_token_warning_rearms_after_a_successful_refresh(make_stream, fake_grpc, caplog):
    auth = FakeAuth(ValueError("truncated"), ValueError("truncated"), "id-token")
    h = make_stream(auth=auth)
    call1 = fake_grpc.call(1)
    assert len(api_records(caplog, logging.WARNING)) == 1

    auth.script(ValueError("truncated again"), "id-token")
    call1.fail(FakeRpcError(grpc.StatusCode.UNAVAILABLE))
    fake_grpc.call(2)

    warnings = api_records(caplog, logging.WARNING)
    assert len(warnings) == 2
    assert "truncated again" in warnings[1].getMessage()
    assert h.alive


# ---------------------------------------------------------------------------
# (2) Unexpected exceptions inside _stream_once
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source", ["topics", "secure_channel"])
def test_unexpected_error_in_stream_once_is_logged_once_and_thread_survives(
        make_stream, fake_grpc, caplog, source):
    h = make_stream(start=False)
    boom = RuntimeError("dictionary changed size during iteration")
    if source == "topics":
        h.topics_error = boom
    else:
        fake_grpc.channel_error = boom
    h.stream.start()

    wait_until(lambda: len(h.backoffs) >= 5, what="several retries")
    assert h.alive
    errors = api_records(caplog, logging.ERROR)
    assert len(errors) == 1, [r.getMessage() for r in errors]
    assert errors[0].getMessage() == "Quilt notifier failed; retrying"
    assert errors[0].exc_info is not None and errors[0].exc_info[1] is boom
    assert h.backoffs[:5] == [1.0, 2.0, 4.0, 8.0, 16.0]

    # Once the cause goes away the same thread opens a stream.
    h.topics_error = None
    fake_grpc.channel_error = None
    call = fake_grpc.call(1)
    assert call.first_request == api.subscribe_request(TOPICS)
    assert len(api_records(caplog, logging.ERROR)) == 1


def test_error_logging_rearms_after_a_successful_connect(make_stream, fake_grpc, caplog):
    h = make_stream(start=False)
    h.topics_error = RuntimeError("first outage")
    h.stream.start()
    wait_until(lambda: len(h.backoffs) >= 3, what="retries")

    h.topics_error = None
    call = fake_grpc.call(1)
    call.push(frame(data_item()))
    call.wait_processed(1)
    assert h.connects == 1

    h.topics_error = RuntimeError("second outage")
    call.end()
    wait_until(lambda: len(api_records(caplog, logging.ERROR)) == 2, what="second error log")
    messages = [r.exc_info[1].args[0] for r in api_records(caplog, logging.ERROR)]
    assert messages == ["first outage", "second outage"]
    assert h.alive


def test_unexpected_error_from_the_open_stream_is_logged_once(make_stream, fake_grpc, caplog):
    h = make_stream()
    for n in (1, 2, 3):
        fake_grpc.call(n).fail(ValueError("malformed response"))
    call4 = fake_grpc.call(4)

    errors = api_records(caplog, logging.ERROR)
    assert len(errors) == 1
    assert errors[0].getMessage() == "Quilt notifier stream failed"
    assert h.alive
    for call in fake_grpc.calls[:3]:
        assert call.cancel_calls >= 1
        assert call.channel.closed
    assert not call4.channel.closed


def test_raising_callback_does_not_kill_the_thread(make_stream, fake_grpc, caplog):
    h = make_stream()
    h.on_events_error = RuntimeError("callback blew up")
    call1 = fake_grpc.call(1)
    call1.push(frame(data_item()))

    call2 = fake_grpc.call(2)
    assert h.alive
    assert call1.channel.closed
    errors = api_records(caplog, logging.ERROR)
    assert [r.getMessage() for r in errors] == ["Quilt notifier stream failed"]
    assert errors[0].exc_info[1] is h.on_events_error
    assert call2.first_request == api.subscribe_request(TOPICS)


def test_rpc_errors_reconnect_quietly(make_stream, fake_grpc, caplog):
    h = make_stream()
    fake_grpc.call(1).fail(FakeRpcError(grpc.StatusCode.UNAVAILABLE))
    fake_grpc.call(2).fail(FakeRpcError(grpc.StatusCode.UNAUTHENTICATED))
    fake_grpc.call(3)

    assert h.alive
    assert api_records(caplog, logging.WARNING) == []
    assert api_records(caplog, logging.ERROR) == []
    debug = [r.getMessage() for r in api_records(caplog, logging.DEBUG)]
    assert "Quilt notifier stream ended: StatusCode.UNAVAILABLE" in debug
    assert "Quilt notifier stream ended: StatusCode.UNAUTHENTICATED" in debug
    assert all(c.channel.closed for c in fake_grpc.calls[:2])


# ---------------------------------------------------------------------------
# (3) Revoked login
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "message",
    [
        'Cognito InitiateAuth 400: {"__type":"NotAuthorizedException",'
        '"message":"Refresh Token has been revoked"}',
        "no IdToken in refresh response (token revoked?)",
    ],
    ids=["NotAuthorizedException", "no-IdToken"],
)
def test_revoked_login_parks_the_thread_without_retrying(make_stream, fake_grpc, caplog,
                                                         message):
    h = make_stream(auth=FakeAuth(api.QuiltAuthError(message)))

    wait_until(lambda: h.backoffs == [None], what="the thread to park")
    time.sleep(0.1)  # give a (wrong) retry loop the chance to show itself
    assert h.auth.calls == 1
    assert h.backoffs == [None]
    assert fake_grpc.channels == []
    assert h.alive
    assert not h.stream.healthy
    warnings = api_records(caplog, logging.WARNING)
    assert len(warnings) == 1
    assert "Quilt rejected the login" in warnings[0].getMessage()

    started = time.monotonic()
    h.stream.stop()
    assert time.monotonic() - started < 1.0
    assert not h.alive
    assert h.auth.calls == 1


# ---------------------------------------------------------------------------
# (4) healthy
# ---------------------------------------------------------------------------

def test_healthy_follows_the_first_data_event_and_the_stream_ending(make_stream, fake_grpc):
    h = make_stream(start=False)
    assert not h.stream.healthy
    h.stream.start()

    call1 = fake_grpc.call(1)
    assert not h.stream.healthy  # open, but nothing heard yet

    call1.push(HEARTBEAT)
    call1.wait_processed(1)
    assert not h.stream.healthy  # keep-alives alone don't confirm the subscription
    assert h.connects == 0

    call1.push(frame(data_item()))
    call1.wait_processed(2)
    assert h.stream.healthy
    assert h.connects == 1
    assert h.events == [[unit_event()]]

    call1.push(frame(data_item(occupied=False)))
    call1.wait_processed(3)
    assert h.connects == 1  # on_connect fires once per stream
    assert h.events[-1] == [unit_event(occupied=False)]

    h.hold_backoff.set()
    call1.fail(FakeRpcError(grpc.StatusCode.UNAVAILABLE))
    wait_until(lambda: len(h.backoffs) == 1, what="the backoff after the stream ended")
    assert not h.stream.healthy
    assert h.stream._call is None
    assert call1.channel.closed

    h.hold_backoff.clear()
    call2 = fake_grpc.call(2)
    assert not h.stream.healthy
    call2.push(frame(data_item()))
    call2.wait_processed(1)
    assert h.stream.healthy
    assert h.connects == 2


def test_healthy_after_topic_appended(make_stream, fake_grpc):
    h = make_stream()
    call = fake_grpc.call(1)
    call.push(frame(control_item(api.CONTROL_TOPIC_APPENDED, TOPICS)))
    call.wait_processed(1)
    assert h.stream.healthy
    assert h.connects == 1
    assert h.events == []  # control events aren't forwarded as data

    h.hold_backoff.set()
    call.end()  # server closes cleanly
    wait_until(lambda: len(h.backoffs) == 1, what="the backoff after the stream ended")
    assert not h.stream.healthy


def test_healthy_goes_false_after_silence_and_back_on_the_next_frame(make_stream, fake_grpc,
                                                                     clock):
    h = make_stream()
    call = fake_grpc.call(1)
    call.push(frame(data_item()))
    call.wait_processed(1)
    assert h.stream.healthy

    clock.offset += api.NotifierStream.SILENCE_TIMEOUT - 1
    assert h.stream.healthy
    clock.offset += 2
    assert not h.stream.healthy
    assert call.cancel_calls == 0  # still connected; only the silence makes it unhealthy

    call.push(HEARTBEAT)  # any frame counts as Quilt still talking
    call.wait_processed(2)
    assert h.stream.healthy


def test_silence_watchdog_cancels_and_reconnects(make_stream, fake_grpc, clock, monkeypatch,
                                                 caplog):
    monkeypatch.setattr(api, "threading", _FastThreading())  # watchdog ticks every 20 ms
    h = make_stream()
    call1 = fake_grpc.call(1)
    call1.push(frame(data_item()))
    call1.wait_processed(1)
    time.sleep(0.1)  # several watchdog ticks while Quilt is talking
    assert call1.cancel_calls == 0

    clock.offset += api.NotifierStream.SILENCE_TIMEOUT + 1
    call2 = fake_grpc.call(2)
    assert call1.cancelled
    assert call1.channel.closed
    assert call2.first_request == api.subscribe_request(TOPICS)
    assert any("silent for 90s; reconnecting" in r.getMessage()
               for r in api_records(caplog, logging.DEBUG))
    assert api_records(caplog, logging.WARNING) == []
    assert h.alive


# ---------------------------------------------------------------------------
# (5) RECONNECT_REQUEST
# ---------------------------------------------------------------------------

def test_reconnect_request_ends_the_stream_and_a_new_one_opens(make_stream, fake_grpc, caplog):
    h = make_stream()
    call1 = fake_grpc.call(1)
    call1.push(frame(data_item()))
    call1.wait_processed(1)

    # Data sharing the frame with the request is still delivered.
    call1.push(frame(data_item(occupied=False),
                     control_item(api.CONTROL_RECONNECT_REQUEST)))
    call2 = fake_grpc.call(2)

    assert call1.reads == 2  # it stopped reading call 1 right after that frame
    assert call1.cancel_calls >= 1
    assert call1.channel.closed
    assert call2.channel is not call1.channel
    assert h.events == [[unit_event()], [unit_event(occupied=False)]]
    assert call2.first_request == api.subscribe_request(TOPICS)
    assert h.backoffs == [1.0]
    assert api_records(caplog, logging.WARNING) == []
    assert "Quilt notifier asked us to reconnect" in [
        r.getMessage() for r in api_records(caplog, logging.DEBUG)]

    call2.push(frame(control_item(api.CONTROL_TOPIC_APPENDED, TOPICS)))
    call2.wait_processed(1)
    assert h.stream.healthy
    assert h.connects == 2


def test_unknown_control_event_warns_and_keeps_the_stream(make_stream, fake_grpc, caplog):
    h = make_stream()
    call = fake_grpc.call(1)
    call.push(frame(control_item(3, ["hds/space/space-9"])))
    call.wait_processed(1)

    warnings = api_records(caplog, logging.WARNING)
    assert [r.getMessage() for r in warnings] == [
        "Quilt notifier control event 3 for ['hds/space/space-9']"]
    assert call.cancel_calls == 0
    assert len(fake_grpc.calls) == 1
    assert not h.stream.healthy
    assert h.connects == 0


# ---------------------------------------------------------------------------
# (6) resubscribe_if_changed
# ---------------------------------------------------------------------------

def test_resubscribe_only_cancels_when_the_topic_set_changes(make_stream, fake_grpc, caplog):
    h = make_stream(start=False)
    h.stream.resubscribe_if_changed(["hds/space/other"])  # no stream yet: nothing to do
    h.stream.start()

    call1 = fake_grpc.call(1)
    call1.push(frame(control_item(api.CONTROL_TOPIC_APPENDED, TOPICS)))
    call1.wait_processed(1)

    h.stream.resubscribe_if_changed(list(reversed(TOPICS)))  # same set, other order
    h.stream.resubscribe_if_changed(TOPICS + TOPICS[:1])  # same set with a duplicate
    call1.push(HEARTBEAT)
    call1.wait_processed(2)  # still reading call 1
    assert call1.cancel_calls == 0
    assert len(fake_grpc.calls) == 1

    grown = TOPICS + ["hds/indoor_unit/unit-2"]
    h.topics = grown
    h.stream.resubscribe_if_changed(grown)
    call2 = fake_grpc.call(2)
    assert call1.cancel_calls >= 1
    assert call1.channel.closed
    assert call2.first_request == api.subscribe_request(grown)
    assert h.stream._subscribed == frozenset(grown)

    shrunk = TOPICS[:1]
    h.topics = shrunk
    h.stream.resubscribe_if_changed(shrunk)
    call3 = fake_grpc.call(3)
    assert call3.first_request == api.subscribe_request(shrunk)
    assert api_records(caplog, logging.WARNING) == []
    assert api_records(caplog, logging.ERROR) == []


# ---------------------------------------------------------------------------
# (7) stop()
# ---------------------------------------------------------------------------

def test_stop_while_fetching_the_token_opens_no_stream(make_stream, fake_grpc):
    in_fetch, release = threading.Event(), threading.Event()

    def slow_token() -> str:
        in_fetch.set()
        release.wait(WAIT)
        return "id-token"

    h = make_stream(auth=FakeAuth(slow_token))
    assert in_fetch.wait(WAIT)

    stopper = threading.Thread(target=h.stream.stop, name="test-stopper")
    stopper.start()
    wait_until(h.stream._stop.is_set, what="stop() to signal")
    release.set()  # the token arrives after stop()
    stopper.join(WAIT)
    assert not stopper.is_alive()

    assert not h.alive
    assert fake_grpc.channels == []
    assert fake_grpc.calls == []
    assert h.auth.calls == 1
    assert not h.stream.healthy


def test_stop_while_the_channel_is_opening_cancels_the_new_call(make_stream, fake_grpc):
    fake_grpc.channel_gate = threading.Event()
    h = make_stream()
    assert fake_grpc.in_secure_channel.wait(WAIT)

    stopper = threading.Thread(target=h.stream.stop, name="test-stopper")
    stopper.start()
    wait_until(h.stream._stop.is_set, what="stop() to signal")
    fake_grpc.channel_gate.set()  # the call opens after stop() already looked for it
    stopper.join(WAIT)
    assert not stopper.is_alive()

    assert not h.alive
    call = fake_grpc.call(1)
    assert call.cancelled
    assert call.reads == 0
    assert call.channel.closed
    assert h.stream._call is None


def test_stop_cancels_an_open_stream_promptly_and_quietly(make_stream, fake_grpc, caplog):
    h = make_stream()
    call = fake_grpc.call(1)
    call.push(frame(data_item()))
    call.wait_processed(1)
    assert h.stream.healthy
    caplog.clear()

    started = time.monotonic()
    h.stream.stop()
    assert time.monotonic() - started < 1.0
    assert not h.alive
    assert call.cancelled
    assert call.channel.closed
    assert not h.stream.healthy
    assert len(fake_grpc.calls) == 1
    assert [r for r in caplog.records if r.name == LOGGER] == []


# ---------------------------------------------------------------------------
# Backoff
# ---------------------------------------------------------------------------

def test_backoff_doubles_to_the_cap_and_resets_after_a_long_healthy_stream(
        make_stream, fake_grpc, clock):
    auth = FakeAuth(ValueError("x"), ValueError("x"), ValueError("x"), "id-token")
    h = make_stream(auth=auth, MAX_BACKOFF=4.0)

    call1 = fake_grpc.call(1)
    assert h.backoffs == [1.0, 2.0, 4.0]

    # A short connected stream doesn't reset the backoff (flapping protection).
    call1.push(frame(data_item()))
    call1.wait_processed(1)
    call1.end()
    call2 = fake_grpc.call(2)
    assert h.backoffs == [1.0, 2.0, 4.0, 4.0]

    # One that stayed up for HEALTHY_AFTER does.
    call2.push(frame(data_item()))
    call2.wait_processed(1)
    clock.offset += api.NotifierStream.HEALTHY_AFTER + 1
    call2.end()
    call3 = fake_grpc.call(3)
    assert h.backoffs[-1] == 1.0

    # A long stream that never connected doesn't count as healthy.
    clock.offset += api.NotifierStream.HEALTHY_AFTER + 1
    call3.end()
    fake_grpc.call(4)
    assert h.backoffs[-1] == 2.0
