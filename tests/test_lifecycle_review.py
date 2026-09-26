"""Lifecycle / concurrency review of v0.5.0: push-health availability, unload order, energy task.

Runs through Home Assistant with the conftest fakes. Where ordering depends on
a real network round trip, the client call is replaced by a plain function so
it runs on a real executor thread (the harness runs Mock targets inline).
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from unittest.mock import patch

import grpc
import pytest

from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from custom_components.quilt import coordinator as coordinator_mod
from custom_components.quilt.const import DOMAIN, ENERGY_REFRESH_INTERVAL

from .conftest import DIAL_ID, DINING

CLIMATE_DINING = ("climate", f"quilt_{DINING}")
ENERGY_DINING = ("sensor", f"quilt_{DINING}_energy_today")
ENERGY_WARNING = "Couldn't read Quilt energy use"


class FakeClock:
    """coordinator.time stand-in: monotonic() only moves when the test says so."""

    def __init__(self) -> None:
        self.now = 50_000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return time.time()  # wall clock (frozen when the freezer fixture is active)

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock():
    fake = FakeClock()
    with patch.object(coordinator_mod, "time", fake):
        yield fake


class CancelledOnClose(grpc.RpcError):
    """What an in-flight unary call raises when its channel is closed."""

    def code(self):
        return grpc.StatusCode.CANCELLED

    def details(self):
        return "Channel closed!"

    def __str__(self):
        return "<_InactiveRpcError status=CANCELLED details='Channel closed!'>"


def _state(hass, ref):
    entity_id = er.async_get(hass).async_get_entity_id(ref[0], DOMAIN, ref[1])
    assert entity_id is not None, ref
    state = hass.states.get(entity_id)
    assert state is not None, ref
    return state


def _energy_warnings(caplog):
    return [
        r for r in caplog.records
        if r.levelno == logging.WARNING and ENERGY_WARNING in r.getMessage()
    ]


# --- push health drives availability -----------------------------------------
# The other direction (stream lost during a poll outage -> unavailable) is also
# covered by test_coordinator.py::test_entities_go_unavailable_when_stream_dies_
# during_a_poll_outage and test_review_owner_setup.py::test_revoked_login_then_
# stream_loss_goes_unavailable.
async def test_entities_come_back_when_the_stream_recovers_during_a_poll_outage(
    hass, setup_integration, mock_client, fake_stream
):
    """QuiltEntity is available while either the poll or the push stream works.

    A repeat poll failure doesn't notify listeners and a push only does when a
    value changes, so the coordinator re-notifies them itself when the stream
    connects or disconnects while polls are failing.
    """
    coordinator = setup_integration.runtime_data
    fake_stream.instance.healthy = False
    mock_client.get_system.side_effect = OSError("Network is unreachable")
    await coordinator.async_refresh()
    await hass.async_block_till_done()
    assert _state(hass, CLIMATE_DINING).state == STATE_UNAVAILABLE  # poll outage

    # The stream reconnects and Quilt sends the Dial's unchanged reading.
    fake_stream.instance.healthy = True
    fake_stream.instance.on_connect()
    fake_stream.instance.on_events([{"kind": "dial", "dial_id": DIAL_ID, "temperature": 25.2}])
    await hass.async_block_till_done()
    assert coordinator.push_healthy is True
    assert coordinator.last_update_success is False  # poll still failing

    assert _state(hass, CLIMATE_DINING).state != STATE_UNAVAILABLE

    # And back: the stream drops again while polls are still failing.
    fake_stream.instance.healthy = False
    fake_stream.instance.on_disconnect()
    await hass.async_block_till_done()
    assert coordinator.last_update_success is False

    assert _state(hass, CLIMATE_DINING).state == STATE_UNAVAILABLE


# --- unload order -------------------------------------------------------------
async def test_unload_does_not_close_the_client_under_an_energy_fetch(
    clock, hass, setup_integration, mock_client, caplog
):
    """async_unload_entry stops the push stream and shuts the coordinator down
    (cancelling any energy read) before it closes the gRPC client, so a fetch in
    flight at unload/reload doesn't fail on the closed channel and log a spurious
    'Couldn't read Quilt energy use' warning.
    """
    entry = setup_integration
    coordinator = entry.runtime_data
    await hass.async_block_till_done(wait_background_tasks=True)
    started, closed, energy_done = threading.Event(), threading.Event(), threading.Event()

    def get_energy(since, until):
        # Runs on a real executor thread; the RPC is in flight until the channel closes.
        started.set()
        try:
            closed.wait(5)
            raise CancelledOnClose()
        finally:
            energy_done.set()

    def close():
        closed.set()
        # grpc's close() cancels in-flight calls; let that call finish failing first.
        energy_done.wait(5)
        time.sleep(0.2)

    mock_client.get_energy = get_energy
    mock_client.close = close
    clock.advance(ENERGY_REFRESH_INTERVAL + 1)
    await coordinator.async_refresh()
    assert await hass.async_add_executor_job(started.wait, 5), "energy fetch in flight"
    caplog.clear()

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)
    assert entry.state is ConfigEntryState.NOT_LOADED
    # Control: the client really was closed under the in-flight call, and that
    # call really did fail, so a missing warning isn't just a missing close.
    assert closed.is_set()
    assert energy_done.is_set()

    assert _energy_warnings(caplog) == []


# --- energy task scheduling ---------------------------------------------------
async def test_cancelled_energy_fetch_does_not_block_the_next_one(
    clock, hass, setup_integration, mock_client
):
    """A cancelled energy task (unload, shutdown) never wedges later fetches."""
    entry = setup_integration
    coordinator = entry.runtime_data
    await hass.async_block_till_done(wait_background_tasks=True)
    entered, release = threading.Semaphore(0), threading.Event()
    calls = []

    def get_energy(since, until):
        calls.append(since)
        entered.release()
        release.wait(5)
        return {DINING: [(int(until) - 3600, 1.0)]}

    mock_client.get_energy = get_energy
    try:
        clock.advance(ENERGY_REFRESH_INTERVAL + 1)
        await coordinator.async_refresh()
        assert await hass.async_add_executor_job(entered.acquire, True, 5)
        tasks = [t for t in entry._background_tasks if t.get_name() == "quilt energy refresh"]
        assert len(tasks) == 1
        tasks[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[0]

        clock.advance(ENERGY_REFRESH_INTERVAL + 1)
        await coordinator.async_refresh()
        assert await hass.async_add_executor_job(entered.acquire, True, 5), (
            "no new energy fetch after the previous one was cancelled"
        )
        assert len(calls) == 2
    finally:
        release.set()
    await hass.async_block_till_done(wait_background_tasks=True)
    assert float(_state(hass, ENERGY_DINING).state) == pytest.approx(1.0)


async def test_failed_energy_fetch_is_retried_on_the_interval_not_every_poll(
    clock, hass, config_entry, mock_client, fake_stream
):
    """While energy reads fail (here from setup, before any last_reset exists),
    polls don't each fire another GetEnergyMetrics call: the next attempt waits
    ENERGY_REFRESH_INTERVAL.
    """
    mock_client.get_energy.side_effect = CancelledOnClose()  # any failure
    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)
    coordinator = config_entry.runtime_data
    assert mock_client.get_energy.call_count == 1  # one attempt at setup
    assert coordinator.energy_last_reset is None

    for _ in range(5):  # five default-interval polls, 5 minutes in all
        clock.advance(60)
        await coordinator.async_refresh()
        await hass.async_block_till_done(wait_background_tasks=True)
    assert coordinator.last_update_success is True  # polls succeed

    assert mock_client.get_energy.call_count == 1

    # Control: once the interval is up it does try again.
    clock.advance(ENERGY_REFRESH_INTERVAL)
    await coordinator.async_refresh()
    await hass.async_block_till_done(wait_background_tasks=True)
    assert mock_client.get_energy.call_count == 2


@pytest.mark.freeze_time("2026-09-25 23:58:00-07:00")
async def test_failed_energy_fetch_after_midnight_is_retried_on_the_interval(
    clock, hass, freezer, setup_integration, mock_client, energy
):
    """A new local day is due at once, but only once: if that read fails, the
    next attempt still waits ENERGY_REFRESH_INTERVAL instead of every poll.
    """
    coordinator = setup_integration.runtime_data
    await hass.async_block_till_done(wait_background_tasks=True)
    yesterday = dt_util.start_of_local_day()
    assert coordinator.energy_last_reset == yesterday
    assert mock_client.get_energy.call_count == 1

    mock_client.get_energy.side_effect = CancelledOnClose()  # any failure
    freezer.move_to("2026-09-26 00:00:30-07:00")
    clock.advance(60)  # well inside ENERGY_REFRESH_INTERVAL
    await coordinator.async_refresh()
    await hass.async_block_till_done(wait_background_tasks=True)
    assert mock_client.get_energy.call_count == 2  # the new day is due right away

    for _ in range(5):
        clock.advance(60)
        await coordinator.async_refresh()
        await hass.async_block_till_done(wait_background_tasks=True)
    assert coordinator.last_update_success is True

    assert mock_client.get_energy.call_count == 2

    # Control: once the interval is up it does try again.
    clock.advance(ENERGY_REFRESH_INTERVAL)
    await coordinator.async_refresh()
    await hass.async_block_till_done(wait_background_tasks=True)
    assert mock_client.get_energy.call_count == 3
