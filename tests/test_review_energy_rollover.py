"""Energy 'today' sensors across local midnight (TOTAL + last_reset semantics).

With state_class TOTAL, the recorder treats a changed last_reset as a new
cycle that starts from 0: the previous cycle contributes only up to the LAST
state it was reported with (homeassistant/components/sensor/recorder.py,
`_sum += new_state - old_state` on reset). So the day's final total must be
reported under the old last_reset before the sensor switches to the new day,
or whatever Quilt metered after the last pre-midnight read is never counted.
These were strict xfails pinning that gap; v0.5.0 reports the finished day's
final total under the old last_reset first, so they now guard the fix.
"""
from __future__ import annotations

from datetime import timedelta
import time
from unittest.mock import patch

import pytest

from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.core import callback
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from custom_components.quilt import coordinator as coordinator_mod
from custom_components.quilt.const import DOMAIN

from .conftest import BEDROOM, DINING, LIVING


class _Clock:
    """coordinator.time stand-in: monotonic only moves when the test says so."""

    def __init__(self) -> None:
        self.now = 50_000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return time.time()  # frozen by the freezer fixture


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(recorder_db_url, enable_custom_integrations):
    """Same as conftest's, but lets the recorder test get its database before hass."""
    yield


@pytest.fixture
def clock():
    fake = _Clock()
    with patch.object(coordinator_mod, "time", fake):
        yield fake


@pytest.mark.freeze_time("2026-09-25 23:50:00-07:00")
async def test_previous_days_final_total_is_reported_before_the_reset(
    hass, freezer, config_entry, mock_client, fake_stream, clock
):
    yesterday = dt_util.start_of_local_day()
    today = yesterday + timedelta(days=1)
    as_of_2350 = {DINING: 0.863, LIVING: 1.266, BEDROOM: 3.31}
    final_yesterday = {DINING: 0.95, LIVING: 1.4, BEDROOM: 3.6}
    early_today = {DINING: 0.004, LIVING: 0.0, BEDROOM: 0.021}

    def quilt_energy(since: float, until: float) -> dict[str, list[tuple[int, float]]]:
        # Quilt's hourly buckets as seen at the moment of the call: yesterday's
        # growth sits in its 23:00 bucket, today's in the 00:00 bucket.
        last_hour = int(today.timestamp()) - 3600
        day = as_of_2350 if time.time() < today.timestamp() else final_yesterday
        out = {}
        for sid in (DINING, LIVING, BEDROOM):
            buckets = [(last_hour, day[sid])] if since <= last_hour else []
            if time.time() >= today.timestamp():
                buckets.append((int(today.timestamp()), early_today[sid]))
            out[sid] = buckets
        return out

    mock_client.get_energy.side_effect = quilt_energy

    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)
    coordinator = config_entry.runtime_data
    entity_id = er.async_get(hass).async_get_entity_id(
        "sensor", DOMAIN, f"quilt_{DINING}_energy_today"
    )
    reported: list[tuple[str, str | None]] = []

    @callback
    def _record(event) -> None:
        if event.data["entity_id"] == entity_id and event.data["new_state"]:
            new = event.data["new_state"]
            reported.append((new.state, new.attributes.get("last_reset")))

    hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    first = hass.states.get(entity_id)
    reported.append((first.state, first.attributes.get("last_reset")))

    # Polls keep running every minute up to midnight; energy isn't due again yet.
    for minute in ("23:55:00", "23:59:30"):
        freezer.move_to(f"2026-09-25 {minute}-07:00")
        clock.now += 300
        await coordinator.async_refresh()
        await hass.async_block_till_done(wait_background_tasks=True)

    # First poll after midnight.
    freezer.move_to("2026-09-26 00:00:30-07:00")
    clock.now += 60
    await coordinator.async_refresh()
    await hass.async_block_till_done(wait_background_tasks=True)

    # Preconditions: the rollover itself happened.
    assert coordinator.energy_last_reset == today
    assert reported[-1] == ("0.004", today.isoformat()), reported

    # Yesterday's cycle must end on yesterday's final total.
    yesterday_values = [float(s) for s, lr in reported if lr == yesterday.isoformat()]
    assert max(yesterday_values) == pytest.approx(final_yesterday[DINING]), (
        f"yesterday's cycle ended at {max(yesterday_values)} kWh; "
        f"{final_yesterday[DINING] - max(yesterday_values):.3f} kWh never recorded; "
        f"reported={reported}"
    )


@pytest.mark.freeze_time("2026-09-25 23:40:00-07:00")
async def test_statistics_sum_over_midnight_matches_quilts_metering(
    recorder_mock, hass, freezer, config_entry, mock_client, fake_stream, clock
):
    from pytest_homeassistant_custom_component.components.recorder.common import (
        async_wait_recording_done,
        do_adhoc_statistics,
    )
    from homeassistant.components.recorder.statistics import statistics_during_period

    yesterday = dt_util.start_of_local_day()
    today = yesterday + timedelta(days=1)
    # Quilt's day total for Dining as it grows (what an up-to-date read returns).
    metered = {"2026-09-25 23:40": 0.80, "2026-09-25 23:52": 0.863, "final": 0.95}
    early_today = 0.004
    now_key = {"value": metered["2026-09-25 23:40"]}

    def quilt_energy(since: float, until: float) -> dict[str, list[tuple[int, float]]]:
        last_hour = int(today.timestamp()) - 3600
        value = now_key["value"] if time.time() < today.timestamp() else metered["final"]
        day = {DINING: value, LIVING: 1.0, BEDROOM: 1.0}
        new_day = {DINING: early_today, LIVING: 0.0, BEDROOM: 0.0}
        out = {}
        for sid in (DINING, LIVING, BEDROOM):
            buckets = [(last_hour, day[sid])] if since <= last_hour else []
            if time.time() >= today.timestamp():
                buckets.append((int(today.timestamp()), new_day[sid]))
            out[sid] = buckets
        return out

    mock_client.get_energy.side_effect = quilt_energy

    config_entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)
    coordinator = config_entry.runtime_data
    entity_id = er.async_get(hass).async_get_entity_id(
        "sensor", DOMAIN, f"quilt_{DINING}_energy_today"
    )
    await async_wait_recording_done(hass)

    # 23:52: a regular 15-minute energy read.
    freezer.move_to("2026-09-25 23:52:00-07:00")
    now_key["value"] = metered["2026-09-25 23:52"]
    clock.now += 16 * 60
    await coordinator.async_refresh()
    await hass.async_block_till_done(wait_background_tasks=True)
    await async_wait_recording_done(hass)

    # 00:00:30: first poll of the new day.
    freezer.move_to("2026-09-26 00:00:30-07:00")
    clock.now += 8 * 60
    await coordinator.async_refresh()
    await hass.async_block_till_done(wait_background_tasks=True)
    await async_wait_recording_done(hass)
    assert hass.states.get(entity_id).attributes["last_reset"] == today.isoformat()

    freezer.move_to("2026-09-26 00:06:00-07:00")
    start = dt_util.as_utc(dt_util.parse_datetime("2026-09-25 23:40:00-07:00"))
    while start < dt_util.as_utc(dt_util.parse_datetime("2026-09-26 00:05:00-07:00")):
        do_adhoc_statistics(hass, start=start)
        await async_wait_recording_done(hass)
        start += timedelta(minutes=5)

    from homeassistant.components.recorder import get_instance

    stats = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        dt_util.as_utc(dt_util.parse_datetime("2026-09-25 23:40:00-07:00")),
        None,
        {entity_id},
        "5minute",
        None,
        {"sum"},
    )
    rows = stats[entity_id]
    counted = rows[-1]["sum"] - 0.0  # sum starts at 0 at the first recorded state (0.80)
    actually_used = (metered["final"] - metered["2026-09-25 23:40"]) + early_today
    assert counted == pytest.approx(actually_used, abs=1e-6), (
        f"statistics counted {counted:.3f} kWh, Quilt metered {actually_used:.3f} kWh; rows={rows}"
    )
