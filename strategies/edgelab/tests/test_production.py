"""
Production hardening tests — process monitor and health server.
All Redis calls are mocked.
"""
from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Process monitor tests
# ---------------------------------------------------------------------------

from src.watchdog.process_monitor import RestartTracker, check_process


class TestRestartTracker:
    def test_initial_count_is_zero(self):
        tracker = RestartTracker(window=600, max_restarts=3)
        assert tracker.count() == 0

    def test_record_increments_count(self):
        tracker = RestartTracker(window=600, max_restarts=3)
        tracker.record()
        tracker.record()
        assert tracker.count() == 2

    def test_not_exceeded_below_threshold(self):
        tracker = RestartTracker(window=600, max_restarts=3)
        tracker.record()
        tracker.record()
        assert not tracker.exceeded()

    def test_exceeded_at_threshold(self):
        tracker = RestartTracker(window=600, max_restarts=3)
        tracker.record()
        tracker.record()
        tracker.record()
        assert tracker.exceeded()

    def test_exceeded_above_threshold(self):
        tracker = RestartTracker(window=600, max_restarts=3)
        for _ in range(5):
            tracker.record()
        assert tracker.exceeded()

    def test_old_restarts_outside_window_do_not_count(self):
        tracker = RestartTracker(window=60, max_restarts=3)
        old_time = time.time() - 120  # 2 minutes ago, outside 60s window
        tracker._times = [old_time, old_time, old_time]
        assert tracker.count() == 0
        assert not tracker.exceeded()

    def test_mix_of_old_and_recent_restarts(self):
        tracker = RestartTracker(window=60, max_restarts=3)
        old_time = time.time() - 120
        tracker._times = [old_time, old_time]  # 2 old, outside window
        tracker.record()  # 1 recent
        assert tracker.count() == 1
        assert not tracker.exceeded()

    def test_exactly_at_window_boundary(self):
        tracker = RestartTracker(window=60, max_restarts=3)
        # Slightly outside the window — should not count
        borderline = time.time() - 61
        tracker._times = [borderline, borderline, borderline]
        assert tracker.count() == 0
        assert not tracker.exceeded()


class TestCheckProcess:
    def test_alive_process(self):
        # os.getpid() is always alive
        import os
        assert check_process(os.getpid()) is True

    def test_dead_process_returns_false(self):
        # PID 0 is never a valid user process; kill(0, 0) raises PermissionError
        # on macOS/Linux for unprivileged callers, which check_process maps to False.
        # Use a very large PID that's almost certainly dead.
        assert check_process(999999) is False


# ---------------------------------------------------------------------------
# Health server tests
# ---------------------------------------------------------------------------

from src.health.health_server import _determine_status_code, _gather_health


def _make_mock_redis(
    *,
    mode: str | None = "PAPER",
    trades_today: str | None = "5",
    daily_pnl: str | None = "12.50",
    global_pause: str | None = "0",
    last_trade_at: str | None = None,
    feed_polymarket: str | None = None,
    feed_kalshi: str | None = None,
    feed_events: str | None = None,
    positions_hlen: int = 2,
) -> AsyncMock:
    """Build a mock aioredis.Redis with deterministic return values."""
    r = AsyncMock()

    now = time.time()
    # Default feeds to "recent" heartbeat
    fp = str(now) if feed_polymarket is None else feed_polymarket
    fk = str(now) if feed_kalshi is None else feed_kalshi
    fe = str(now) if feed_events is None else feed_events

    r.gather = None  # not used directly

    # asyncio.gather calls r.get / r.hlen concurrently — wire them up
    async def _get(key):
        mapping = {
            "edgelab:mode": mode,
            "edgelab:cb:trades_today": trades_today,
            "edgelab:cb:daily_pnl": daily_pnl,
            "edgelab:cb:global_pause": global_pause,
            "edgelab:last_trade_at": last_trade_at,
            "edgelab:feed:polymarket:last_update": fp,
            "edgelab:feed:kalshi:last_update": fk,
            "edgelab:feed:events:last_update": fe,
        }
        return mapping.get(key)

    async def _hlen(key):
        if key == "edgelab:positions":
            return positions_hlen
        return 0

    r.get = AsyncMock(side_effect=_get)
    r.hlen = AsyncMock(side_effect=_hlen)
    return r


@pytest.mark.asyncio
async def test_health_returns_valid_structure():
    r = _make_mock_redis()
    data = await _gather_health(r)

    assert "status" in data
    assert "uptime_seconds" in data
    assert "mode" in data
    assert "trades_today" in data
    assert "pnl_today" in data
    assert "open_positions" in data
    assert "circuit_breakers_active" in data
    assert "last_trade_at" in data
    assert "feeds" in data
    assert set(data["feeds"].keys()) == {"polymarket", "kalshi", "events"}


@pytest.mark.asyncio
async def test_health_values_correct():
    r = _make_mock_redis(
        mode="LIVE",
        trades_today="10",
        daily_pnl="25.00",
        global_pause="0",
        positions_hlen=4,
    )
    data = await _gather_health(r)

    assert data["mode"] == "LIVE"
    assert data["trades_today"] == 10
    assert data["pnl_today"] == 25.0
    assert data["open_positions"] == 4
    assert data["circuit_breakers_active"] is False


@pytest.mark.asyncio
async def test_health_circuit_breaker_active():
    r = _make_mock_redis(global_pause="1")
    data = await _gather_health(r)
    assert data["circuit_breakers_active"] is True


@pytest.mark.asyncio
async def test_status_code_200_when_healthy():
    r = _make_mock_redis(global_pause="0")
    data = await _gather_health(r)
    assert _determine_status_code(data) == 200


@pytest.mark.asyncio
async def test_status_code_503_when_circuit_breaker_active():
    r = _make_mock_redis(global_pause="1")
    data = await _gather_health(r)
    assert _determine_status_code(data) == 503


@pytest.mark.asyncio
async def test_status_code_503_when_last_trade_stale():
    stale_epoch = time.time() - 7201  # just over 2 hours ago
    # Store as ISO string
    from datetime import datetime, timezone
    stale_iso = datetime.fromtimestamp(stale_epoch, tz=timezone.utc).isoformat()
    r = _make_mock_redis(last_trade_at=stale_iso, global_pause="0")
    data = await _gather_health(r)
    assert _determine_status_code(data) == 503


@pytest.mark.asyncio
async def test_status_code_200_when_last_trade_recent():
    recent_epoch = time.time() - 60  # 1 minute ago
    from datetime import datetime, timezone
    recent_iso = datetime.fromtimestamp(recent_epoch, tz=timezone.utc).isoformat()
    r = _make_mock_redis(last_trade_at=recent_iso, global_pause="0")
    data = await _gather_health(r)
    assert _determine_status_code(data) == 200


@pytest.mark.asyncio
async def test_feeds_marked_stale_when_no_heartbeat():
    very_old = str(time.time() - 300)  # 5 minutes ago — beyond 2-min window
    r = _make_mock_redis(
        feed_polymarket=very_old,
        feed_kalshi=very_old,
        feed_events=very_old,
    )
    data = await _gather_health(r)
    for feed, status in data["feeds"].items():
        assert status.startswith("stale"), f"{feed} expected stale, got {status!r}"


@pytest.mark.asyncio
async def test_feeds_ok_with_recent_heartbeat():
    recent = str(time.time() - 10)
    r = _make_mock_redis(
        feed_polymarket=recent,
        feed_kalshi=recent,
        feed_events=recent,
    )
    data = await _gather_health(r)
    for feed, status in data["feeds"].items():
        assert status == "ok", f"{feed} expected ok, got {status!r}"


@pytest.mark.asyncio
async def test_feeds_unknown_when_no_key():
    r = _make_mock_redis(
        feed_polymarket=None,
        feed_kalshi=None,
        feed_events=None,
    )

    async def _get_no_feeds(key):
        no_feed_keys = {
            "edgelab:feed:polymarket:last_update",
            "edgelab:feed:kalshi:last_update",
            "edgelab:feed:events:last_update",
        }
        if key in no_feed_keys:
            return None
        mapping = {
            "edgelab:mode": "PAPER",
            "edgelab:cb:trades_today": "0",
            "edgelab:cb:daily_pnl": "0",
            "edgelab:cb:global_pause": "0",
            "edgelab:last_trade_at": None,
        }
        return mapping.get(key)

    r.get = AsyncMock(side_effect=_get_no_feeds)
    data = await _gather_health(r)
    for feed, status in data["feeds"].items():
        assert status == "unknown", f"{feed} expected unknown, got {status!r}"


@pytest.mark.asyncio
async def test_defaults_when_redis_keys_missing():
    r = _make_mock_redis(
        mode=None,
        trades_today=None,
        daily_pnl=None,
        global_pause=None,
        positions_hlen=0,
    )
    data = await _gather_health(r)
    assert data["mode"] == "PAPER"
    assert data["trades_today"] == 0
    assert data["pnl_today"] == 0.0
    assert data["open_positions"] == 0
    assert data["circuit_breakers_active"] is False
