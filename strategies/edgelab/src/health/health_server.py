"""
EdgeLab Health Server — HTTP health endpoint for monitoring.

Serves GET /health on port 8502, aggregating Redis + tracker state.

Run as:
    python -m src.health.health_server
    python src/health/health_server.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import redis.asyncio as aioredis
from aiohttp import web

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("HEALTH_PORT", "8502"))
FEED_STALE_SECONDS = 120       # feed considered down if no heartbeat for 2 min
LAST_TRADE_STALE_SECONDS = 7200  # 503 if no trade in 2 hours

# Redis key names (mirror circuit_breaker.py and feed conventions)
_KEY_GLOBAL_PAUSE = "edgelab:cb:global_pause"
_KEY_DAILY_PNL = "edgelab:cb:daily_pnl"
_KEY_TRADES_TODAY = "edgelab:cb:trades_today"
_KEY_MODE = "edgelab:mode"
_KEY_LAST_TRADE_AT = "edgelab:last_trade_at"
_FEED_KEYS = {
    "polymarket": "edgelab:feed:polymarket:last_update",
    "kalshi": "edgelab:feed:kalshi:last_update",
    "events": "edgelab:feed:events:last_update",
}
_KEY_POSITIONS_HASH = "edgelab:positions"

# Server start time (used for uptime)
_start_time = time.monotonic()
_start_wall = time.time()


# ---------------------------------------------------------------------------
# Data gathering
# ---------------------------------------------------------------------------

async def _gather_health(r: aioredis.Redis) -> dict:
    now_epoch = time.time()

    # Parallel Redis reads
    (
        mode_raw,
        trades_today_raw,
        daily_pnl_raw,
        global_pause_raw,
        last_trade_raw,
        *feed_raws,
        positions_count_raw,
    ) = await asyncio.gather(
        r.get(_KEY_MODE),
        r.get(_KEY_TRADES_TODAY),
        r.get(_KEY_DAILY_PNL),
        r.get(_KEY_GLOBAL_PAUSE),
        r.get(_KEY_LAST_TRADE_AT),
        *[r.get(v) for v in _FEED_KEYS.values()],
        r.hlen(_KEY_POSITIONS_HASH),
    )

    # Mode
    mode = (mode_raw or "PAPER").upper()

    # Trades today
    trades_today = int(trades_today_raw) if trades_today_raw else 0

    # Daily P&L
    pnl_today = round(float(daily_pnl_raw), 2) if daily_pnl_raw else 0.0

    # Open positions
    open_positions = int(positions_count_raw) if positions_count_raw else 0

    # Circuit breakers active
    circuit_breakers_active = global_pause_raw == "1"

    # Last trade timestamp
    last_trade_at: str | None = None
    last_trade_epoch: float | None = None
    if last_trade_raw:
        last_trade_at = last_trade_raw
        try:
            # Try parsing ISO format
            dt = datetime.fromisoformat(last_trade_raw.replace("Z", "+00:00"))
            last_trade_epoch = dt.timestamp()
        except (ValueError, AttributeError):
            try:
                last_trade_epoch = float(last_trade_raw)
                last_trade_at = datetime.fromtimestamp(
                    last_trade_epoch, tz=timezone.utc
                ).isoformat()
            except (ValueError, TypeError):
                pass

    # Feed statuses
    feeds: dict[str, str] = {}
    for feed_name, feed_raw in zip(_FEED_KEYS.keys(), feed_raws):
        if feed_raw is None:
            feeds[feed_name] = "unknown"
        else:
            try:
                last_ts = float(feed_raw)
                age = now_epoch - last_ts
                feeds[feed_name] = "ok" if age <= FEED_STALE_SECONDS else f"stale ({int(age)}s)"
            except (ValueError, TypeError):
                feeds[feed_name] = "unknown"

    # Uptime
    uptime_seconds = int(time.monotonic() - _start_time)

    return {
        "status": "ok",
        "uptime_seconds": uptime_seconds,
        "mode": mode,
        "trades_today": trades_today,
        "pnl_today": pnl_today,
        "open_positions": open_positions,
        "circuit_breakers_active": circuit_breakers_active,
        "last_trade_at": last_trade_at,
        "feeds": feeds,
        "_meta": {
            "circuit_breakers_active": circuit_breakers_active,
            "last_trade_epoch": last_trade_epoch,
            "now_epoch": now_epoch,
        },
    }


def _determine_status_code(data: dict) -> int:
    """Return 200 if healthy, 503 if degraded."""
    meta = data.get("_meta", {})

    if meta.get("circuit_breakers_active"):
        return 503

    last_trade_epoch = meta.get("last_trade_epoch")
    now_epoch = meta.get("now_epoch", time.time())
    if last_trade_epoch is not None:
        if now_epoch - last_trade_epoch > LAST_TRADE_STALE_SECONDS:
            return 503

    return 200


# ---------------------------------------------------------------------------
# Route handler
# ---------------------------------------------------------------------------

async def health_handler(request: web.Request) -> web.Response:
    r: aioredis.Redis = request.app["redis"]

    try:
        data = await _gather_health(r)
    except Exception as exc:
        logger.error("Health gather failed: %s", exc)
        return web.Response(
            status=503,
            content_type="application/json",
            text=json.dumps({"status": "error", "detail": str(exc)}),
        )

    status_code = _determine_status_code(data)
    if status_code != 200:
        data["status"] = "degraded"

    # Strip internal _meta before returning
    payload = {k: v for k, v in data.items() if not k.startswith("_")}

    return web.Response(
        status=status_code,
        content_type="application/json",
        text=json.dumps(payload),
    )


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

async def _on_startup(app: web.Application) -> None:
    app["redis"] = await aioredis.from_url(REDIS_URL, decode_responses=True)
    logger.info("Health server connected to Redis at %s", REDIS_URL)


async def _on_cleanup(app: web.Application) -> None:
    r: aioredis.Redis = app.get("redis")
    if r:
        await r.aclose()
    logger.info("Health server Redis connection closed.")


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    return app


async def run() -> None:
    """Start the health server. Called from main or directly."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health server listening on http://0.0.0.0:%d/health", PORT)
    # Run forever until cancelled
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(run())
