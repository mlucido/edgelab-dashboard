"""
EdgeLab Mode Manager — Single source of truth for PAPER vs LIVE trading mode.

All execution code must call is_live() before placing any real order.
Mode is persisted in Redis so it survives service restarts.

Redis keys:
  edgelab:mode          — current mode string ("PAPER" or "LIVE")
  edgelab:mode_history  — Redis list of last 10 mode change records (JSON)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

_KEY_MODE = "edgelab:mode"
_KEY_MODE_HISTORY = "edgelab:mode_history"
_HISTORY_MAX = 10

VALID_MODES = {"PAPER", "LIVE"}


async def _get_redis() -> aioredis.Redis:
    return await aioredis.from_url(REDIS_URL, decode_responses=True)


async def get_mode(redis: aioredis.Redis | None = None) -> str:
    """
    Return the current trading mode ("PAPER" or "LIVE").

    Reads from Redis key `edgelab:mode`. Defaults to "PAPER" if not set.
    Always returns an uppercase string.
    """
    r = redis or await _get_redis()
    try:
        raw = await r.get(_KEY_MODE)
        if raw is None:
            return "PAPER"
        mode = raw.upper()
        return mode if mode in VALID_MODES else "PAPER"
    finally:
        if redis is None:
            await r.aclose()


async def set_mode(
    mode: str,
    confirmed_by: str,
    redis: aioredis.Redis | None = None,
) -> None:
    """
    Set the trading mode. Logs who changed it and when, publishes alert.

    Args:
        mode:         "PAPER" or "LIVE" (case-insensitive)
        confirmed_by: identifier for who/what triggered the change (user, script, etc.)
        redis:        optional shared Redis connection

    Raises:
        ValueError: if mode is not "PAPER" or "LIVE"
    """
    mode_upper = mode.upper()
    if mode_upper not in VALID_MODES:
        raise ValueError(f"Invalid mode {mode!r}. Must be one of {VALID_MODES}")

    r = redis or await _get_redis()
    try:
        previous_mode = await get_mode(r)
        ts = datetime.now(timezone.utc).isoformat()

        # Write new mode
        await r.set(_KEY_MODE, mode_upper)

        # Append history record
        history_entry: dict[str, Any] = {
            "mode": mode_upper,
            "previous_mode": previous_mode,
            "confirmed_by": confirmed_by,
            "timestamp": ts,
        }
        await r.rpush(_KEY_MODE_HISTORY, json.dumps(history_entry))
        # Trim to last N entries
        await r.ltrim(_KEY_MODE_HISTORY, -_HISTORY_MAX, -1)

        logger.info(
            "Trading mode changed: %s -> %s (by: %s) at %s",
            previous_mode, mode_upper, confirmed_by, ts,
        )

        # Publish mode change alert
        try:
            await r.publish("alerts", json.dumps({
                "type": "TRADING_MODE_CHANGED",
                "details": {
                    "previous_mode": previous_mode,
                    "new_mode": mode_upper,
                    "confirmed_by": confirmed_by,
                    "timestamp": ts,
                },
            }))
        except Exception as exc:
            logger.debug("Failed to publish mode change alert: %s", exc)

    finally:
        if redis is None:
            await r.aclose()


async def is_live(redis: aioredis.Redis | None = None) -> bool:
    """
    Return True if trading mode is currently LIVE, False otherwise.

    This is the primary guard used by execution code.
    """
    mode = await get_mode(redis)
    return mode == "LIVE"


async def get_mode_history(redis: aioredis.Redis | None = None) -> list[dict[str, Any]]:
    """
    Return the last 10 mode change records from Redis.

    Each record contains: mode, previous_mode, confirmed_by, timestamp.
    Returns an empty list if no history exists.
    """
    r = redis or await _get_redis()
    try:
        raw_entries = await r.lrange(_KEY_MODE_HISTORY, 0, -1)
        history = []
        for entry in raw_entries:
            try:
                history.append(json.loads(entry))
            except json.JSONDecodeError:
                pass
        return history
    finally:
        if redis is None:
            await r.aclose()
