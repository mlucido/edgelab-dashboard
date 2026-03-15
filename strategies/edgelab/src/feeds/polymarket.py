"""
Polymarket feed: WebSocket + REST poller.
Publishes normalized market data to Redis channel `polymarket:prices`.
Stores snapshots in SQLite table `market_snapshots`.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Optional, Union

import aiohttp
import aiosqlite
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = "polymarket:prices"

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
REST_URL = "https://gamma-api.polymarket.com/markets"
REST_PARAMS = {"active": "true", "closed": "false", "limit": 500}
REST_POLL_INTERVAL = 60  # seconds

DB_PATH = os.getenv(
    "EDGELAB_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "edgelab.db"),
)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def init_db(db: aiosqlite.Connection) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT NOT NULL DEFAULT 'polymarket',
            market_id   TEXT NOT NULL,
            question    TEXT,
            prob_yes    REAL,
            prob_no     REAL,
            volume      REAL,
            liquidity   REAL,
            end_date    TEXT,
            tags        TEXT,
            slug        TEXT,
            raw         TEXT,
            captured_at TEXT NOT NULL
        )
        """
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshots_market_id ON market_snapshots(market_id)"
    )
    await db.commit()
    logger.debug("SQLite market_snapshots table ready")


async def save_snapshot(db: aiosqlite.Connection, market: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """
        INSERT INTO market_snapshots
            (source, market_id, question, prob_yes, prob_no, volume, liquidity,
             end_date, tags, slug, raw, captured_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "polymarket",
            market.get("market_id", ""),
            market.get("question"),
            market.get("prob_yes"),
            market.get("prob_no"),
            market.get("volume"),
            market.get("liquidity"),
            market.get("end_date"),
            json.dumps(market.get("tags", [])),
            market.get("slug"),
            json.dumps(market.get("_raw", {})),
            now,
        ),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _parse_outcome_prices(market: dict) -> tuple[Optional[float], Optional[float]]:
    """Extract YES/NO probabilities from Polymarket outcomes list."""
    outcomes = market.get("outcomes")
    outcome_prices = market.get("outcomePrices")

    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except (json.JSONDecodeError, TypeError):
            outcomes = []

    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except (json.JSONDecodeError, TypeError):
            outcome_prices = []

    if not outcomes or not outcome_prices:
        return None, None

    prob_yes, prob_no = None, None
    for outcome, price in zip(outcomes, outcome_prices):
        label = str(outcome).lower()
        try:
            p = float(price)
        except (TypeError, ValueError):
            continue
        if label == "yes":
            prob_yes = p
        elif label == "no":
            prob_no = p

    return prob_yes, prob_no


def normalize_market(raw: dict) -> Optional[dict]:
    """
    Normalize a raw Polymarket market dict.
    Returns None if the market is not a binary (2-outcome) market.
    """
    outcomes = raw.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except (json.JSONDecodeError, TypeError):
            outcomes = []

    if not isinstance(outcomes, list) or len(outcomes) != 2:
        return None

    prob_yes, prob_no = _parse_outcome_prices(raw)

    tags = raw.get("tags") or []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except (json.JSONDecodeError, TypeError):
            tags = []

    end_date = raw.get("endDate") or raw.get("end_date_iso")

    # Compute days_to_resolution from end_date
    days_to_resolution: Optional[float] = None
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            delta = end_dt - datetime.now(timezone.utc)
            days_to_resolution = max(delta.total_seconds() / 86400, 0.0)
        except (ValueError, AttributeError):
            pass

    market = {
        "market_id": str(raw.get("id") or raw.get("condition_id") or ""),
        "question": raw.get("question"),
        "prob_yes": prob_yes,
        "prob_no": prob_no,
        # price is an alias for prob_yes for backward compat with strategies
        "price": prob_yes,
        "volume": _safe_float(raw.get("volume") or raw.get("volume24hr")),
        "liquidity": _safe_float(raw.get("liquidity")),
        "end_date": end_date,
        "days_to_resolution": days_to_resolution,
        "tags": tags if isinstance(tags, list) else [],
        "slug": raw.get("slug") or raw.get("market_slug"),
        "source": "polymarket",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "_raw": raw,
    }
    return market


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Redis publisher
# ---------------------------------------------------------------------------

async def publish(redis: aioredis.Redis, market: dict) -> None:
    payload = {k: v for k, v in market.items() if k != "_raw"}
    await redis.publish(REDIS_CHANNEL, json.dumps(payload))


# ---------------------------------------------------------------------------
# REST poller
# ---------------------------------------------------------------------------

async def rest_poller(redis: aioredis.Redis, db: aiosqlite.Connection) -> None:
    logger.info("Starting Polymarket REST poller (interval=%ds)", REST_POLL_INTERVAL)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(REST_URL, params=REST_PARAMS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

                markets_raw = data if isinstance(data, list) else data.get("markets", [])
                count = 0
                for raw in markets_raw:
                    market = normalize_market(raw)
                    if market is None:
                        continue
                    await publish(redis, market)
                    await save_snapshot(db, market)
                    count += 1

                logger.info("REST poll complete: %d binary markets published", count)

            except aiohttp.ClientError as exc:
                logger.warning("Polymarket REST error: %s", exc)
            except Exception as exc:
                logger.error("Unexpected REST error: %s", exc, exc_info=True)

            await asyncio.sleep(REST_POLL_INTERVAL)


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------

async def _handle_ws_message(message: Union[str, bytes], redis: aioredis.Redis, db: aiosqlite.Connection) -> None:
    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError:
            return

    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return

    # WS messages can be a list or a single object
    events = data if isinstance(data, list) else [data]
    for event in events:
        event_type = event.get("event_type") or event.get("type") or ""
        if event_type in ("price_change", "book", "trade"):
            # Try to extract market data; WS events often have partial fields
            market = normalize_market(event)
            if market:
                await publish(redis, market)
                await save_snapshot(db, market)


async def ws_client(redis: aioredis.Redis, db: aiosqlite.Connection) -> None:
    logger.info("Connecting to Polymarket WebSocket: %s", WS_URL)
    backoff = 1
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as ws:
                logger.info("Polymarket WebSocket connected")
                backoff = 1

                # Subscribe to all market events
                subscribe_msg = json.dumps({"type": "subscribe", "channel": "market"})
                await ws.send(subscribe_msg)

                async for message in ws:
                    try:
                        await _handle_ws_message(message, redis, db)
                    except Exception as exc:
                        logger.warning("Error processing WS message: %s", exc)

        except ConnectionClosed as exc:
            logger.warning("Polymarket WS connection closed: %s — reconnecting in %ds", exc, backoff)
        except WebSocketException as exc:
            logger.warning("Polymarket WS error: %s — reconnecting in %ds", exc, backoff)
        except Exception as exc:
            logger.error("Unexpected WS error: %s — reconnecting in %ds", exc, backoff, exc_info=True)

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run() -> None:
    """Start Polymarket feed: WebSocket + REST poller concurrently."""
    redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await init_db(db)
        logger.info("Polymarket feed starting")

        try:
            await asyncio.gather(
                ws_client(redis, db),
                rest_poller(redis, db),
            )
        finally:
            await redis.aclose()
            logger.info("Polymarket feed stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(run())
