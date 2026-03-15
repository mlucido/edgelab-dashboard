"""
Kalshi feed: REST poller.
Publishes normalized market data to Redis channel `kalshi:prices`.
Falls back to realistic mock data when KALSHI_API_KEY is not set.

Auth: RSA key-pair signing (same as strategies/bot/).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import aiohttp
import aiosqlite
import redis.asyncio as aioredis
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = "kalshi:prices"
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "")


# ---------------------------------------------------------------------------
# RSA signature auth (mirrors strategies/bot/kalshi_client.py)
# ---------------------------------------------------------------------------

_private_key_cache = None


def _load_private_key():
    global _private_key_cache
    if _private_key_cache is not None:
        return _private_key_cache

    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "kalshi.pem")
    p = Path(key_path)
    if not p.is_absolute():
        p = Path(__file__).parent / p
    with open(p, "rb") as f:
        _private_key_cache = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    return _private_key_cache


def _sign(text: str) -> str:
    pk = _load_private_key()
    sig = pk.sign(
        text.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")


def _auth_headers(method: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    msg = ts + method.upper() + path.split("?")[0]
    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY,
        "KALSHI-ACCESS-SIGNATURE": _sign(msg),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }

REST_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
REST_PARAMS = {"status": "open", "limit": 200}
REST_POLL_INTERVAL = 60  # seconds

DB_PATH = os.getenv(
    "EDGELAB_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "edgelab.db"),
)


# ---------------------------------------------------------------------------
# Mock data (used when KALSHI_API_KEY is not set)
# ---------------------------------------------------------------------------

_MOCK_MARKETS: list[dict] = [
    # Politics
    {
        "ticker": "PRES-2024-DEM", "title": "Will the Democratic candidate win the 2024 presidential election?",
        "yes_bid": 0.48, "yes_ask": 0.50, "no_bid": 0.50, "no_ask": 0.52,
        "volume": 4200000, "liquidity": 320000,
        "close_time": "2024-11-05T23:59:00Z", "tags": ["politics", "election"], "slug": "pres-2024-dem",
    },
    {
        "ticker": "SENATE-2024-DEM", "title": "Will Democrats control the Senate after the 2024 election?",
        "yes_bid": 0.44, "yes_ask": 0.46, "no_bid": 0.54, "no_ask": 0.56,
        "volume": 1800000, "liquidity": 140000,
        "close_time": "2024-11-05T23:59:00Z", "tags": ["politics", "election"], "slug": "senate-2024-dem",
    },
    {
        "ticker": "SCOTUS-VACANCY-2024", "title": "Will there be a SCOTUS vacancy in 2024?",
        "yes_bid": 0.22, "yes_ask": 0.24, "no_bid": 0.76, "no_ask": 0.78,
        "volume": 520000, "liquidity": 45000,
        "close_time": "2024-12-31T23:59:00Z", "tags": ["politics", "scotus"], "slug": "scotus-vacancy-2024",
    },
    # Economics
    {
        "ticker": "FED-RATE-CUT-DEC24", "title": "Will the Fed cut rates in December 2024?",
        "yes_bid": 0.71, "yes_ask": 0.73, "no_bid": 0.27, "no_ask": 0.29,
        "volume": 3100000, "liquidity": 280000,
        "close_time": "2024-12-20T00:00:00Z", "tags": ["economics", "fed"], "slug": "fed-rate-cut-dec24",
    },
    {
        "ticker": "CPI-NOV24-ABOVE3", "title": "Will CPI exceed 3% YoY in November 2024?",
        "yes_bid": 0.31, "yes_ask": 0.33, "no_bid": 0.67, "no_ask": 0.69,
        "volume": 870000, "liquidity": 72000,
        "close_time": "2024-12-11T00:00:00Z", "tags": ["economics", "inflation"], "slug": "cpi-nov24-above3",
    },
    {
        "ticker": "RECESSION-2024", "title": "Will the US enter a recession in 2024?",
        "yes_bid": 0.17, "yes_ask": 0.19, "no_bid": 0.81, "no_ask": 0.83,
        "volume": 1200000, "liquidity": 95000,
        "close_time": "2025-01-15T00:00:00Z", "tags": ["economics", "recession"], "slug": "recession-2024",
    },
    {
        "ticker": "SP500-EOY-5500", "title": "Will the S&P 500 close above 5500 on Dec 31 2024?",
        "yes_bid": 0.55, "yes_ask": 0.57, "no_bid": 0.43, "no_ask": 0.45,
        "volume": 2400000, "liquidity": 200000,
        "close_time": "2024-12-31T23:59:00Z", "tags": ["economics", "markets"], "slug": "sp500-eoy-5500",
    },
    # Sports
    {
        "ticker": "NBA-FINALS-CELTICS-25", "title": "Will the Boston Celtics win the 2025 NBA Championship?",
        "yes_bid": 0.28, "yes_ask": 0.30, "no_bid": 0.70, "no_ask": 0.72,
        "volume": 950000, "liquidity": 78000,
        "close_time": "2025-06-30T00:00:00Z", "tags": ["sports", "nba"], "slug": "nba-finals-celtics-25",
    },
    {
        "ticker": "NFL-SB-59-CHIEFS", "title": "Will the Kansas City Chiefs win Super Bowl LIX?",
        "yes_bid": 0.21, "yes_ask": 0.23, "no_bid": 0.77, "no_ask": 0.79,
        "volume": 1600000, "liquidity": 130000,
        "close_time": "2025-02-09T23:59:00Z", "tags": ["sports", "nfl"], "slug": "nfl-sb-59-chiefs",
    },
    {
        "ticker": "NCAA-BB-25-DUKE", "title": "Will Duke win the 2025 NCAA Basketball Tournament?",
        "yes_bid": 0.09, "yes_ask": 0.11, "no_bid": 0.89, "no_ask": 0.91,
        "volume": 430000, "liquidity": 35000,
        "close_time": "2025-04-07T00:00:00Z", "tags": ["sports", "ncaa"], "slug": "ncaa-bb-25-duke",
    },
    {
        "ticker": "MLB-WS-25-DODGERS", "title": "Will the LA Dodgers win the 2025 World Series?",
        "yes_bid": 0.18, "yes_ask": 0.20, "no_bid": 0.80, "no_ask": 0.82,
        "volume": 780000, "liquidity": 60000,
        "close_time": "2025-10-31T00:00:00Z", "tags": ["sports", "mlb"], "slug": "mlb-ws-25-dodgers",
    },
    # Crypto / Tech
    {
        "ticker": "BTC-100K-2024", "title": "Will Bitcoin reach $100,000 in 2024?",
        "yes_bid": 0.61, "yes_ask": 0.63, "no_bid": 0.37, "no_ask": 0.39,
        "volume": 5600000, "liquidity": 450000,
        "close_time": "2024-12-31T23:59:00Z", "tags": ["crypto", "bitcoin"], "slug": "btc-100k-2024",
    },
    {
        "ticker": "ETH-ETF-2024", "title": "Will a spot Ethereum ETF launch in the US in 2024?",
        "yes_bid": 0.79, "yes_ask": 0.81, "no_bid": 0.19, "no_ask": 0.21,
        "volume": 2300000, "liquidity": 190000,
        "close_time": "2024-12-31T23:59:00Z", "tags": ["crypto", "ethereum"], "slug": "eth-etf-2024",
    },
    # Geopolitics
    {
        "ticker": "UKRAINE-CEASEFIRE-2024", "title": "Will there be a ceasefire in Ukraine by end of 2024?",
        "yes_bid": 0.14, "yes_ask": 0.16, "no_bid": 0.84, "no_ask": 0.86,
        "volume": 1100000, "liquidity": 88000,
        "close_time": "2024-12-31T23:59:00Z", "tags": ["geopolitics", "ukraine"], "slug": "ukraine-ceasefire-2024",
    },
    {
        "ticker": "UN-SEC-COUNCIL-REFORM", "title": "Will the UN Security Council expand permanent members by 2026?",
        "yes_bid": 0.06, "yes_ask": 0.08, "no_bid": 0.92, "no_ask": 0.94,
        "volume": 210000, "liquidity": 18000,
        "close_time": "2026-12-31T00:00:00Z", "tags": ["geopolitics", "un"], "slug": "un-sec-council-reform",
    },
    # Science / Climate
    {
        "ticker": "HOTTEST-YEAR-2024", "title": "Will 2024 be the hottest year on record globally?",
        "yes_bid": 0.87, "yes_ask": 0.89, "no_bid": 0.11, "no_ask": 0.13,
        "volume": 640000, "liquidity": 52000,
        "close_time": "2025-01-31T00:00:00Z", "tags": ["climate", "science"], "slug": "hottest-year-2024",
    },
    {
        "ticker": "MARS-MISSION-2026", "title": "Will SpaceX launch a crewed Mars mission before 2027?",
        "yes_bid": 0.11, "yes_ask": 0.13, "no_bid": 0.87, "no_ask": 0.89,
        "volume": 370000, "liquidity": 30000,
        "close_time": "2026-12-31T00:00:00Z", "tags": ["science", "space"], "slug": "mars-mission-2026",
    },
    # Entertainment
    {
        "ticker": "OSCARS-2025-BEST-PIC", "title": "Will 'Conclave' win Best Picture at the 2025 Oscars?",
        "yes_bid": 0.33, "yes_ask": 0.35, "no_bid": 0.65, "no_ask": 0.67,
        "volume": 290000, "liquidity": 24000,
        "close_time": "2025-03-02T23:59:00Z", "tags": ["entertainment", "oscars"], "slug": "oscars-2025-best-pic",
    },
    {
        "ticker": "TAYLOR-ERAS-GROSS-2B", "title": "Will Taylor Swift's Eras Tour gross over $2 billion total?",
        "yes_bid": 0.92, "yes_ask": 0.94, "no_bid": 0.06, "no_ask": 0.08,
        "volume": 155000, "liquidity": 12000,
        "close_time": "2025-06-30T00:00:00Z", "tags": ["entertainment", "music"], "slug": "taylor-eras-gross-2b",
    },
    {
        "ticker": "APPLE-AI-IPHONE-25", "title": "Will Apple Intelligence ship on all iPhone 16 models by mid-2025?",
        "yes_bid": 0.72, "yes_ask": 0.74, "no_bid": 0.26, "no_ask": 0.28,
        "volume": 520000, "liquidity": 43000,
        "close_time": "2025-07-01T00:00:00Z", "tags": ["tech", "apple"], "slug": "apple-ai-iphone-25",
    },
]


def _mock_normalize(raw: dict) -> dict:
    yes_bid = raw.get("yes_bid", 0.5)
    yes_ask = raw.get("yes_ask", 0.5)
    no_bid = raw.get("no_bid", 0.5)
    no_ask = raw.get("no_ask", 0.5)
    prob_yes = round((yes_bid + yes_ask) / 2, 4)
    prob_no = round((no_bid + no_ask) / 2, 4)

    end_date = raw.get("close_time")
    days_to_resolution: Optional[float] = None
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            delta = end_dt - datetime.now(timezone.utc)
            days_to_resolution = max(delta.total_seconds() / 86400, 0.0)
        except (ValueError, AttributeError):
            pass

    return {
        "market_id": raw["ticker"],
        "question": raw["title"],
        "prob_yes": prob_yes,
        "prob_no": prob_no,
        # price is an alias for prob_yes for backward compat with strategies
        "price": prob_yes,
        "volume": raw.get("volume"),
        "liquidity": raw.get("liquidity"),
        "end_date": end_date,
        "days_to_resolution": days_to_resolution,
        "tags": raw.get("tags", []),
        "slug": raw.get("slug", ""),
        "source": "kalshi_mock",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Normalization (live API)
# ---------------------------------------------------------------------------

def normalize_market(raw: dict) -> Optional[dict]:
    """
    Normalize a raw Kalshi v2 market dict.
    Returns None if market is not binary.
    """
    yes_price = raw.get("yes_bid") or raw.get("last_price") or raw.get("yes_ask")
    no_price = raw.get("no_bid") or raw.get("no_ask")

    # Kalshi uses cent prices (0-100), convert to probability
    try:
        prob_yes = float(yes_price) / 100 if yes_price is not None else None
    except (TypeError, ValueError):
        prob_yes = None

    try:
        prob_no = float(no_price) / 100 if no_price is not None else (1 - prob_yes if prob_yes is not None else None)
    except (TypeError, ValueError):
        prob_no = None

    tags = raw.get("tags") or []

    end_date = raw.get("close_time") or raw.get("expiration_time")
    days_to_resolution: Optional[float] = None
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            delta = end_dt - datetime.now(timezone.utc)
            days_to_resolution = max(delta.total_seconds() / 86400, 0.0)
        except (ValueError, AttributeError):
            pass

    prob_yes_rounded = round(prob_yes, 4) if prob_yes is not None else None

    return {
        "market_id": str(raw.get("ticker") or raw.get("id") or ""),
        "question": raw.get("title") or raw.get("subtitle"),
        "prob_yes": prob_yes_rounded,
        "prob_no": round(prob_no, 4) if prob_no is not None else None,
        # price is an alias for prob_yes for backward compat with strategies
        "price": prob_yes_rounded,
        "volume": _safe_float(raw.get("volume") or raw.get("volume_24h")),
        "liquidity": _safe_float(raw.get("liquidity")),
        "end_date": end_date,
        "days_to_resolution": days_to_resolution,
        "tags": tags if isinstance(tags, list) else [],
        "slug": raw.get("ticker_name") or raw.get("slug") or str(raw.get("ticker", "")),
        "source": "kalshi",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


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
    await redis.publish(REDIS_CHANNEL, json.dumps(market))


# ---------------------------------------------------------------------------
# REST poller — live
# ---------------------------------------------------------------------------

MAX_POLL_PAGES = 5  # Cap pagination — Kalshi has 150k+ markets, only need top results


async def _poll_live(session: aiohttp.ClientSession, redis: aioredis.Redis) -> None:
    cursor = None
    count = 0
    pages = 0

    while pages < MAX_POLL_PAGES:
        params = dict(REST_PARAMS)
        if cursor:
            params["cursor"] = cursor

        headers = _auth_headers("GET", "/trade-api/v2/markets")
        async with session.get(
            REST_URL, params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        markets_raw = data.get("markets", [])
        for raw in markets_raw:
            market = normalize_market(raw)
            if market:
                await publish(redis, market)
                count += 1

        pages += 1
        cursor = data.get("cursor")
        if not cursor or not markets_raw:
            break

    logger.info("Kalshi live poll: %d markets published (%d pages)", count, pages)


# ---------------------------------------------------------------------------
# REST poller — mock
# ---------------------------------------------------------------------------

async def _poll_mock(redis: aioredis.Redis) -> None:
    count = 0
    for raw in _MOCK_MARKETS:
        market = _mock_normalize(raw)
        await publish(redis, market)
        count += 1
    logger.info("Kalshi mock poll: %d markets published", count)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run() -> None:
    """Start Kalshi REST poller. Uses mock data if KALSHI_API_KEY is not set."""
    redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

    use_mock = not KALSHI_API_KEY
    if use_mock:
        logger.warning("KALSHI_API_KEY not set — using mock data for %d markets", len(_MOCK_MARKETS))
    else:
        logger.info("Kalshi live feed starting (API key present)")

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    if use_mock:
                        await _poll_mock(redis)
                    else:
                        await _poll_live(session, redis)
                except aiohttp.ClientResponseError as exc:
                    if exc.status in (401, 403):
                        logger.error(
                            "Kalshi auth error (%d) — falling back to mock data. "
                            "Check KALSHI_API_KEY.",
                            exc.status,
                        )
                        use_mock = True
                    else:
                        logger.warning("Kalshi HTTP error %d: %s", exc.status, exc.message)
                except aiohttp.ClientError as exc:
                    logger.warning("Kalshi request error: %s", exc)
                except Exception as exc:
                    logger.error("Unexpected Kalshi error: %s", exc, exc_info=True)

                await asyncio.sleep(REST_POLL_INTERVAL)
    finally:
        await redis.aclose()
        logger.info("Kalshi feed stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(run())
