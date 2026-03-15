"""
EdgeLab Kalshi Live Feed — Full REST API integration with pagination and normalization.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = "kalshi:prices"
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "")
KALSHI_BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"
REST_POLL_INTERVAL = 60  # seconds


# ---------------------------------------------------------------------------
# Mock data (20 realistic markets across categories)
# ---------------------------------------------------------------------------

_MOCK_MARKETS: list[dict] = [
    # Politics
    {
        "ticker": "PRES-2028-DEM", "title": "Will the Democratic candidate win the 2028 presidential election?",
        "yes_bid": 48, "yes_ask": 50, "volume": 4200000, "open_interest": 320000,
        "close_time": "2028-11-05T23:59:00Z", "tags": ["politics", "election"],
    },
    {
        "ticker": "SENATE-2026-DEM", "title": "Will Democrats control the Senate after the 2026 midterms?",
        "yes_bid": 42, "yes_ask": 46, "volume": 1800000, "open_interest": 140000,
        "close_time": "2026-11-03T23:59:00Z", "tags": ["politics", "election"],
    },
    {
        "ticker": "SCOTUS-VACANCY-2026", "title": "Will there be a SCOTUS vacancy in 2026?",
        "yes_bid": 28, "yes_ask": 32, "volume": 520000, "open_interest": 45000,
        "close_time": "2026-12-31T23:59:00Z", "tags": ["politics", "scotus"],
    },
    {
        "ticker": "TRUMP-APPROVAL-50-2025", "title": "Will Trump's approval rating exceed 50% in 2025?",
        "yes_bid": 22, "yes_ask": 26, "volume": 980000, "open_interest": 78000,
        "close_time": "2025-12-31T23:59:00Z", "tags": ["politics", "approval"],
    },
    # Economics
    {
        "ticker": "FED-RATE-CUT-JUN25", "title": "Will the Fed cut rates in June 2025?",
        "yes_bid": 55, "yes_ask": 59, "volume": 3100000, "open_interest": 280000,
        "close_time": "2025-06-20T00:00:00Z", "tags": ["economics", "fed"],
    },
    {
        "ticker": "CPI-2025-ABOVE3", "title": "Will CPI exceed 3% YoY by end of 2025?",
        "yes_bid": 38, "yes_ask": 42, "volume": 870000, "open_interest": 72000,
        "close_time": "2025-12-31T00:00:00Z", "tags": ["economics", "inflation"],
    },
    {
        "ticker": "RECESSION-2025", "title": "Will the US enter a recession in 2025?",
        "yes_bid": 31, "yes_ask": 35, "volume": 1200000, "open_interest": 95000,
        "close_time": "2025-12-31T00:00:00Z", "tags": ["economics", "recession"],
    },
    {
        "ticker": "SP500-EOY-6000-2025", "title": "Will the S&P 500 close above 6000 on Dec 31 2025?",
        "yes_bid": 51, "yes_ask": 55, "volume": 2400000, "open_interest": 200000,
        "close_time": "2025-12-31T23:59:00Z", "tags": ["economics", "markets"],
    },
    # Sports
    {
        "ticker": "NBA-FINALS-2025-CELTICS", "title": "Will the Boston Celtics win the 2025 NBA Championship?",
        "yes_bid": 26, "yes_ask": 30, "volume": 950000, "open_interest": 78000,
        "close_time": "2025-06-30T00:00:00Z", "tags": ["sports", "nba"],
    },
    {
        "ticker": "NFL-SB-60-LIONS", "title": "Will the Detroit Lions win Super Bowl LX?",
        "yes_bid": 14, "yes_ask": 18, "volume": 1600000, "open_interest": 130000,
        "close_time": "2026-02-08T23:59:00Z", "tags": ["sports", "nfl"],
    },
    {
        "ticker": "MLB-WS-2025-DODGERS", "title": "Will the LA Dodgers win the 2025 World Series?",
        "yes_bid": 19, "yes_ask": 23, "volume": 780000, "open_interest": 60000,
        "close_time": "2025-10-31T00:00:00Z", "tags": ["sports", "mlb"],
    },
    # Crypto / Tech
    {
        "ticker": "BTC-150K-2025", "title": "Will Bitcoin reach $150,000 in 2025?",
        "yes_bid": 44, "yes_ask": 48, "volume": 5600000, "open_interest": 450000,
        "close_time": "2025-12-31T23:59:00Z", "tags": ["crypto", "bitcoin"],
    },
    {
        "ticker": "ETH-5K-2025", "title": "Will Ethereum reach $5,000 in 2025?",
        "yes_bid": 36, "yes_ask": 40, "volume": 2300000, "open_interest": 190000,
        "close_time": "2025-12-31T23:59:00Z", "tags": ["crypto", "ethereum"],
    },
    {
        "ticker": "AI-GPT5-RELEASE-2025", "title": "Will OpenAI release GPT-5 before end of 2025?",
        "yes_bid": 68, "yes_ask": 72, "volume": 1500000, "open_interest": 120000,
        "close_time": "2025-12-31T23:59:00Z", "tags": ["tech", "ai"],
    },
    # Geopolitics
    {
        "ticker": "UKRAINE-CEASEFIRE-2025", "title": "Will there be a ceasefire in Ukraine by end of 2025?",
        "yes_bid": 38, "yes_ask": 42, "volume": 1100000, "open_interest": 88000,
        "close_time": "2025-12-31T23:59:00Z", "tags": ["geopolitics", "ukraine"],
    },
    {
        "ticker": "TAIWAN-CONFLICT-2025", "title": "Will there be a military conflict involving Taiwan in 2025?",
        "yes_bid": 6, "yes_ask": 10, "volume": 890000, "open_interest": 70000,
        "close_time": "2025-12-31T23:59:00Z", "tags": ["geopolitics", "taiwan"],
    },
    # Science / Climate
    {
        "ticker": "HOTTEST-YEAR-2025", "title": "Will 2025 be the hottest year on record globally?",
        "yes_bid": 58, "yes_ask": 62, "volume": 640000, "open_interest": 52000,
        "close_time": "2026-01-31T00:00:00Z", "tags": ["climate", "science"],
    },
    {
        "ticker": "MARS-MISSION-2027", "title": "Will SpaceX launch a crewed Mars mission before 2028?",
        "yes_bid": 18, "yes_ask": 22, "volume": 370000, "open_interest": 30000,
        "close_time": "2027-12-31T00:00:00Z", "tags": ["science", "space"],
    },
    # Entertainment
    {
        "ticker": "OSCARS-2026-BEST-PIC", "title": "Will 'The Brutalist' win Best Picture at the 2026 Oscars?",
        "yes_bid": 28, "yes_ask": 32, "volume": 290000, "open_interest": 24000,
        "close_time": "2026-03-02T23:59:00Z", "tags": ["entertainment", "oscars"],
    },
    {
        "ticker": "APPLE-AI-2025-FULL", "title": "Will Apple Intelligence be fully available on all iPhone 16 models by mid-2025?",
        "yes_bid": 70, "yes_ask": 74, "volume": 520000, "open_interest": 43000,
        "close_time": "2025-07-01T00:00:00Z", "tags": ["tech", "apple"],
    },
]


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def normalize_market(raw: dict) -> Optional[dict]:
    """
    Normalize a raw Kalshi v2 REST market to EdgeLab schema.
    Kalshi prices are in cents (0-100) — divide by 100 to get probability.
    Returns None if essential fields are missing.
    """
    ticker = raw.get("ticker") or raw.get("id")
    if not ticker:
        return None

    title = raw.get("title") or raw.get("subtitle") or raw.get("question")
    if not title:
        return None

    # yes_bid is the primary price field from the API
    yes_bid_raw = raw.get("yes_bid")
    yes_ask_raw = raw.get("yes_ask")

    # Convert from cents to probability
    try:
        yes_bid = float(yes_bid_raw) / 100 if yes_bid_raw is not None else None
    except (TypeError, ValueError):
        yes_bid = None

    try:
        yes_ask = float(yes_ask_raw) / 100 if yes_ask_raw is not None else None
    except (TypeError, ValueError):
        yes_ask = None

    # Use midpoint when both sides available, else whichever is present
    if yes_bid is not None and yes_ask is not None:
        prob_yes = round((yes_bid + yes_ask) / 2, 4)
    elif yes_bid is not None:
        prob_yes = round(yes_bid, 4)
    elif yes_ask is not None:
        prob_yes = round(yes_ask, 4)
    else:
        # Fall back to last_price if available
        last_price_raw = raw.get("last_price")
        try:
            prob_yes = round(float(last_price_raw) / 100, 4) if last_price_raw is not None else None
        except (TypeError, ValueError):
            prob_yes = None

    prob_no = round(1.0 - prob_yes, 4) if prob_yes is not None else None

    end_date = raw.get("close_time") or raw.get("expiration_time")
    days_to_resolution: Optional[float] = None
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            delta = end_dt - datetime.now(timezone.utc)
            days_to_resolution = max(delta.total_seconds() / 86400, 0.0)
        except (ValueError, AttributeError):
            pass

    tags = raw.get("tags") or []

    return {
        "id": str(ticker),
        "market_id": str(ticker),  # backward compat alias
        "question": title,
        "prob_yes": prob_yes,
        "prob_no": prob_no,
        "price": prob_yes,          # backward compat alias
        "volume": _safe_float(raw.get("volume") or raw.get("volume_24h")),
        "liquidity": _safe_float(raw.get("open_interest") or raw.get("liquidity")),
        "end_date": end_date,
        "days_to_resolution": days_to_resolution,
        "source": "kalshi",
        "tags": tags if isinstance(tags, list) else [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _normalize_mock(raw: dict) -> dict:
    """Normalize a mock market (prices already in cents)."""
    yes_bid = raw.get("yes_bid", 50)
    yes_ask = raw.get("yes_ask", 50)
    prob_yes = round((yes_bid + yes_ask) / 2 / 100, 4)
    prob_no = round(1.0 - prob_yes, 4)

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
        "id": raw["ticker"],
        "market_id": raw["ticker"],
        "question": raw["title"],
        "prob_yes": prob_yes,
        "prob_no": prob_no,
        "price": prob_yes,
        "volume": _safe_float(raw.get("volume")),
        "liquidity": _safe_float(raw.get("open_interest")),
        "end_date": end_date,
        "days_to_resolution": days_to_resolution,
        "source": "kalshi_mock",
        "tags": raw.get("tags", []),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# REST poller — live (cursor-based pagination)
# ---------------------------------------------------------------------------

async def _fetch_all_markets(client: httpx.AsyncClient) -> list[dict]:
    """Paginate through all open Kalshi markets and return raw market dicts."""
    headers = {
        "Authorization": f"Bearer {KALSHI_API_KEY}",
        "Content-Type": "application/json",
    }
    url = f"{KALSHI_BASE_URL}/markets"
    params: dict[str, Any] = {"status": "open", "limit": 200}
    all_markets: list[dict] = []

    while True:
        resp = await client.get(url, params=params, headers=headers, timeout=30.0)
        resp.raise_for_status()
        data = resp.json()

        markets_raw = data.get("markets", [])
        all_markets.extend(markets_raw)

        cursor = data.get("cursor")
        if not cursor or not markets_raw:
            break

        params = {"status": "open", "limit": 200, "cursor": cursor}

    return all_markets


async def _poll_live(client: httpx.AsyncClient, redis: aioredis.Redis) -> int:
    """Fetch all live markets, normalize, and publish to Redis. Returns count published."""
    markets_raw = await _fetch_all_markets(client)
    count = 0
    for raw in markets_raw:
        market = normalize_market(raw)
        if market:
            await redis.publish(REDIS_CHANNEL, json.dumps(market))
            count += 1
    logger.info("Kalshi live poll: %d markets published", count)
    return count


async def _poll_mock(redis: aioredis.Redis) -> int:
    """Publish mock markets to Redis. Returns count published."""
    count = 0
    for raw in _MOCK_MARKETS:
        market = _normalize_mock(raw)
        await redis.publish(REDIS_CHANNEL, json.dumps(market))
        count += 1
    logger.info("Kalshi mock poll: %d markets published", count)
    return count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run() -> None:
    """Start Kalshi REST poller. Uses mock data if KALSHI_API_KEY is not set."""
    api_key = os.getenv("KALSHI_API_KEY", "")
    use_mock = not api_key

    if use_mock:
        logger.warning(
            "KALSHI_API_KEY not set — using mock data for %d markets", len(_MOCK_MARKETS)
        )
    else:
        logger.info("Kalshi live feed starting (API key present)")

    redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

    try:
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    if use_mock:
                        await _poll_mock(redis)
                    else:
                        await _poll_live(client, redis)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in (401, 403):
                        logger.error(
                            "Kalshi auth error (%d) — falling back to mock data. "
                            "Check KALSHI_API_KEY.",
                            exc.response.status_code,
                        )
                        use_mock = True
                    else:
                        logger.warning(
                            "Kalshi HTTP error %d: %s",
                            exc.response.status_code,
                            exc.response.text[:200],
                        )
                except httpx.RequestError as exc:
                    logger.warning("Kalshi request error: %s", exc)
                except Exception as exc:
                    logger.error("Unexpected Kalshi error: %s", exc, exc_info=True)

                await asyncio.sleep(REST_POLL_INTERVAL)
    finally:
        await redis.aclose()
        logger.info("Kalshi live feed stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    asyncio.run(run())
