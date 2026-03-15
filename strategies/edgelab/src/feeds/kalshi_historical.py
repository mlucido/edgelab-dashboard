"""
Kalshi Historical Feed — fetches resolved/settled markets from the Kalshi
historical API (no auth required) and stores them to data/kalshi_historical.json.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
HISTORICAL_MARKETS_URL = f"{KALSHI_BASE_URL}/historical/markets"
HISTORICAL_TRADES_URL = f"{KALSHI_BASE_URL}/historical/trades"
HISTORICAL_CUTOFF_URL = f"{KALSHI_BASE_URL}/historical/cutoff"

_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "data"
)
OUTPUT_PATH = os.path.join(_DATA_DIR, "kalshi_historical.json")

# Seconds to sleep between paginated requests (avoid hammering the API)
RATE_LIMIT_SLEEP = 0.25

# Max markets per page (API cap)
PAGE_LIMIT = 200


# ---------------------------------------------------------------------------
# Category mapping
# ---------------------------------------------------------------------------

# Series ticker prefix → normalized category
_SERIES_PREFIX_MAP: dict[str, str] = {
    # Crypto
    "KXBTC": "Crypto",
    "KXETH": "Crypto",
    "KXSOL": "Crypto",
    "KXDOGE": "Crypto",
    "KXCRYPTO": "Crypto",
    # Finance / equities
    "KXSPY": "Finance",
    "KXNASDAQ": "Finance",
    "KXDOW": "Finance",
    "KXSPX": "Finance",
    "KXSTOCKS": "Finance",
    # Economics / macro
    "KXCPI": "Economics",
    "KXPCE": "Economics",
    "KXGDP": "Economics",
    "KXJOBS": "Economics",
    "KXFED": "Economics",
    "KXRATE": "Economics",
    "KXUNEMP": "Economics",
    # Politics
    "KXPRES": "Politics",
    "KXSENATE": "Politics",
    "KXHOUSE": "Politics",
    "KXGOV": "Politics",
    "KXELECT": "Politics",
    "KXSCOTUS": "Politics",
    # Sports
    "KXNFL": "Sports",
    "KXNBA": "Sports",
    "KXMLB": "Sports",
    "KXNHL": "Sports",
    "KXSOCCER": "Sports",
    "KXNCAA": "Sports",
    "KXSPORTS": "Sports",
    # Weather
    "KXWEATHER": "Weather",
    "KXHURR": "Weather",
    "KXTEMP": "Weather",
    # Science
    "KXSCIENCE": "Science",
    "KXSPACE": "Science",
    "KXCLIMATE": "Science",
    "KXHEALTH": "Science",
    # Entertainment
    "KXENTERTAIN": "Entertainment",
    "KXOSCARS": "Entertainment",
    "KXGRAMMY": "Entertainment",
    "KXMUSIC": "Entertainment",
    "KXMOVIE": "Entertainment",
}

# Tag/keyword → normalized category (fallback when prefix map misses)
_TAG_KEYWORD_MAP: dict[str, str] = {
    "bitcoin": "Crypto",
    "crypto": "Crypto",
    "ethereum": "Crypto",
    "blockchain": "Crypto",
    "fed": "Economics",
    "inflation": "Economics",
    "recession": "Economics",
    "gdp": "Economics",
    "cpi": "Economics",
    "unemployment": "Economics",
    "interest rate": "Economics",
    "stock": "Finance",
    "equity": "Finance",
    "s&p": "Finance",
    "nasdaq": "Finance",
    "dow": "Finance",
    "election": "Politics",
    "president": "Politics",
    "senate": "Politics",
    "congress": "Politics",
    "scotus": "Politics",
    "politics": "Politics",
    "nfl": "Sports",
    "nba": "Sports",
    "mlb": "Sports",
    "nhl": "Sports",
    "soccer": "Sports",
    "sports": "Sports",
    "hurricane": "Weather",
    "weather": "Weather",
    "temperature": "Weather",
    "climate": "Science",
    "science": "Science",
    "spacex": "Science",
    "nasa": "Science",
    "oscar": "Entertainment",
    "grammy": "Entertainment",
    "emmy": "Entertainment",
    "entertainment": "Entertainment",
}


def map_category(market: dict) -> str:
    """
    Map a raw Kalshi market to a normalized category.

    Resolution order:
    1. series_ticker prefix (most reliable)
    2. event_ticker prefix
    3. Tags / category field from API
    4. Title keyword scan
    5. "Other"
    """
    # 1. Try series_ticker prefix
    for prefix, category in _SERIES_PREFIX_MAP.items():
        series = market.get("series_ticker") or ""
        if series.upper().startswith(prefix):
            return category

    # 2. Try event_ticker prefix
    for prefix, category in _SERIES_PREFIX_MAP.items():
        event = market.get("event_ticker") or ""
        if event.upper().startswith(prefix):
            return category

    # 3. Check API-provided category/tags fields
    api_category = (market.get("category") or "").lower()
    tags: list[str] = market.get("tags") or []
    combined_tags = " ".join(t.lower() for t in tags) + " " + api_category

    for keyword, category in _TAG_KEYWORD_MAP.items():
        if keyword in combined_tags:
            return category

    # 4. Scan title
    title = (market.get("title") or market.get("subtitle") or "").lower()
    for keyword, category in _TAG_KEYWORD_MAP.items():
        if keyword in title:
            return category

    return "Other"


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def normalize_historical_market(raw: dict) -> Optional[dict]:
    """
    Normalize a raw Kalshi historical market object to EdgeLab schema.
    Returns None if essential fields are missing.
    Kalshi prices are in cents (0–100).
    """
    ticker = raw.get("ticker")
    if not ticker:
        return None

    title = raw.get("title") or raw.get("subtitle")
    if not title:
        return None

    # Derive probability from yes_bid/yes_ask midpoint; fall back to result
    yes_bid_raw = raw.get("yes_bid")
    yes_ask_raw = raw.get("yes_ask")

    try:
        yes_bid = float(yes_bid_raw) / 100 if yes_bid_raw is not None else None
    except (TypeError, ValueError):
        yes_bid = None

    try:
        yes_ask = float(yes_ask_raw) / 100 if yes_ask_raw is not None else None
    except (TypeError, ValueError):
        yes_ask = None

    if yes_bid is not None and yes_ask is not None:
        prob_yes = round((yes_bid + yes_ask) / 2, 4)
    elif yes_bid is not None:
        prob_yes = round(yes_bid, 4)
    elif yes_ask is not None:
        prob_yes = round(yes_ask, 4)
    else:
        last_raw = raw.get("last_price")
        try:
            prob_yes = round(float(last_raw) / 100, 4) if last_raw is not None else None
        except (TypeError, ValueError):
            prob_yes = None

    prob_no = round(1.0 - prob_yes, 4) if prob_yes is not None else None

    # Result field: "yes" | "no" | None
    result = raw.get("result")
    resolved_yes: Optional[bool] = None
    if result == "yes":
        resolved_yes = True
    elif result == "no":
        resolved_yes = False

    close_time = raw.get("close_time")

    return {
        "ticker": str(ticker),
        "title": title,
        "subtitle": raw.get("subtitle"),
        "event_ticker": raw.get("event_ticker"),
        "series_ticker": raw.get("series_ticker"),
        "status": raw.get("status"),
        "result": result,
        "resolved_yes": resolved_yes,
        "prob_yes": prob_yes,
        "prob_no": prob_no,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": _safe_float(raw.get("no_bid")) / 100
            if raw.get("no_bid") is not None else None,
        "no_ask": _safe_float(raw.get("no_ask")) / 100
            if raw.get("no_ask") is not None else None,
        "volume": _safe_float(raw.get("volume")),
        "open_interest": _safe_float(raw.get("open_interest")),
        "close_time": close_time,
        "category": map_category(raw),
        "tags": raw.get("tags") or [],
        "source": "kalshi_historical",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Async fetch
# ---------------------------------------------------------------------------

async def fetch_historical_markets(
    *,
    limit_per_page: int = PAGE_LIMIT,
    max_pages: Optional[int] = None,
    event_ticker: Optional[str] = None,
    tickers: Optional[list[str]] = None,
) -> list[dict]:
    """
    Fetch all resolved/settled Kalshi historical markets via cursor pagination.

    Args:
        limit_per_page: Markets per API request (max 200).
        max_pages: If set, stop after this many pages (useful for testing).
        event_ticker: Filter by event ticker.
        tickers: Filter by specific market tickers.

    Returns:
        List of normalized market dicts. Empty list on API failure.
    """
    markets: list[dict] = []

    params: dict[str, Any] = {"limit": limit_per_page}
    if event_ticker:
        params["event_ticker"] = event_ticker
    if tickers:
        params["tickers"] = ",".join(tickers)

    page = 0
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Check cutoff availability first (best-effort, non-fatal)
        try:
            cutoff_resp = await client.get(HISTORICAL_CUTOFF_URL)
            if cutoff_resp.status_code == 200:
                cutoff_data = cutoff_resp.json()
                logger.info("Kalshi historical cutoff: %s", cutoff_data)
        except Exception as exc:
            logger.debug("Could not fetch historical cutoff: %s", exc)

        while True:
            try:
                resp = await client.get(HISTORICAL_MARKETS_URL, params=params)
                resp.raise_for_status()
                data = resp.json()
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "Kalshi historical API HTTP error %d: %s",
                    exc.response.status_code,
                    exc.response.text[:300],
                )
                break
            except httpx.RequestError as exc:
                logger.warning("Kalshi historical API unreachable: %s", exc)
                break
            except Exception as exc:
                logger.error("Unexpected error fetching Kalshi historical markets: %s", exc, exc_info=True)
                break

            raw_markets: list[dict] = data.get("markets", [])
            if not raw_markets:
                logger.debug("Kalshi historical: empty page, stopping pagination")
                break

            for raw in raw_markets:
                # Only keep resolved/settled markets
                status = raw.get("status", "")
                if status not in ("settled", "resolved", "finalized"):
                    continue
                normalized = normalize_historical_market(raw)
                if normalized:
                    markets.append(normalized)

            page += 1
            logger.debug(
                "Kalshi historical: page %d fetched %d raw, %d normalized so far",
                page, len(raw_markets), len(markets),
            )

            cursor = data.get("cursor")
            if not cursor:
                break

            if max_pages and page >= max_pages:
                logger.info("Kalshi historical: reached max_pages=%d, stopping", max_pages)
                break

            params = dict(params)
            params["cursor"] = cursor

            await asyncio.sleep(RATE_LIMIT_SLEEP)

    logger.info("Kalshi historical fetch complete: %d resolved markets", len(markets))

    # Persist to disk
    if markets:
        _ensure_data_dir()
        _write_output(markets)

    return markets


def _ensure_data_dir() -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)


def _write_output(markets: list[dict]) -> None:
    tmp_path = OUTPUT_PATH + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "count": len(markets),
                    "markets": markets,
                },
                fh,
                indent=2,
                ensure_ascii=False,
            )
        os.replace(tmp_path, OUTPUT_PATH)
        logger.info("Kalshi historical data written to %s (%d markets)", OUTPUT_PATH, len(markets))
    except OSError as exc:
        logger.error("Failed to write Kalshi historical data: %s", exc)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Sync wrapper
# ---------------------------------------------------------------------------

def fetch_and_store(
    *,
    limit_per_page: int = PAGE_LIMIT,
    max_pages: Optional[int] = None,
    event_ticker: Optional[str] = None,
    tickers: Optional[list[str]] = None,
) -> list[dict]:
    """
    Synchronous wrapper around fetch_historical_markets().

    Safe to call from non-async contexts (scripts, tests, CLI).
    Returns the list of normalized markets (empty on failure).
    """
    return asyncio.run(
        fetch_historical_markets(
            limit_per_page=limit_per_page,
            max_pages=max_pages,
            event_ticker=event_ticker,
            tickers=tickers,
        )
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    markets = fetch_and_store()
    print(f"Fetched {len(markets)} resolved markets → {OUTPUT_PATH}")
