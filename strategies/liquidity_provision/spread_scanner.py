"""
Spread Scanner — scans Kalshi markets for LP opportunities.
Rate-limits orderbook fetches to max 10 req/sec via threading.Semaphore.
"""
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

try:
    import httpx
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

from . import config

logger = logging.getLogger("liquidity_provision.spread_scanner")

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Semaphore for rate limiting: max KALSHI_ORDERBOOK_RATE_LIMIT concurrent requests
_rate_sem = threading.Semaphore(config.KALSHI_ORDERBOOK_RATE_LIMIT)
_last_batch_time = 0.0
_batch_lock = threading.Lock()


@dataclass
class SpreadOpportunity:
    market_id: str
    title: str
    best_bid: float
    best_ask: float
    spread_pct: float
    volume_24h: float
    days_to_resolution: float
    category: str
    lp_score: float
    raw_market: dict = field(default_factory=dict, repr=False)


def _days_to_resolution_score(days: float) -> float:
    """Score days-to-resolution: prefer mid-range windows (2–5 days)."""
    if days <= 0:
        return 0.0
    if days < 1:
        return 0.1
    if days <= 5:
        return 1.0
    if days <= 7:
        return 0.7
    return 0.4


def _parse_resolution_dt(market: dict) -> Optional[datetime]:
    """Extract resolution datetime from market dict, return None if unparseable."""
    for key in ("close_time", "expiration_time", "expected_expiration_time"):
        val = market.get(key)
        if val:
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00"))
            except Exception:
                continue
    return None


def _get_category(market: dict) -> str:
    """Best-effort category extraction from market tags/series."""
    for key in ("category", "series_ticker", "group"):
        val = market.get(key, "")
        if val:
            return str(val).lower()
    return "unknown"


def _rate_limited_get(client: httpx.Client, url: str, **kwargs) -> Optional[httpx.Response]:
    """GET with per-second rate limiting via semaphore."""
    global _last_batch_time
    with _rate_sem:
        with _batch_lock:
            now = time.monotonic()
            elapsed = now - _last_batch_time
            if elapsed < (1.0 / config.KALSHI_ORDERBOOK_RATE_LIMIT):
                time.sleep((1.0 / config.KALSHI_ORDERBOOK_RATE_LIMIT) - elapsed)
            _last_batch_time = time.monotonic()
        try:
            resp = client.get(url, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            logger.warning(f"API request failed {url}: {e}")
            return None


def _fetch_open_markets() -> list:
    """Fetch up to 200 open Kalshi markets."""
    try:
        with httpx.Client(timeout=15) as client:
            resp = _rate_limited_get(
                client,
                f"{KALSHI_BASE}/markets",
                params={"status": "open", "limit": 200},
            )
            if resp is not None:
                return resp.json().get("markets", [])
    except Exception as e:
        logger.error(f"Failed to fetch open markets: {e}")
    return []


def _fetch_orderbook(client: httpx.Client, ticker: str) -> Optional[dict]:
    """Fetch orderbook for a single market ticker."""
    resp = _rate_limited_get(client, f"{KALSHI_BASE}/markets/{ticker}/orderbook")
    if resp is not None:
        return resp.json().get("orderbook", {})
    return None


def scan_for_spreads() -> list:
    """
    Main scan function. Returns list of SpreadOpportunity sorted by lp_score desc.
    Gracefully returns empty list on any API failure.
    """
    opportunities = []
    now_utc = datetime.now(timezone.utc)

    markets = _fetch_open_markets()
    if not markets:
        logger.warning("No markets returned from Kalshi — returning empty scan")
        return []

    logger.info(f"Scanning {len(markets)} open markets for LP opportunities")

    with httpx.Client(timeout=10) as client:
        for market in markets:
            ticker = market.get("ticker") or market.get("id", "")
            if not ticker:
                continue

            # Category filter
            category = _get_category(market)
            if any(avoid in category for avoid in config.AVOID_CATEGORIES):
                continue

            # Time-to-resolution filter
            res_dt = _parse_resolution_dt(market)
            if res_dt is None:
                continue
            hours_remaining = (res_dt - now_utc).total_seconds() / 3600
            if not (config.MIN_TIME_TO_RESOLUTION_HOURS <= hours_remaining
                    <= config.MAX_TIME_TO_RESOLUTION_HOURS):
                continue

            # Fetch orderbook
            orderbook = _fetch_orderbook(client, ticker)
            if not orderbook:
                continue

            # Parse best bid/ask
            yes_bids = orderbook.get("yes", [])   # list of {price, quantity}
            yes_asks = orderbook.get("no", [])    # NO price = 1 - YES ask

            try:
                # Kalshi orderbook: "yes" side has bids sorted desc, "no" side has asks
                best_bid = max((lvl.get("price", 0) for lvl in yes_bids), default=0) / 100.0
                best_ask_no = max((lvl.get("price", 0) for lvl in yes_asks), default=0) / 100.0
                best_ask = round(1.0 - best_ask_no, 4) if best_ask_no > 0 else 0
            except Exception:
                continue

            if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
                continue

            spread_pct = round(best_ask - best_bid, 4)
            if spread_pct < config.MIN_SPREAD_PCT:
                continue

            # Volume (24h) — use volume or open_interest as proxy
            volume_24h = float(market.get("volume_24h", market.get("volume", 0)) or 0)

            days_to_res = hours_remaining / 24.0
            res_score = _days_to_resolution_score(days_to_res)

            # Prefer bonus
            prefer_bonus = 1.2 if any(p in category for p in config.PREFER_CATEGORIES) else 1.0

            lp_score = round(spread_pct * (1 + volume_24h / 1000) * res_score * prefer_bonus, 4)

            opp = SpreadOpportunity(
                market_id=ticker,
                title=market.get("title", ticker),
                best_bid=best_bid,
                best_ask=best_ask,
                spread_pct=spread_pct,
                volume_24h=volume_24h,
                days_to_resolution=round(days_to_res, 2),
                category=category,
                lp_score=lp_score,
                raw_market=market,
            )
            opportunities.append(opp)

    opportunities.sort(key=lambda o: o.lp_score, reverse=True)
    logger.info(f"Found {len(opportunities)} LP opportunities above {config.MIN_SPREAD_PCT:.0%} spread")
    return opportunities
