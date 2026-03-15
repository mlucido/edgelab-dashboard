"""
Lag Scanner — given a resolved event with a certainty score, find prediction
markets where the price hasn't yet adjusted to the known outcome.

PRIMARY SCAN: Intra-Kalshi CLOB lag — settled/finalized markets whose orderbook
still has stale orders priced away from settlement value.

SECONDARY SCAN (fallback): Cross-platform keyword matching — find open markets on
other platforms that haven't reacted to a resolved event.

Returns list of LagOpportunity dicts:
  {
    market_id: str,
    platform: str,
    expected_resolution: str,       # "YES" | "NO"
    current_price: float,           # 0.0–1.0
    expected_price: float,          # where price should go (0.98 for YES, 0.02 for NO)
    edge_pct: float,                # |expected - current| - fees - slippage
    certainty_score: float,
    time_since_resolution: float,   # seconds
    urgency: str,                   # "IMMEDIATE" | "FADING" | "GONE"
    market_title: str,
    scan_type: str,                 # "intra_kalshi_clob" | "cross_platform"
  }
"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import httpx
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

from . import config

logger = logging.getLogger("resolution_lag.scanner")

# Cache for open markets fetch — avoid hammering Kalshi API
_open_markets_cache: list = []
_open_markets_cache_ts: float = 0
_OPEN_MARKETS_CACHE_TTL = 120  # seconds — refresh every 2 minutes max

# Intra-Kalshi CLOB: conservative settlement values (leave 2% margin)
_INTRA_YES_EXPECTED = 0.98
_INTRA_NO_EXPECTED = 0.02

# Cross-platform: wider margin
_CROSS_YES_EXPECTED = 0.95
_CROSS_NO_EXPECTED = 0.05

# Urgency thresholds
_URGENCY_IMMEDIATE_THRESHOLD = 0.15  # >15% away from fair value
_URGENCY_FADING_THRESHOLD = 0.05     # 5–15% away


# ── PRIMARY SCAN: Intra-Kalshi CLOB lag ─────────────────────────────────────

def _fetch_kalshi_orderbook(ticker: str) -> dict | None:
    """
    Fetch current orderbook for a Kalshi market.
    Returns dict with 'yes' and 'no' sides, each having bids/asks,
    or None on failure.  Public endpoint — no auth required.
    """
    try:
        url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}/orderbook"
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                logger.debug(f"Orderbook fetch HTTP {resp.status_code} for {ticker}")
                return None
            return resp.json().get("orderbook", resp.json())
    except Exception as e:
        logger.debug(f"Orderbook fetch failed for {ticker}: {e}")
        return None


def _extract_best_prices(orderbook: dict) -> dict:
    """
    Extract best yes_ask and best yes_bid from orderbook response.

    Kalshi orderbook format (actual API response):
      {"orderbook_fp": {"yes_dollars": [[price_cents, qty], ...],
                        "no_dollars": [[price_cents, qty], ...]}}
    OR legacy:
      {"yes": [[price, qty], ...], "no": [[price, qty], ...]}

    Prices are in cents (1-99).
    """
    result = {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None}

    # Handle actual Kalshi format: orderbook_fp.yes_dollars / no_dollars
    ob = orderbook.get("orderbook_fp", orderbook)
    yes_side = ob.get("yes_dollars", ob.get("yes", []))
    no_side = ob.get("no_dollars", ob.get("no", []))

    def _extract_prices(levels):
        prices = []
        for level in levels:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                prices.append(int(level[0]))
            elif isinstance(level, dict):
                prices.append(int(level.get("price", 0)))
        return prices

    # yes_dollars = offers to sell YES contracts (asks)
    yes_prices = _extract_prices(yes_side)
    if yes_prices:
        result["yes_ask"] = min(yes_prices) / 100.0

    # no_dollars = offers to sell NO contracts
    # NO ask at X cents ↔ YES bid at (100 - X) cents
    no_prices = _extract_prices(no_side)
    if no_prices:
        best_no_ask = min(no_prices)
        result["no_ask"] = best_no_ask / 100.0
        result["yes_bid"] = (100 - best_no_ask) / 100.0

    return result


def _scan_intra_kalshi_clob(resolved_event: dict, certainty: dict, time_since: float) -> list:
    """
    PRIMARY SCAN: Check if the settled market's own CLOB still has stale orders.

    If market resolved YES but orderbook still has YES asks < 0.95 → buy YES cheap.
    If market resolved NO but orderbook still has YES bids > 0.05 → sell YES / buy NO.
    """
    ticker = resolved_event.get("_kalshi_ticker", "")
    if not ticker:
        return []

    winning_side = resolved_event.get("_winning_side", "").upper()
    if winning_side not in ("YES", "NO"):
        return []

    result_str = winning_side.lower()  # "yes" or "no"
    certainty_score = certainty.get("score", 0.0)
    title = resolved_event.get("event_description", ticker)

    orderbook = _fetch_kalshi_orderbook(ticker)
    if not orderbook:
        logger.debug(f"No orderbook data for settled market {ticker}")
        return []

    prices = _extract_best_prices(orderbook)
    logger.debug(f"Orderbook {ticker} (result={result_str}): {prices}")

    opportunities = []

    if result_str == "yes":
        # Market resolved YES → should be trading at ~1.0
        # Look for cheap YES asks (someone selling YES below fair value)
        best_yes_ask = prices.get("yes_ask")
        if best_yes_ask is not None and best_yes_ask < 0.95:
            edge = _INTRA_YES_EXPECTED - best_yes_ask
            total_costs = config.ESTIMATED_FEE_PCT + config.ESTIMATED_SLIPPAGE_PCT
            net_edge = edge - total_costs

            if net_edge >= config.MIN_EDGE_PCT:
                urgency = _classify_urgency(edge)
                if urgency != "GONE":
                    opp = _build_opportunity(
                        ticker=ticker,
                        title=title,
                        side="YES",
                        current_price=best_yes_ask,
                        expected_price=_INTRA_YES_EXPECTED,
                        edge_pct=net_edge,
                        certainty_score=certainty_score,
                        time_since=time_since,
                        urgency=urgency,
                        resolved_event=resolved_event,
                        scan_type="intra_kalshi_clob",
                    )
                    opportunities.append(opp)
                    logger.info(
                        f"INTRA-KALSHI LAG: {ticker} | result=YES | "
                        f"yes_ask={best_yes_ask:.3f} → buy YES | "
                        f"edge={net_edge:.1%} | urgency={urgency}"
                    )

    elif result_str == "no":
        # Market resolved NO → YES should be trading at ~0.0
        # Look for YES bids still sitting high (someone buying YES at stale price)
        best_yes_bid = prices.get("yes_bid")
        if best_yes_bid is not None and best_yes_bid > 0.05:
            edge = best_yes_bid - _INTRA_NO_EXPECTED
            total_costs = config.ESTIMATED_FEE_PCT + config.ESTIMATED_SLIPPAGE_PCT
            net_edge = edge - total_costs

            if net_edge >= config.MIN_EDGE_PCT:
                urgency = _classify_urgency(edge)
                if urgency != "GONE":
                    opp = _build_opportunity(
                        ticker=ticker,
                        title=title,
                        side="NO",
                        current_price=best_yes_bid,
                        expected_price=_INTRA_NO_EXPECTED,
                        edge_pct=net_edge,
                        certainty_score=certainty_score,
                        time_since=time_since,
                        urgency=urgency,
                        resolved_event=resolved_event,
                        scan_type="intra_kalshi_clob",
                    )
                    opportunities.append(opp)
                    logger.info(
                        f"INTRA-KALSHI LAG: {ticker} | result=NO | "
                        f"yes_bid={best_yes_bid:.3f} → buy NO | "
                        f"edge={net_edge:.1%} | urgency={urgency}"
                    )

    return opportunities


# ── SECONDARY SCAN: Cross-platform keyword matching ────────────────────────

def _fetch_open_kalshi_markets(keywords: list) -> list:
    """Fetch open Kalshi markets and filter to those matching keywords.
    Public endpoint — no auth required.  Caches full market list for 2 min."""
    global _open_markets_cache, _open_markets_cache_ts

    now = time.time()
    if now - _open_markets_cache_ts > _OPEN_MARKETS_CACHE_TTL or not _open_markets_cache:
        try:
            with httpx.Client(timeout=15) as client:
                resp = client.get(
                    "https://api.elections.kalshi.com/trade-api/v2/markets",
                    params={"status": "open", "limit": 200},
                )
                if resp.status_code == 429:
                    logger.debug("Kalshi open markets rate limited — using cache")
                    # Fall through to use stale cache
                elif resp.status_code != 200:
                    logger.warning(f"Kalshi open markets HTTP {resp.status_code}")
                else:
                    _open_markets_cache = resp.json().get("markets", [])
                    _open_markets_cache_ts = now
        except Exception as e:
            logger.warning(f"Failed to fetch open Kalshi markets: {e}")

    # Filter cached markets by keywords
    kw_lower = [k.lower() for k in keywords]
    markets = []
    for market in _open_markets_cache:
        title = market.get("title", "").lower()
        ticker = market.get("ticker", "").lower()
        if any(kw in title or kw in ticker for kw in kw_lower):
            markets.append(market)

    return markets


def _get_current_price(market: dict):
    """Extract the current YES bid/ask midpoint from a market dict."""
    yes_ask = market.get("yes_ask")
    yes_bid = market.get("yes_bid")
    last = market.get("last_price")

    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / 2
    if yes_ask is not None:
        return yes_ask
    if yes_bid is not None:
        return yes_bid
    if last is not None:
        return last

    result = market.get("result", {})
    if isinstance(result, dict) and result.get("yes_price") is not None:
        return result["yes_price"]

    return None


def _scan_cross_platform(resolved_event: dict, certainty: dict, time_since: float) -> list:
    """
    SECONDARY SCAN: Find open markets on other platforms matching resolved event keywords.
    """
    keywords = resolved_event.get("relevant_market_keywords", [])
    if not keywords:
        return []

    recommended_side = certainty.get("recommended_side", "YES")
    certainty_score = certainty.get("score", 0.0)

    open_markets = _fetch_open_kalshi_markets(keywords)
    if not open_markets:
        logger.debug(f"Cross-platform: no open markets matched keywords: {keywords[:3]}")
        return []

    opportunities = []

    for market in open_markets:
        current_price = _get_current_price(market)
        if current_price is None:
            continue

        if recommended_side == "YES":
            expected_price = _CROSS_YES_EXPECTED
            if current_price >= 0.90:
                continue
        elif recommended_side == "NO":
            expected_price = _CROSS_NO_EXPECTED
            if current_price <= 0.10:
                continue
        else:
            continue

        price_gap = abs(expected_price - current_price)
        total_costs = config.ESTIMATED_FEE_PCT + config.ESTIMATED_SLIPPAGE_PCT
        edge_pct = price_gap - total_costs

        if edge_pct < config.MIN_EDGE_PCT:
            continue

        urgency = _classify_urgency(price_gap)
        if urgency == "GONE":
            continue

        ticker = market.get("ticker", market.get("id", "unknown"))
        title = market.get("title", ticker)

        opp = _build_opportunity(
            ticker=ticker,
            title=title,
            side=recommended_side,
            current_price=current_price,
            expected_price=expected_price,
            edge_pct=edge_pct,
            certainty_score=certainty_score,
            time_since=time_since,
            urgency=urgency,
            resolved_event=resolved_event,
            scan_type="cross_platform",
        )
        opportunities.append(opp)

        logger.info(
            f"CROSS-PLATFORM LAG: {ticker} | side={recommended_side} | "
            f"current={current_price:.3f} expected={expected_price:.3f} | "
            f"edge={edge_pct:.1%} | urgency={urgency}"
        )

    return opportunities


# ── Shared helpers ──────────────────────────────────────────────────────────

def _classify_urgency(price_gap: float) -> str:
    if price_gap >= _URGENCY_IMMEDIATE_THRESHOLD:
        return "IMMEDIATE"
    elif price_gap >= _URGENCY_FADING_THRESHOLD:
        return "FADING"
    return "GONE"


def _build_opportunity(
    ticker: str, title: str, side: str,
    current_price: float, expected_price: float, edge_pct: float,
    certainty_score: float, time_since: float, urgency: str,
    resolved_event: dict, scan_type: str,
) -> dict:
    return {
        "market_id": ticker,
        "platform": "kalshi",
        "expected_resolution": side,
        "current_price": round(current_price, 4),
        "expected_price": round(expected_price, 4),
        "edge_pct": round(edge_pct, 4),
        "certainty_score": certainty_score,
        "time_since_resolution": round(time_since, 1),
        "urgency": urgency,
        "market_title": title,
        "event_description": resolved_event.get("event_description", "")[:100],
        "source_method": resolved_event.get("_source_method", "unknown"),
        "scan_type": scan_type,
    }


def _parse_time_since_resolution(resolved_at_str: str) -> float:
    """Return seconds since resolution timestamp."""
    try:
        resolved_at = datetime.fromisoformat(resolved_at_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - resolved_at).total_seconds()
    except Exception:
        return 0.0


# ── Main entry point ────────────────────────────────────────────────────────

def scan_for_lag(resolved_event: dict, certainty: dict) -> list:
    """
    Main entry point. Two-phase scan:
      1. PRIMARY: Intra-Kalshi CLOB lag (settled market with stale orderbook)
      2. SECONDARY: Cross-platform keyword matching (fallback if primary finds nothing)

    Returns list of LagOpportunity dicts, sorted by edge_pct descending.
    """
    if not certainty.get("passes_threshold"):
        logger.debug("Certainty below threshold — skipping lag scan")
        return []

    time_since = _parse_time_since_resolution(resolved_event.get("resolved_at", ""))

    if time_since > config.MAX_LAG_WINDOW_SECS:
        logger.debug(f"Resolution too old ({time_since:.0f}s > {config.MAX_LAG_WINDOW_SECS}s)")
        return []
    if time_since < config.MIN_LAG_WINDOW_SECS:
        logger.debug(f"Resolution too fresh ({time_since:.0f}s < {config.MIN_LAG_WINDOW_SECS}s)")
        return []

    # PRIMARY: Intra-Kalshi CLOB lag
    opportunities = _scan_intra_kalshi_clob(resolved_event, certainty, time_since)
    if opportunities:
        logger.info(f"Primary scan found {len(opportunities)} intra-Kalshi CLOB opportunity(s)")
        opportunities.sort(key=lambda x: x["edge_pct"], reverse=True)
        return opportunities

    # SECONDARY: Cross-platform keyword matching (only if primary found nothing)
    opportunities = _scan_cross_platform(resolved_event, certainty, time_since)
    if opportunities:
        logger.info(f"Secondary scan found {len(opportunities)} cross-platform opportunity(s)")

    opportunities.sort(key=lambda x: x["edge_pct"], reverse=True)
    return opportunities
