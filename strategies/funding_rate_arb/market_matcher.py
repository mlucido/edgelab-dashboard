"""
Matches asset + direction to open Kalshi range markets.

BEARISH on BTC → look for "BTC above $X" markets near current price → trade NO
BULLISH on BTC → find "BTC above $X" markets below current price → trade YES
"""
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    import httpx
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

from . import config

logger = logging.getLogger("funding_arb.matcher")

KALSHI_MARKETS_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"


def _fetch_kalshi_markets(asset: str) -> list:
    """Fetch open Kalshi markets with asset prefix."""
    prefix = config.ASSETS.get(asset, {}).get("kalshi_prefix", f"{asset}-")
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                KALSHI_MARKETS_URL,
                params={"status": "open", "limit": 200, "series_ticker": prefix.rstrip("-")},
            )
            if resp.status_code == 200:
                return resp.json().get("markets", [])
            # Try without series_ticker filter
            resp2 = client.get(
                KALSHI_MARKETS_URL,
                params={"status": "open", "limit": 200},
            )
            if resp2.status_code == 200:
                all_markets = resp2.json().get("markets", [])
                return [m for m in all_markets
                        if prefix.lower() in m.get("ticker", "").lower()
                        or prefix.lower() in m.get("title", "").lower()]
    except Exception as e:
        logger.warning(f"Kalshi market fetch failed for {asset}: {e}")
    return []


def _extract_price_boundary(market: dict) -> Optional[float]:
    """
    Extract the dollar boundary from a range market title.
    e.g. "Will BTC be above $95,000 on Jan 15?" → 95000.0
    """
    title = market.get("title", "") + " " + market.get("subtitle", "")
    # Match patterns like $95,000 or $95000 or 95,000
    matches = re.findall(r"\$?([\d,]+(?:\.\d+)?)", title)
    for m in matches:
        val = float(m.replace(",", ""))
        if val > 1000:  # Realistic crypto price boundary
            return val
    return None


def _expiry_hours(market: dict) -> float:
    """Return hours until market expiry. Returns inf if unparseable."""
    close_time = market.get("close_time") or market.get("expiration_time", "")
    if not close_time:
        return float("inf")
    try:
        if isinstance(close_time, (int, float)):
            exp = datetime.fromtimestamp(close_time / 1000, tz=timezone.utc)
        else:
            exp = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
        delta = exp - datetime.now(timezone.utc)
        return delta.total_seconds() / 3600
    except Exception:
        return float("inf")


def match_asset_to_markets(asset: str, direction: str, current_price: float) -> list:
    """
    Find up to 3 Kalshi range markets matching the signal.

    BEARISH → "above $X" markets where X ≈ current_price → trade NO
    BULLISH → "above $X" markets where X < current_price → trade YES

    Returns list of dicts: {market_id, title, boundary, side, hours_to_expiry, score}
    """
    markets = _fetch_kalshi_markets(asset)
    if not markets:
        logger.warning(f"No Kalshi markets found for {asset}")
        return []

    candidates = []
    for m in markets:
        hours = _expiry_hours(m)
        if hours > config.SIGNAL_WINDOW_HOURS or hours <= 0:
            continue  # Outside signal window

        boundary = _extract_price_boundary(m)
        if boundary is None:
            continue

        # Determine which side to trade
        if direction == "BEARISH":
            # Want to bet price falls below boundary → trade NO on "above X"
            # Best when boundary ≈ current price (most liquid, highest edge)
            side = "NO"
            distance = abs(boundary - current_price) / current_price
            # Score: closer boundary + sooner expiry = better
            score = 1.0 - distance - (hours / config.SIGNAL_WINDOW_HOURS * 0.1)
        else:  # BULLISH
            # Want to bet price stays above boundary → trade YES on "above X"
            # Only if boundary is below current price
            if boundary >= current_price:
                continue
            side = "YES"
            distance = (current_price - boundary) / current_price
            score = 1.0 - distance - (hours / config.SIGNAL_WINDOW_HOURS * 0.1)

        market_id = m.get("ticker") or m.get("id", "")
        candidates.append({
            "market_id": market_id,
            "title": m.get("title", ""),
            "boundary": boundary,
            "side": side,
            "hours_to_expiry": round(hours, 2),
            "yes_ask": m.get("yes_ask", 0.5),
            "no_ask": m.get("no_ask", 0.5),
            "score": round(score, 4),
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:3]
    logger.info(f"[MATCH] {asset} {direction} @ ${current_price:,.0f} → {len(top)} markets")
    return top
