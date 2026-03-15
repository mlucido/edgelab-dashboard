"""
polymarket_client.py — Read-only Polymarket API client.

Uses two public endpoints (no auth required):
  - Gamma API  → market metadata, questions, end dates, categories
  - CLOB API   → live order books, YES/NO prices, liquidity

Scans for short-duration crypto directional markets and returns
structured PolyMarket objects ready for the signal engine.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import aiohttp

import config

log = logging.getLogger(__name__)


@dataclass
class PolyMarket:
    condition_id:    str
    question:        str
    asset:           str        # BTC / ETH / SOL
    direction:       str        # UP / DOWN
    yes_token_id:    str
    no_token_id:     str
    yes_price:       float      # 0–1
    no_price:        float      # 0–1
    spread:          float      # yes_price + no_price - 1 (ideally ~0)
    liquidity:       float      # USD
    end_date:        datetime
    hours_remaining: float

    @property
    def is_tradeable(self) -> bool:
        return (
            self.liquidity >= config.MIN_MARKET_LIQUIDITY
            and abs(self.spread) <= config.MAX_BID_ASK_SPREAD
            and 0 < self.hours_remaining <= config.MAX_MARKET_HOURS_TO_CLOSE
        )


def _detect_asset_direction(question: str) -> tuple[Optional[str], Optional[str]]:
    """
    Parse the market question to detect which asset and direction it covers.
    Returns (asset, direction) or (None, None) if not a lag-arb candidate.

    Examples:
      "Will BTC be higher in the next 15 minutes?" → ("BTC", "UP")
      "Will ETH price be lower at 3pm UTC?"        → ("ETH", "DOWN")
      "Will Bitcoin close above $95,000 today?"    → ("BTC", "UP")
    """
    q = question.lower()

    # Detect asset
    asset = None
    if any(k in q for k in ["btc", "bitcoin"]):
        asset = "BTC"
    elif any(k in q for k in ["eth", "ethereum", "ether"]):
        asset = "ETH"
    elif any(k in q for k in ["sol", "solana"]):
        asset = "SOL"
    else:
        return None, None

    # Detect direction
    direction = None
    up_words   = ["higher", "above", "up", "increase", "rise", "bull", "over"]
    down_words = ["lower", "below", "down", "decrease", "fall", "bear", "under"]

    if any(w in q for w in up_words):
        direction = "UP"
    elif any(w in q for w in down_words):
        direction = "DOWN"
    else:
        return asset, None   # can't determine direction

    return asset, direction


async def fetch_crypto_markets(session: aiohttp.ClientSession) -> List[PolyMarket]:
    """
    Fetch all active short-duration crypto markets from Polymarket.
    Returns a list of PolyMarket objects with live prices.
    """
    markets = []

    # Step 1: Get market list from Gamma API (has human-readable metadata)
    try:
        params = {
            "active":   "true",
            "closed":   "false",
            "tag_slug": "crypto",   # filter to crypto category
            "limit":    200,
        }
        async with session.get(
            f"{config.POLYMARKET_GAMMA_URL}/markets",
            params=params,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                log.warning("Gamma API returned %d", resp.status)
                return []
            gamma_data = await resp.json()
    except Exception as e:
        log.error("Failed to fetch Gamma markets: %s", e)
        return []

    # Gamma returns a list or {"data": [...], ...}
    if isinstance(gamma_data, dict):
        gamma_data = gamma_data.get("data", gamma_data.get("markets", []))

    now_utc = datetime.now(timezone.utc)

    for m in gamma_data:
        try:
            # Parse end date
            end_iso = m.get("endDate") or m.get("end_date_iso") or m.get("endDateIso", "")
            if not end_iso:
                continue
            # Handle both with and without timezone suffix
            if end_iso.endswith("Z"):
                end_iso = end_iso[:-1] + "+00:00"
            try:
                end_dt = datetime.fromisoformat(end_iso)
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            hours_left = (end_dt - now_utc).total_seconds() / 3600
            if hours_left <= 0 or hours_left > config.MAX_MARKET_HOURS_TO_CLOSE:
                continue

            question = m.get("question", "")
            asset, direction = _detect_asset_direction(question)
            if not asset or not direction:
                continue

            # Get tokens (YES / NO)
            tokens = m.get("tokens", m.get("clobTokenIds", []))
            if not tokens or len(tokens) < 2:
                continue

            # Tokens may be dicts {outcome, token_id} or plain strings
            if isinstance(tokens[0], dict):
                yes_token = next(
                    (t["token_id"] for t in tokens
                     if t.get("outcome", "").upper() == "YES"), None
                )
                no_token = next(
                    (t["token_id"] for t in tokens
                     if t.get("outcome", "").upper() == "NO"), None
                )
            else:
                yes_token, no_token = str(tokens[0]), str(tokens[1])

            if not yes_token or not no_token:
                continue

            condition_id = m.get("conditionId") or m.get("condition_id", "")

            # Step 2: Fetch live orderbook prices from CLOB
            yes_price, no_price, liquidity = await _fetch_book_price(
                session, yes_token, no_token
            )
            if yes_price is None:
                continue

            spread = abs((yes_price + no_price) - 1.0)

            poly = PolyMarket(
                condition_id    = condition_id,
                question        = question,
                asset           = asset,
                direction       = direction,
                yes_token_id    = yes_token,
                no_token_id     = no_token,
                yes_price       = yes_price,
                no_price        = no_price,
                spread          = spread,
                liquidity       = liquidity,
                end_date        = end_dt,
                hours_remaining = hours_left,
            )
            markets.append(poly)

        except Exception as e:
            log.debug("Skipping market due to parse error: %s", e)
            continue

    log.info("Found %d tradeable crypto markets on Polymarket", len(markets))
    return markets


async def _fetch_book_price(
    session: aiohttp.ClientSession,
    yes_token: str,
    no_token: str,
) -> tuple[Optional[float], Optional[float], float]:
    """
    Fetch best ask price for YES and NO tokens from the CLOB order book.
    Returns (yes_price, no_price, liquidity_usd) or (None, None, 0).
    """
    try:
        async with session.get(
            f"{config.POLYMARKET_CLOB_URL}/book",
            params={"token_id": yes_token},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status == 401:
                body = await resp.text()
                log.warning("CLOB /book returned 401 Unauthorized for token %s: %s",
                            yes_token[:12], body[:200])
                return None, None, 0
            if resp.status != 200:
                log.debug("CLOB /book returned %d for token %s", resp.status, yes_token[:12])
                return None, None, 0
            book = await resp.json()

        # Best ask = lowest price someone will sell YES at
        asks = book.get("asks", [])
        if not asks:
            return None, None, 0

        # Sort asks ascending by price
        sorted_asks = sorted(asks, key=lambda x: float(x.get("price", 1)))
        yes_ask = float(sorted_asks[0]["price"])

        # Total liquidity = sum of all bid sizes (in USD)
        bids = book.get("bids", [])
        liquidity = sum(
            float(b.get("size", 0)) * float(b.get("price", 0))
            for b in bids
        )

        # Implied NO price = 1 - YES price (approximate, good enough for scanning)
        no_price = round(1.0 - yes_ask, 4)

        return round(yes_ask, 4), no_price, round(liquidity, 2)

    except Exception as e:
        log.debug("CLOB book fetch error for token %s: %s", yes_token[:8], e)
        return None, None, 0


async def get_market_snapshot(session: aiohttp.ClientSession,
                               condition_id: str,
                               yes_token_id: str) -> Optional[tuple[float, float]]:
    """
    Refresh the YES/NO price for a specific open position.
    Returns (yes_price, no_price) or None on error.
    """
    try:
        async with session.get(
            f"{config.POLYMARKET_CLOB_URL}/book",
            params={"token_id": yes_token_id},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status == 401:
                body = await resp.text()
                log.warning("CLOB /book returned 401 Unauthorized for condition %s: %s",
                            condition_id[:12], body[:200])
                return None
            if resp.status != 200:
                log.debug("CLOB /book returned %d for condition %s", resp.status, condition_id[:12])
                return None
            book = await resp.json()
        asks = sorted(book.get("asks", []), key=lambda x: float(x.get("price", 1)))
        if not asks:
            return None
        yes_price = float(asks[0]["price"])
        return round(yes_price, 4), round(1 - yes_price, 4)
    except Exception:
        return None
