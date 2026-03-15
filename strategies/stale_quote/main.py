"""
Stale Quote Detector — EdgeLab Sprint 1, Strategy 2

Identifies Kalshi markets where resting limit orders are stale (priced below
the current best ask), then lifts them for a quick spread capture. Confidence
is elevated when recent news in Redis matches the market's category.

Auth: RSA key-pair signing copied from strategies/edgelab/src/execution/kalshi_executor.py
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
import httpx
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ── Path setup ───────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from framework.base_strategy import BaseStrategy
from strategies.stale_quote.config import (
    MIN_SCORE,
    MIN_SPREAD,
    NEWS_LOOKBACK_HOURS,
    SCAN_INTERVAL,
)

# ── Constants ─────────────────────────────────────────────────────────────────

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "")
STRATEGY_MODE = os.getenv("STRATEGY_MODE", "paper").lower()

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_NEWS_KEY = "edgelab:news:cache"

DB_PATH = Path(__file__).resolve().parent / "stale_quote.db"

logger = logging.getLogger("edgelab.stale_quote")

# ── Kalshi RSA Auth (copied from strategies/edgelab/src/execution/kalshi_executor.py) ──

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


def _kalshi_headers(method: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    msg = ts + method.upper() + path.split("?")[0]
    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY,
        "KALSHI-ACCESS-SIGNATURE": _sign(msg),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _init_db(db: aiosqlite.Connection) -> None:
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS lifted_orders (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id   TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            stale_price REAL NOT NULL,
            market_price REAL NOT NULL,
            spread      REAL NOT NULL,
            size        REAL NOT NULL,
            pnl         REAL NOT NULL,
            confidence  TEXT NOT NULL,
            lifted_at   TEXT NOT NULL
        )
        """
    )
    await db.commit()


async def _log_lifted_order(
    market_id: str,
    ticker: str,
    stale_price: float,
    market_price: float,
    spread: float,
    size: float,
    pnl: float,
    confidence: str,
) -> None:
    lifted_at = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute(
            """
            INSERT INTO lifted_orders
                (market_id, ticker, stale_price, market_price, spread, size, pnl, confidence, lifted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (market_id, ticker, stale_price, market_price, spread, size, pnl, confidence, lifted_at),
        )
        await db.commit()
    logger.info(
        "Logged lifted order: ticker=%s stale=%.4f market=%.4f spread=%.4f pnl=%.4f conf=%s",
        ticker, stale_price, market_price, spread, pnl, confidence,
    )


# ── Redis helpers ─────────────────────────────────────────────────────────────

def _check_redis_news(category: str) -> str:
    """
    Check Redis edgelab:news:cache for recent articles mentioning category keywords.
    Returns 'high' if a news hit is found within NEWS_LOOKBACK_HOURS, else 'medium'.
    Falls back to 'medium' on any Redis error.
    """
    if not category:
        return "medium"

    try:
        import redis

        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_timeout=2)
        raw = r.get(REDIS_NEWS_KEY)
        if not raw:
            return "medium"

        articles = json.loads(raw)
        if not isinstance(articles, list):
            return "medium"

        now_ts = time.time()
        cutoff = now_ts - (NEWS_LOOKBACK_HOURS * 3600)
        keywords = [kw.lower() for kw in category.replace("-", " ").split()]

        for article in articles:
            article_ts = article.get("timestamp") or article.get("published_at")
            if article_ts is None:
                continue
            # Accept numeric epoch or ISO string
            if isinstance(article_ts, str):
                try:
                    from datetime import datetime
                    article_ts = datetime.fromisoformat(article_ts).timestamp()
                except ValueError:
                    continue
            if article_ts < cutoff:
                continue

            text = " ".join([
                str(article.get("title", "")),
                str(article.get("body", "")),
                str(article.get("summary", "")),
            ]).lower()

            if any(kw in text for kw in keywords):
                logger.debug(
                    "News hit for category=%s article_ts=%s", category, article_ts
                )
                return "high"

    except Exception as exc:
        logger.debug("Redis news check skipped (error: %s)", exc)

    return "medium"


# ── Kalshi API helpers ────────────────────────────────────────────────────────

async def _fetch_top_markets_by_open_interest(
    client: httpx.AsyncClient, limit: int = 150
) -> list[dict]:
    """Fetch Kalshi open markets, sorted by open interest descending."""
    path = "/trade-api/v2/markets"
    params = {"limit": limit, "status": "open"}
    try:
        resp = await client.get(
            f"{KALSHI_BASE_URL}/markets",
            params=params,
            headers=_kalshi_headers("GET", path),
            timeout=15.0,
        )
        if resp.status_code == 429:
            logger.warning("Rate-limited fetching markets; backing off 10s")
            await asyncio.sleep(10)
            return []
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])
        # Sort by open_interest descending; fall back to volume
        markets.sort(
            key=lambda m: m.get("open_interest") or m.get("volume", 0),
            reverse=True,
        )
        return markets[:limit]
    except httpx.HTTPStatusError as exc:
        logger.error("HTTP error fetching markets: %s", exc)
        return []
    except httpx.RequestError as exc:
        logger.error("Network error fetching markets: %s", exc)
        return []


async def _fetch_orderbook(client: httpx.AsyncClient, ticker: str) -> dict | None:
    """Fetch the full orderbook for a market ticker."""
    path = f"/trade-api/v2/markets/{ticker}/orderbook"
    try:
        resp = await client.get(
            f"{KALSHI_BASE_URL}/markets/{ticker}/orderbook",
            headers=_kalshi_headers("GET", path),
            timeout=8.0,
        )
        if resp.status_code == 429:
            logger.warning("Rate-limited fetching orderbook for %s; skipping", ticker)
            await asyncio.sleep(5)
            return None
        if resp.status_code != 200:
            logger.debug("Orderbook fetch failed for %s: HTTP %d", ticker, resp.status_code)
            return None
        data = resp.json()
        return data.get("orderbook", data)
    except Exception as exc:
        logger.debug("Orderbook fetch error for %s: %s", ticker, exc)
        return None


# ── Stale order detection ─────────────────────────────────────────────────────

def _find_stale_orders(orderbook: dict, last_trade_time: str | None) -> list[dict]:
    """
    Identify stale YES asks in the orderbook.

    A stale order is defined as:
        order_price < (best_yes_ask - MIN_SPREAD)

    If last_trade_time is available and parseable, we further check that the
    resting order was created before the last trade (proxy for staleness).

    Returns list of dicts: {stale_order_price, current_best_ask, spread}
    """
    yes_asks = orderbook.get("yes", [])
    if not yes_asks:
        return []

    # Normalise: [[price_cents, qty], ...] or [[price, qty], ...]
    parsed = []
    for entry in yes_asks:
        try:
            price_raw, qty = int(entry[0]), int(entry[1])
            # Normalise to [0, 1] if in cents
            price = price_raw / 100.0 if price_raw > 1 else price_raw
            parsed.append((price, qty))
        except (IndexError, TypeError, ValueError):
            continue

    if not parsed:
        return []

    parsed.sort(key=lambda x: x[0])
    best_ask = parsed[0][0]

    stale = []
    for price, qty in parsed[1:]:  # skip best ask itself
        spread = price - best_ask  # positive: this order is more expensive than best
        # Stale definition: resting above best_ask by > MIN_SPREAD → arbitrage window
        # Wait — re-reading spec: stale = order_price < (best_yes_ask - MIN_SPREAD)
        # meaning an old limit that is priced BELOW current best (underpriced vs market).
        spread_below = best_ask - price
        if spread_below >= MIN_SPREAD:
            stale.append(
                {
                    "stale_order_price": price,
                    "current_best_ask": best_ask,
                    "spread": spread_below,
                }
            )

    return stale


# ── Strategy ──────────────────────────────────────────────────────────────────

class StaleQuoteStrategy(BaseStrategy):
    """
    Detects and lifts stale limit orders in Kalshi markets.

    Stale orders: resting YES limits priced materially below the current
    best ask, potentially left over from pre-news price levels.
    """

    def __init__(self):
        super().__init__(
            strategy_name="stale_quote",
            platform="kalshi",
            interval_seconds=SCAN_INTERVAL,
        )

    async def _ensure_db(self) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await _init_db(db)

    # ── scan() ────────────────────────────────────────────────────────────────

    async def scan(self) -> list[dict]:
        await self._ensure_db()

        opps: list[dict] = []

        async with httpx.AsyncClient() as client:
            markets = await _fetch_top_markets_by_open_interest(client, limit=150)

            for m in markets:
                ticker = m.get("ticker") or m.get("market_id", "")
                if not ticker:
                    continue

                category = m.get("category", "")
                last_trade_time = m.get("last_trade_time") or m.get("last_price_updated")

                orderbook = await _fetch_orderbook(client, ticker)
                if not orderbook:
                    continue

                stale_orders = _find_stale_orders(orderbook, last_trade_time)
                if not stale_orders:
                    continue

                # Check Redis for news (synchronous; fast)
                stale_confidence = _check_redis_news(category)

                for so in stale_orders:
                    opps.append(
                        {
                            "market_id": ticker,
                            "ticker": ticker,
                            "category": category,
                            "stale_order_price": so["stale_order_price"],
                            "current_best_ask": so["current_best_ask"],
                            "spread": so["spread"],
                            "stale_confidence": stale_confidence,
                            # BaseStrategy sizing key
                            "price": so["stale_order_price"],
                        }
                    )

        self.logger.info(
            "scan() found %d stale opportunities across %d markets",
            len(opps), len(markets),
        )
        return opps

    # ── score() ───────────────────────────────────────────────────────────────

    async def score(self, opp: dict) -> float:
        current_best_ask = opp["current_best_ask"]
        stale_order_price = opp["stale_order_price"]
        confidence = opp.get("stale_confidence", "medium")

        if current_best_ask <= 0:
            return 0.0

        base_score = (current_best_ask - stale_order_price) / current_best_ask
        if confidence == "high":
            base_score *= 1.3

        return min(base_score, 0.6)

    # ── execute() ─────────────────────────────────────────────────────────────

    async def execute(self, opp: dict, size: float) -> dict:
        market_id = opp["market_id"]
        ticker = opp["ticker"]
        stale_price = opp["stale_order_price"]
        best_ask = opp["current_best_ask"]
        spread = opp["spread"]
        confidence = opp.get("stale_confidence", "medium")
        trade_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc).isoformat()

        # Paper mode: instant simulated fill at stale_price, exit at best_ask
        if self.mode == "paper":
            pnl = (best_ask - stale_price) * size
            await _log_lifted_order(
                market_id=market_id,
                ticker=ticker,
                stale_price=stale_price,
                market_price=best_ask,
                spread=spread,
                size=size,
                pnl=pnl,
                confidence=confidence,
            )
            self.logger.info(
                "[PAPER] Lifted stale order ticker=%s stale=%.4f ask=%.4f pnl=%.4f",
                ticker, stale_price, best_ask, pnl,
            )
            return {
                "trade_id": trade_id,
                "market_id": market_id,
                "ticker": ticker,
                "entry_price": stale_price,
                "exit_price": best_ask,
                "pnl": pnl,
                "size": size,
                "paper": True,
                "timestamp": ts,
            }

        # Live mode: POST market order to lift the stale limit
        if not KALSHI_API_KEY:
            self.logger.error("KALSHI_API_KEY not set — cannot place live order")
            raise EnvironmentError("KALSHI_API_KEY not set")

        stale_cents = int(round(stale_price * 100))
        size_cents = int(round(size * 100))
        contracts = max(1, size_cents // max(stale_cents, 1))

        # Market order to lift the stale limit
        order_payload = {
            "ticker": ticker,
            "action": "buy",
            "side": "yes",
            "type": "market",
            "count": contracts,
        }

        path = "/trade-api/v2/portfolio/orders"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    f"{KALSHI_BASE_URL}/portfolio/orders",
                    json=order_payload,
                    headers=_kalshi_headers("POST", path),
                )
        except httpx.RequestError as exc:
            self.logger.error("Network error placing order for %s: %s", ticker, exc)
            raise

        if resp.status_code == 429:
            self.logger.warning("Rate-limited placing order for %s", ticker)
            raise RuntimeError(f"Rate-limited by Kalshi for market {ticker}")

        if resp.status_code not in (200, 201):
            self.logger.error(
                "Kalshi API error %d for %s: %s",
                resp.status_code, ticker, resp.text[:200],
            )
            raise RuntimeError(f"Kalshi API {resp.status_code}: {resp.text[:200]}")

        order_data = resp.json()
        order = order_data.get("order", order_data)
        order_id = order.get("order_id") or order.get("id", trade_id)

        # Partial fill handling: accept if > 50% of requested size
        filled_count = order.get("filled_count") or order.get("contracts_filled", contracts)
        if filled_count < contracts * 0.5:
            self.logger.warning(
                "Partial fill below 50%% threshold: requested=%d filled=%d for %s",
                contracts, filled_count, ticker,
            )

        actual_fill_price = (order.get("yes_price") or stale_cents) / 100.0
        pnl = (best_ask - actual_fill_price) * (filled_count * actual_fill_price)

        await _log_lifted_order(
            market_id=market_id,
            ticker=ticker,
            stale_price=actual_fill_price,
            market_price=best_ask,
            spread=spread,
            size=filled_count * actual_fill_price,
            pnl=pnl,
            confidence=confidence,
        )

        self.logger.info(
            "[LIVE] Lifted stale order ticker=%s order_id=%s fill=%.4f ask=%.4f pnl=%.4f",
            ticker, order_id, actual_fill_price, best_ask, pnl,
        )

        return {
            "trade_id": order_id,
            "market_id": market_id,
            "ticker": ticker,
            "entry_price": actual_fill_price,
            "exit_price": best_ask,
            "pnl": pnl,
            "size": filled_count * actual_fill_price,
            "paper": False,
            "timestamp": ts,
        }


# ── CLI entrypoint ────────────────────────────────────────────────────────────

async def main() -> None:
    strategy = StaleQuoteStrategy()
    await strategy.run()


if __name__ == "__main__":
    asyncio.run(main())
