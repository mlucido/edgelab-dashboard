"""
Behavioral Bias Scanner — EdgeLab Sprint 1, Strategy 1

Identifies Kalshi markets where the current YES ask price diverges from the
category's historical resolution rate, indicating a potential behavioral bias
(overconfidence, anchoring, etc.) that can be exploited.

Auth: RSA key-pair signing copied from strategies/edgelab/src/execution/kalshi_executor.py
"""
from __future__ import annotations

import asyncio
import base64
import logging
import math
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
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
from strategies.behavioral_bias.config import (
    MAX_YES_PRICE,
    MIN_EDGE,
    MIN_VOLUME_24H,
    MIN_YES_PRICE,
    SCAN_INTERVAL,
)

# ── Constants ─────────────────────────────────────────────────────────────────

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "")
STRATEGY_MODE = os.getenv("STRATEGY_MODE", "paper").lower()

DB_PATH = Path(__file__).resolve().parent / "resolution_history.db"

logger = logging.getLogger("edgelab.behavioral_bias")

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
        CREATE TABLE IF NOT EXISTS resolutions (
            market_id    TEXT NOT NULL,
            category     TEXT,
            resolved_yes INTEGER NOT NULL,  -- 1 = YES, 0 = NO
            resolved_at  TEXT NOT NULL,
            final_yes_price REAL,
            PRIMARY KEY (market_id)
        )
        """
    )
    await db.commit()


async def _get_historical_yes_rate(category: str) -> float | None:
    """Return the fraction of resolved YES outcomes for a given category."""
    if not category:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        cursor = await db.execute(
            """
            SELECT COUNT(*), SUM(resolved_yes)
            FROM resolutions
            WHERE category = ?
            """,
            (category,),
        )
        row = await cursor.fetchone()
    if not row or row[0] == 0:
        return None
    total, yes_count = row
    return (yes_count or 0) / total


async def _log_fill(
    market_id: str,
    category: str,
    fill_price: float,
    paper: bool,
) -> None:
    """Log an executed fill into resolution_history.db (pre-resolution placeholder)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        # We record a fill as a pending resolution; resolved_yes = -1 sentinel
        await db.execute(
            """
            INSERT OR IGNORE INTO resolutions
                (market_id, category, resolved_yes, resolved_at, final_yes_price)
            VALUES (?, ?, -1, ?, ?)
            """,
            (
                market_id,
                category,
                datetime.now(timezone.utc).isoformat(),
                fill_price,
            ),
        )
        await db.commit()
    mode_tag = "PAPER" if paper else "LIVE"
    logger.info(
        "[%s] Fill logged market_id=%s category=%s fill_price=%.4f",
        mode_tag, market_id, category, fill_price,
    )


# ── Field parsing helpers (Kalshi v2 API) ─────────────────────────────────────

def _parse_dollar_field(m: dict, *keys: str) -> float:
    """Extract a dollar-denominated price field, trying keys in order."""
    for k in keys:
        v = m.get(k)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return 0.0


def _parse_float_field(m: dict, *keys: str) -> float:
    """Extract a numeric field from string-encoded float, trying keys in order."""
    for k in keys:
        v = m.get(k)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return 0.0


def _extract_category(m: dict) -> str:
    """Derive category from event_ticker prefix (e.g. KXNBAGAME -> NBA)."""
    cat = m.get("category", "")
    if cat:
        return cat
    event = m.get("event_ticker", "")
    # Kalshi event tickers often encode category: KX<CAT>..., strip KX prefix
    if event.startswith("KX"):
        # e.g. KXNBAGAME-... -> NBAGAME, KXCRYPTO-... -> CRYPTO
        rest = event[2:]
        dash = rest.find("-")
        return rest[:dash] if dash > 0 else rest
    return ""


# ── Kalshi API helpers ────────────────────────────────────────────────────────

async def _fetch_open_markets(client: httpx.AsyncClient) -> list[dict]:
    path = "/trade-api/v2/markets"
    params = {"limit": 200, "status": "open"}
    try:
        resp = await client.get(
            f"{KALSHI_BASE_URL}/markets",
            params=params,
            headers=_kalshi_headers("GET", path),
            timeout=15.0,
        )
        if resp.status_code == 429:
            logger.warning("Rate-limited fetching open markets; backing off 10s")
            await asyncio.sleep(10)
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("markets", [])
    except httpx.HTTPStatusError as exc:
        logger.error("HTTP error fetching open markets: %s", exc)
        return []
    except httpx.RequestError as exc:
        logger.error("Network error fetching open markets: %s", exc)
        return []


async def _fetch_settled_markets(client: httpx.AsyncClient) -> list[dict]:
    """Fetch markets settled in the last 30 days for backfill."""
    path = "/trade-api/v2/markets"
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())
    params = {"limit": 200, "status": "settled", "min_close_ts": cutoff}
    try:
        resp = await client.get(
            f"{KALSHI_BASE_URL}/markets",
            params=params,
            headers=_kalshi_headers("GET", path),
            timeout=20.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("markets", [])
    except Exception as exc:
        logger.error("Error fetching settled markets for backfill: %s", exc)
        return []


# ── Strategy ──────────────────────────────────────────────────────────────────

class BehavioralBiasStrategy(BaseStrategy):
    """
    Scans Kalshi open markets for behavioral bias opportunities.

    Logic: if category's historical YES resolution rate >> current YES ask
    price, the market may be systematically underpricing YES, creating an edge.
    """

    def __init__(self):
        super().__init__(
            strategy_name="behavioral_bias",
            platform="kalshi",
            interval_seconds=SCAN_INTERVAL,
        )
        self._backfill_done = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def _ensure_db(self) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await _init_db(db)

    async def _backfill(self) -> None:
        """One-time backfill of settled markets into resolution_history.db."""
        self.logger.info("Starting resolution history backfill (last 30 days)…")
        async with httpx.AsyncClient() as client:
            markets = await _fetch_settled_markets(client)

        if not markets:
            self.logger.warning("Backfill: no settled markets returned")
            return

        rows = []
        for m in markets:
            market_id = m.get("ticker") or m.get("market_id", "")
            category = _extract_category(m)
            result = m.get("result", "")
            resolved_yes = 1 if result and result.upper() == "YES" else 0
            resolved_at = m.get("close_time") or m.get("expiration_time", "")
            final_yes_price = _parse_dollar_field(m, "last_price_dollars", "yes_bid_dollars")
            if not market_id:
                continue
            rows.append((market_id, category, resolved_yes, resolved_at, final_yes_price))

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.executemany(
                """
                INSERT OR IGNORE INTO resolutions
                    (market_id, category, resolved_yes, resolved_at, final_yes_price)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
            await db.commit()

        self.logger.info("Backfill complete: %d markets ingested", len(rows))
        self._backfill_done = True

    # ── scan() ────────────────────────────────────────────────────────────────

    async def scan(self) -> list[dict]:
        await self._ensure_db()

        if not self._backfill_done:
            await self._backfill()

        async with httpx.AsyncClient() as client:
            markets = await _fetch_open_markets(client)

        opps: list[dict] = []
        price_rejected = 0
        volume_rejected = 0
        for m in markets:
            ticker = m.get("ticker") or m.get("market_id", "")
            if not ticker:
                continue

            yes_ask = _parse_dollar_field(m, "yes_ask_dollars", "last_price_dollars")
            category = _extract_category(m)
            close_time = m.get("close_time") or m.get("expiration_time", "")
            volume_24h = _parse_float_field(m, "volume_24h_fp", "volume_fp")

            # Filter
            if not (MIN_YES_PRICE < yes_ask < MAX_YES_PRICE):
                price_rejected += 1
                continue
            if volume_24h < MIN_VOLUME_24H:
                volume_rejected += 1
                continue

            historical_yes_rate = await _get_historical_yes_rate(category)

            opps.append(
                {
                    "market_id": ticker,
                    "ticker": ticker,
                    "yes_ask": yes_ask,
                    "category": category,
                    "close_time": close_time,
                    "volume_24h": volume_24h,
                    "historical_yes_rate": historical_yes_rate,
                    # Expose price key for BaseStrategy sizing
                    "price": yes_ask,
                }
            )

        self.logger.info(
            "scan() complete: %d/%d markets passed filters (price_rejected=%d, volume_rejected=%d)",
            len(opps), len(markets), price_rejected, volume_rejected,
        )
        return opps

    # ── score() ───────────────────────────────────────────────────────────────

    async def score(self, opp: dict) -> float:
        historical_yes_rate = opp.get("historical_yes_rate")
        yes_ask = opp["yes_ask"]

        if historical_yes_rate is None:
            return 0.0

        edge = historical_yes_rate - yes_ask
        if edge < MIN_EDGE:
            return 0.0

        return min(edge / yes_ask, 0.5)

    # ── execute() ─────────────────────────────────────────────────────────────

    async def execute(self, opp: dict, size: float) -> dict:
        market_id = opp["market_id"]
        yes_ask = opp["yes_ask"]
        category = opp.get("category", "")
        trade_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc).isoformat()

        if self.mode == "paper":
            # Mock fill at yes_ask
            await _log_fill(market_id, category, yes_ask, paper=True)
            return {
                "trade_id": trade_id,
                "market_id": market_id,
                "entry_price": yes_ask,
                "size": size,
                "paper": True,
                "timestamp": ts,
            }

        # Live mode: POST /trade-api/v2/orders
        if not KALSHI_API_KEY:
            self.logger.error("KALSHI_API_KEY not set — cannot place live order")
            raise EnvironmentError("KALSHI_API_KEY not set")

        yes_ask_cents = int(round(yes_ask * 100))
        size_cents = int(round(size * 100))
        contracts = max(1, math.floor(size_cents / yes_ask_cents))

        order_payload = {
            "ticker": market_id,
            "action": "buy",
            "side": "yes",
            "type": "limit",
            "count": contracts,
            "yes_price": yes_ask_cents,
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
            self.logger.error("Network error placing order for %s: %s", market_id, exc)
            raise

        if resp.status_code == 429:
            self.logger.warning("Rate-limited placing order for %s", market_id)
            raise RuntimeError(f"Rate-limited by Kalshi for market {market_id}")

        if resp.status_code not in (200, 201):
            self.logger.error(
                "Kalshi API error %d for %s: %s",
                resp.status_code, market_id, resp.text[:200],
            )
            raise RuntimeError(f"Kalshi API {resp.status_code}: {resp.text[:200]}")

        order_data = resp.json()
        order = order_data.get("order", order_data)
        order_id = order.get("order_id") or order.get("id", trade_id)

        await _log_fill(market_id, category, yes_ask, paper=False)

        self.logger.info(
            "[LIVE] Order placed market_id=%s order_id=%s contracts=%d yes_price=%d¢",
            market_id, order_id, contracts, yes_ask_cents,
        )

        return {
            "trade_id": order_id,
            "market_id": market_id,
            "entry_price": yes_ask,
            "size": size,
            "contracts": contracts,
            "paper": False,
            "timestamp": ts,
        }

    # ── Resolution polling ────────────────────────────────────────────────────

    async def _poll_resolutions(self) -> None:
        """
        Poll Kalshi for settled markets and update resolution_history.db.
        Called periodically after markets close.
        """
        self.logger.debug("Polling for settled markets to record outcomes…")
        async with httpx.AsyncClient() as client:
            markets = await _fetch_settled_markets(client)

        if not markets:
            return

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            for m in markets:
                market_id = m.get("ticker") or m.get("market_id", "")
                if not market_id:
                    continue
                result = m.get("result", "")
                resolved_yes = 1 if result and result.upper() == "YES" else 0
                category = m.get("category", "")
                resolved_at = m.get("close_time") or m.get("expiration_time", "")
                final_yes_price = m.get("last_price") or m.get("yes_bid") or 0.0

                # Update any pending (-1) or insert fresh
                await db.execute(
                    """
                    INSERT INTO resolutions
                        (market_id, category, resolved_yes, resolved_at, final_yes_price)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(market_id) DO UPDATE SET
                        resolved_yes = excluded.resolved_yes,
                        resolved_at  = excluded.resolved_at,
                        final_yes_price = excluded.final_yes_price
                    """,
                    (market_id, category, resolved_yes, resolved_at, final_yes_price),
                )
            await db.commit()

        self.logger.info("Resolution poll complete: %d markets updated", len(markets))


# ── CLI entrypoint ────────────────────────────────────────────────────────────

async def main() -> None:
    strategy = BehavioralBiasStrategy()
    await strategy.run()


if __name__ == "__main__":
    asyncio.run(main())
