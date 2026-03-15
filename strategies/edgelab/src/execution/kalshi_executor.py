"""
EdgeLab Kalshi Executor — Places orders on Kalshi exchange.

Auth: RSA key-pair signing (same as strategies/bot/).
"""
from __future__ import annotations

import base64
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "")
KALSHI_MAX_TRADE_CENTS = int(os.getenv("KALSHI_MAX_TRADE_CENTS", "1000"))  # default $10
TRADING_MODE = os.getenv("TRADING_MODE", "paper").lower()


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


def _kalshi_headers(method: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    msg = ts + method.upper() + path.split("?")[0]
    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY,
        "KALSHI-ACCESS-SIGNATURE": _sign(msg),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class KalshiTradingModeError(Exception):
    """Raised when a live order is attempted while TRADING_MODE != live."""


class KalshiInsufficientBalanceError(Exception):
    """Raised when Kalshi balance is too low to cover the order."""


class KalshiSizeLimitError(Exception):
    """Raised when requested order size exceeds KALSHI_MAX_TRADE_CENTS."""


# ---------------------------------------------------------------------------
# Orderbook — real best-ask fetch
# ---------------------------------------------------------------------------

async def _fetch_best_ask(market_ticker: str, side: str) -> int | None:
    """
    Fetch the best ask price (in cents) from the Kalshi orderbook.

    Returns None if orderbook is empty or returns a 0.50/0.50 placeholder.
    Follows the same guard pattern as strategies/bot/live_engine.py.
    """
    if not KALSHI_API_KEY:
        return None

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            path = f"/trade-api/v2/markets/{market_ticker}/orderbook"
            resp = await client.get(
                f"{KALSHI_BASE_URL}/markets/{market_ticker}/orderbook",
                headers=_kalshi_headers("GET", path),
            )
            if resp.status_code != 200:
                logger.warning("Orderbook fetch failed for %s: HTTP %d", market_ticker, resp.status_code)
                return None

            data = resp.json()
            book = data.get("orderbook", data)

            # Pick the relevant side
            if side.lower() == "yes":
                asks = book.get("yes", [])
            else:
                asks = book.get("no", [])

            if not asks:
                return None

            # Asks are [[price_cents, quantity], ...] — take lowest price
            sorted_asks = sorted(asks, key=lambda x: int(x[0]))
            best_price = int(sorted_asks[0][0])

            # Guard: reject 50¢ placeholder (empty orderbook signal)
            if best_price == 50:
                # Double-check: if all asks are at 50, it's a placeholder
                if all(int(a[0]) == 50 for a in sorted_asks):
                    logger.warning(
                        "Orderbook placeholder detected for %s (all asks at 50¢)",
                        market_ticker,
                    )
                    return None

            return max(1, min(99, best_price))

    except Exception as exc:
        logger.warning("Orderbook fetch error for %s: %s", market_ticker, exc)
        return None


# ---------------------------------------------------------------------------
# Balance check
# ---------------------------------------------------------------------------

async def get_kalshi_balance() -> int:
    """
    Fetch available balance from Kalshi portfolio.
    Returns balance in cents.
    Raises httpx.HTTPStatusError on API errors.
    """
    if not KALSHI_API_KEY:
        raise EnvironmentError("KALSHI_API_KEY not set in environment")

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"{KALSHI_BASE_URL}/portfolio/balance",
            headers=_kalshi_headers("GET", "/trade-api/v2/portfolio/balance"),
        )
        resp.raise_for_status()
        data = resp.json()

    # Kalshi returns balance in cents under "balance" key
    balance_cents = int(data.get("balance", 0))
    logger.debug("Kalshi balance: %d cents ($%.2f)", balance_cents, balance_cents / 100)
    return balance_cents


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

async def place_kalshi_order(
    market_ticker: str,
    side: str,
    size_cents: int,
) -> dict[str, Any]:
    """
    Place a limit order on Kalshi.

    Args:
        market_ticker: Kalshi market ticker (e.g. "PRES-2028-DEM")
        side: "yes" or "no"
        size_cents: Order size in cents (e.g. 1000 = $10)

    Safety gates (enforced before any network call):
        1. TRADING_MODE must be "live" — raises KalshiTradingModeError otherwise
        2. size_cents must be <= KALSHI_MAX_TRADE_CENTS — raises KalshiSizeLimitError
        3. Available balance must cover size_cents — raises KalshiInsufficientBalanceError

    Returns a dict with keys:
        success: bool
        order_id: str (on success)
        error: str (on failure)
        market_ticker, side, size_cents, timestamp
    """
    ts = datetime.now(timezone.utc).isoformat()

    # --- Gate 1: mode check ---
    mode = os.getenv("TRADING_MODE", "paper").lower()
    if mode != "live":
        raise KalshiTradingModeError(
            f"place_kalshi_order() called but TRADING_MODE={mode!r}. "
            "Set TRADING_MODE=live to enable real trading."
        )

    # --- Gate 2: size cap ---
    cap = int(os.getenv("KALSHI_MAX_TRADE_CENTS", str(KALSHI_MAX_TRADE_CENTS)))
    if size_cents > cap:
        raise KalshiSizeLimitError(
            f"Requested size {size_cents} cents exceeds KALSHI_MAX_TRADE_CENTS={cap}. "
            "Increase limit or reduce trade size."
        )

    # --- Gate 3: balance check ---
    try:
        balance = await get_kalshi_balance()
    except Exception as exc:
        logger.error("Failed to fetch Kalshi balance: %s", exc)
        return {
            "success": False,
            "error": f"Balance check failed: {exc}",
            "market_ticker": market_ticker,
            "side": side,
            "size_cents": size_cents,
            "timestamp": ts,
        }

    if balance < size_cents:
        raise KalshiInsufficientBalanceError(
            f"Insufficient Kalshi balance: have {balance} cents, need {size_cents} cents "
            f"for market {market_ticker}"
        )

    # --- Fetch real best-ask price from Kalshi orderbook ---
    best_ask_cents = await _fetch_best_ask(market_ticker, side)
    if best_ask_cents is None:
        logger.warning(
            "ORDER DEFERRED: empty orderbook for %s side=%s — no best ask available",
            market_ticker, side,
        )
        return {
            "success": False,
            "error": f"Orderbook empty for {market_ticker} — order deferred",
            "market_ticker": market_ticker,
            "side": side,
            "size_cents": size_cents,
            "timestamp": ts,
        }

    # Contracts = size in cents / best-ask price in cents
    contracts = max(1, size_cents // best_ask_cents)

    order_payload: dict[str, Any] = {
        "ticker": market_ticker,
        "action": "buy",
        "side": side.lower(),
        "type": "limit",
        "count": contracts,
    }

    if side.lower() == "yes":
        order_payload["yes_price"] = best_ask_cents
    else:
        order_payload["no_price"] = best_ask_cents

    # --- Place order ---
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{KALSHI_BASE_URL}/portfolio/orders",
                json=order_payload,
                headers=_kalshi_headers("POST", "/trade-api/v2/portfolio/orders"),
            )
    except httpx.RequestError as exc:
        logger.error("Kalshi API request failed for market %s: %s", market_ticker, exc)
        return {
            "success": False,
            "error": f"Network error: {exc}",
            "market_ticker": market_ticker,
            "side": side,
            "size_cents": size_cents,
            "timestamp": ts,
        }

    if resp.status_code not in (200, 201):
        logger.error(
            "Kalshi API error %d for market %s: %s",
            resp.status_code, market_ticker, resp.text[:200],
        )
        return {
            "success": False,
            "error": f"Kalshi API {resp.status_code}: {resp.text[:200]}",
            "market_ticker": market_ticker,
            "side": side,
            "size_cents": size_cents,
            "timestamp": ts,
        }

    order_data = resp.json()
    order = order_data.get("order", order_data)
    order_id = order.get("order_id") or order.get("id", "")

    logger.info(
        "[LIVE-KALSHI] Order placed: ticker=%s side=%s size=%d¢ contracts=%d order_id=%s",
        market_ticker, side, size_cents, contracts, order_id,
    )

    # --- Log to SQLite ---
    await _log_kalshi_trade(
        market_ticker=market_ticker,
        side=side,
        size_cents=size_cents,
        contracts=contracts,
        order_id=order_id,
        timestamp=ts,
    )

    return {
        "success": True,
        "order_id": order_id,
        "market_ticker": market_ticker,
        "side": side,
        "size_cents": size_cents,
        "contracts": contracts,
        "timestamp": ts,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _log_kalshi_trade(
    market_ticker: str,
    side: str,
    size_cents: int,
    contracts: int,
    order_id: str,
    timestamp: str,
) -> None:
    """Persist a Kalshi live trade to the trades_live SQLite table."""
    import aiosqlite
    from src.calibration.tracker import DB_PATH

    size_usdc = size_cents / 100.0
    strategy = f"kalshi:{side}"

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO trades_live
                (market_id, entry_price, entry_prob, strategy, size, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
            """,
            (
                market_ticker,
                0.0,         # entry_price filled at execution; unknown here
                0.0,         # entry_prob unknown without order book data
                strategy,
                size_usdc,
                timestamp,
            ),
        )
        await db.commit()

    logger.debug(
        "Logged Kalshi trade to trades_live: market=%s order_id=%s",
        market_ticker, order_id,
    )
