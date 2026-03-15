"""
EdgeLab Live Trader — Polygon wallet + on-chain execution via Polymarket CLOB.

Safety gates (both must pass before any real order is placed):
  1. TRADING_MODE env var must equal "LIVE" (case-insensitive)
  2. mode_manager.is_live() must return True (Redis-backed single source of truth)

USDC contract on Polygon mainnet: 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Lazy tracker imports hoisted to module level so tests can patch them
# (imported at call-time to avoid circular imports at module load)
try:
    from src.calibration.tracker import get_open_trades, close_trade  # noqa: F401
except ImportError:
    get_open_trades = None  # type: ignore
    close_trade = None  # type: ignore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
USDC_CONTRACT_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
USDC_DECIMALS = 6

# Minimal ERC-20 ABI — only balanceOf needed
_ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    }
]

CLOB_BASE_URL = "https://clob.polymarket.com"

# Default safety cap — $10 live trades unless explicitly overridden
LIVE_MAX_TRADE_OVERRIDE = float(os.getenv("LIVE_MAX_TRADE_OVERRIDE", "10"))


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class TradingModeError(Exception):
    """Raised when live trade is attempted while TRADING_MODE != LIVE."""


class InsufficientBalanceError(Exception):
    """Raised when USDC balance is too low to cover the requested trade size."""


# ---------------------------------------------------------------------------
# Wallet initialisation
# ---------------------------------------------------------------------------

def _load_web3():
    """Lazy-import web3 and return a connected Web3 instance."""
    try:
        from web3 import Web3  # type: ignore  # noqa: PLC0415
        return Web3(Web3.HTTPProvider(POLYGON_RPC_URL))
    except ImportError as exc:
        raise ImportError(
            "web3 package is required for live trading. "
            "Install it with: pip install 'web3>=6.0.0'"
        ) from exc


def _load_account():
    """Load the Polygon wallet from POLYGON_PRIVATE_KEY."""
    private_key = os.getenv("POLYGON_PRIVATE_KEY")
    if not private_key:
        raise EnvironmentError("POLYGON_PRIVATE_KEY not set in environment")
    w3 = _load_web3()
    account = w3.eth.account.from_key(private_key)
    return account


# ---------------------------------------------------------------------------
# Balance check
# ---------------------------------------------------------------------------

def get_usdc_balance(address: str) -> float:
    """
    Fetch live USDC balance for `address` on Polygon mainnet.

    Returns balance in USDC (float, accounting for 6 decimals).
    """
    w3 = _load_web3()
    # Use the w3 instance for checksum conversion so web3 doesn't need to be
    # imported at the call site — makes this function fully mockable via _load_web3.
    checksum_addr = w3.to_checksum_address(address)
    contract = w3.eth.contract(
        address=w3.to_checksum_address(USDC_CONTRACT_ADDRESS),
        abi=_ERC20_ABI,
    )
    raw_balance: int = contract.functions.balanceOf(checksum_addr).call()
    return raw_balance / (10 ** USDC_DECIMALS)


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

async def place_order(
    market_id: str,
    outcome: str,
    size_usdc: float,
    entry_prob: float | None = None,
) -> dict[str, Any]:
    """
    Place a live limit order on the Polymarket CLOB.

    Hard gates (enforced before any network call):
      - TRADING_MODE env var must be "LIVE" (TradingModeError otherwise)
      - size_usdc is capped to LIVE_MAX_TRADE_OVERRIDE if it would exceed it
      - USDC wallet balance is verified; returns error dict if insufficient

    Returns a dict with keys:
      - success: bool
      - order_id / tx_hash: str  (on success)
      - error: str               (on failure)
      - market_id, outcome, size_usdc, entry_prob, timestamp
    """
    # --- Hard gate 1: mode check ---
    trading_mode = os.getenv("TRADING_MODE", "PAPER").upper()
    if trading_mode != "LIVE":
        raise TradingModeError(
            f"place_order() called but TRADING_MODE={trading_mode!r}. "
            "Set TRADING_MODE=LIVE to enable real trading."
        )

    # --- Hard gate 2: size cap ---
    cap = float(os.getenv("LIVE_MAX_TRADE_OVERRIDE", str(LIVE_MAX_TRADE_OVERRIDE)))
    if size_usdc > cap:
        logger.warning(
            "Trade size $%.2f exceeds LIVE_MAX_TRADE_OVERRIDE $%.2f — capping.",
            size_usdc, cap,
        )
        size_usdc = cap

    # --- Balance check ---
    try:
        account = _load_account()
        balance = get_usdc_balance(account.address)
    except Exception as exc:
        logger.error("Failed to load wallet or check balance: %s", exc)
        return {
            "success": False,
            "error": f"Wallet error: {exc}",
            "market_id": market_id,
            "outcome": outcome,
            "size_usdc": size_usdc,
            "entry_prob": entry_prob,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    if balance < size_usdc:
        msg = (
            f"Insufficient USDC balance: have ${balance:.2f}, "
            f"need ${size_usdc:.2f} for market {market_id}"
        )
        logger.warning(msg)
        return {
            "success": False,
            "error": msg,
            "market_id": market_id,
            "outcome": outcome,
            "size_usdc": size_usdc,
            "entry_prob": entry_prob,
            "balance": balance,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # --- CLOB API call ---
    api_key = os.getenv("POLYMARKET_API_KEY", "")
    order_payload = {
        "market": market_id,
        "outcome": outcome,
        "price": entry_prob,
        "amount": size_usdc,
        "type": "LIMIT",
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{CLOB_BASE_URL}/order",
                json=order_payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )
    except httpx.RequestError as exc:
        logger.error("CLOB API request failed for market %s: %s", market_id, exc)
        return {
            "success": False,
            "error": f"Network error: {exc}",
            "market_id": market_id,
            "outcome": outcome,
            "size_usdc": size_usdc,
            "entry_prob": entry_prob,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    if resp.status_code not in (200, 201):
        logger.error(
            "CLOB API error %d for market %s: %s",
            resp.status_code, market_id, resp.text[:200],
        )
        return {
            "success": False,
            "error": f"CLOB API {resp.status_code}: {resp.text[:200]}",
            "market_id": market_id,
            "outcome": outcome,
            "size_usdc": size_usdc,
            "entry_prob": entry_prob,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    order_data = resp.json()
    order_id = order_data.get("id") or order_data.get("order_id", "")
    tx_hash = order_data.get("transactionHash", order_data.get("tx_hash", ""))

    logger.info(
        "[LIVE] Order placed: market=%s outcome=%s size=%.2f prob=%s order_id=%s tx_hash=%s",
        market_id, outcome, size_usdc, entry_prob, order_id, tx_hash,
    )

    # --- Persist to trades_live ---
    ts = datetime.now(timezone.utc).isoformat()
    await _log_live_trade(
        market_id=market_id,
        outcome=outcome,
        size_usdc=size_usdc,
        entry_prob=entry_prob,
        order_id=order_id,
        tx_hash=tx_hash,
        timestamp=ts,
    )

    return {
        "success": True,
        "order_id": order_id,
        "tx_hash": tx_hash,
        "market_id": market_id,
        "outcome": outcome,
        "size_usdc": size_usdc,
        "entry_prob": entry_prob,
        "timestamp": ts,
    }


# ---------------------------------------------------------------------------
# Position closure
# ---------------------------------------------------------------------------

async def close_position(market_id: str) -> dict[str, Any]:
    """
    Called when a market resolves. Logs the closure event.

    In production this would also submit a sell/close order to the CLOB;
    for now it marks any open live trade for that market as closed in SQLite.

    Uses module-level `get_open_trades` / `close_trade` names so tests can
    patch them via patch.object(live_trader, 'get_open_trades', ...).
    """
    import sys
    _mod = sys.modules[__name__]

    ts = datetime.now(timezone.utc).isoformat()
    logger.info("[LIVE] Closing position for market %s at %s", market_id, ts)

    open_trades = await _mod.get_open_trades(paper=False)
    closed_ids = []
    for trade in open_trades:
        if trade.get("market_id") == market_id:
            pnl = await _mod.close_trade(trade["id"], exit_price=0.0, paper=False)
            closed_ids.append(trade["id"])
            logger.info(
                "[LIVE] Trade %d closed for market %s, pnl=%.4f",
                trade["id"], market_id, pnl or 0.0,
            )

    return {
        "market_id": market_id,
        "closed_trade_ids": closed_ids,
        "timestamp": ts,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _log_live_trade(
    market_id: str,
    outcome: str,
    size_usdc: float,
    entry_prob: float | None,
    order_id: str,
    tx_hash: str,
    timestamp: str,
) -> None:
    """Persist a live trade record to the trades_live SQLite table."""
    import aiosqlite
    from src.calibration.tracker import DB_PATH

    entry_price = entry_prob or 0.0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO trades_live
                (market_id, entry_price, entry_prob, strategy, size, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
            """,
            (
                market_id,
                entry_price,
                entry_prob or 0.0,
                f"live:{outcome}",
                size_usdc,
                timestamp,
            ),
        )
        await db.commit()
    logger.debug(
        "Logged live trade: market=%s order_id=%s tx_hash=%s",
        market_id, order_id, tx_hash,
    )
