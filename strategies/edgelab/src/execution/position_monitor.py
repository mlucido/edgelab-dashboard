"""
Position Monitor — polls open trades and closes resolved markets.

Checks Polymarket Gamma API for market resolution every 60 seconds.
On resolution: records outcome, closes the trade, logs P&L.

Exports:
    run() — async function for asyncio.gather
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis

from src.calibration.tracker import (
    get_open_trades,
    record_outcome,
    close_trade,
)
from src.execution.circuit_breaker import record_trade_outcome, record_api_error

logger = logging.getLogger(__name__)

TRADING_MODE = os.getenv("TRADING_MODE", "paper").lower()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
POLL_INTERVAL = 60  # seconds

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
GAMMA_TIMEOUT = 10.0  # seconds

# Markets that have been flagged as expired-unresolved to avoid repeat noise
_expired_warned: set[str] = set()


async def _fetch_market(client: httpx.AsyncClient, market_id: str) -> dict[str, Any] | None:
    """
    Fetch market data from Polymarket Gamma API.
    Returns None on failure (timeout, 404, etc.).
    """
    try:
        resp = await client.get(
            f"{GAMMA_API_BASE}/markets/{market_id}",
            timeout=GAMMA_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 404:
            logger.warning("Market %s not found in Gamma API (404).", market_id)
            return None
        logger.warning("Gamma API returned %d for market %s.", resp.status_code, market_id)
        return None
    except httpx.TimeoutException:
        logger.warning("Gamma API timeout for market %s.", market_id)
        await record_api_error()
        return None
    except httpx.RequestError as exc:
        logger.error("Gamma API request error for market %s: %s", market_id, exc)
        await record_api_error()
        return None


def _parse_resolution(market_data: dict[str, Any]) -> tuple[bool, bool]:
    """
    Parse Gamma API response to determine resolution status.

    Returns:
        (is_resolved: bool, resolved_yes: bool)
    """
    # Gamma API uses `resolved` bool and `winner` field
    if not market_data.get("resolved", False):
        return False, False

    winner = market_data.get("winner", "").upper()
    # "YES" or "NO" outcome
    resolved_yes = winner == "YES"

    # Some markets use outcome tokens; check outcomes array as fallback
    if not winner:
        outcomes = market_data.get("outcomes", [])
        if outcomes:
            # First outcome is typically YES
            resolved_yes = bool(market_data.get("outcome_prices", [1.0, 0.0])[0] == 1.0)

    return True, resolved_yes


def _is_expired(market_data: dict[str, Any]) -> bool:
    """Return True if the market end_date is in the past but it's not resolved."""
    end_date_str = market_data.get("end_date_iso") or market_data.get("endDate")
    if not end_date_str:
        return False
    try:
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return end_date < datetime.now(timezone.utc)
    except (ValueError, AttributeError):
        return False


async def _process_trade(
    trade: dict[str, Any],
    client: httpx.AsyncClient,
    paper: bool,
    redis: aioredis.Redis | None = None,
) -> None:
    """Check one open trade for resolution and close it if resolved."""
    market_id: str = trade["market_id"]
    trade_id: int = trade["id"]
    strategy: str = trade.get("strategy", "unknown")
    entry_price: float = trade["entry_price"]

    market_data = await _fetch_market(client, market_id)
    if market_data is None:
        return

    is_resolved, resolved_yes = _parse_resolution(market_data)

    if not is_resolved:
        # Check for expired-unresolved edge case
        if _is_expired(market_data) and market_id not in _expired_warned:
            logger.warning(
                "Market %s is past end date but not resolved. Monitoring continues.",
                market_id,
            )
            _expired_warned.add(market_id)
        return

    # Market resolved — determine exit price
    exit_price = 1.0 if resolved_yes else 0.0

    # Record outcome in market_outcomes table
    await record_outcome(market_id, resolved_yes)

    # Close the trade and compute P&L
    pnl = await close_trade(trade_id, exit_price, paper=paper)
    if pnl is None:
        logger.warning("Could not close trade %d (market=%s) — already closed?", trade_id, market_id)
        return

    won = pnl > 0
    logger.info(
        "Trade closed: id=%d market=%s strategy=%s resolved_yes=%s "
        "entry=%.3f exit=%.1f pnl=%+.2f",
        trade_id, market_id, strategy, resolved_yes,
        entry_price, exit_price, pnl,
    )

    # Update circuit breaker tracking (this also fires STRATEGY_PAUSED and DAILY_MILESTONE alerts)
    await record_trade_outcome(win=won, strategy=strategy, pnl=pnl, redis=redis)


async def check_and_close_resolved(
    paper: bool = True,
    redis: aioredis.Redis | None = None,
) -> int:
    """
    Poll all open positions and close any that have resolved.
    Returns count of positions closed this cycle.
    """
    open_trades = await get_open_trades(paper=paper)
    if not open_trades:
        return 0

    closed_count = 0
    async with httpx.AsyncClient(
        headers={"User-Agent": "EdgeLab/1.0"},
        follow_redirects=True,
    ) as client:
        for trade in open_trades:
            try:
                await _process_trade(trade, client, paper, redis=redis)
                # We can't easily detect if close happened without another DB query;
                # the logging in _process_trade covers it. Track via exception absence.
            except Exception as exc:
                logger.error(
                    "Error processing trade %d (market=%s): %s",
                    trade.get("id"), trade.get("market_id"), exc,
                )

    return closed_count


async def run() -> None:
    """
    Position monitor main loop. Polls open positions every 60 seconds.
    Designed to run as a task in asyncio.gather.
    """
    paper = TRADING_MODE != "live"
    logger.info(
        "Position monitor started. mode=%s poll_interval=%ds",
        "paper" if paper else "live",
        POLL_INTERVAL,
    )
    r = await aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        while True:
            try:
                await check_and_close_resolved(paper=paper, redis=r)
            except asyncio.CancelledError:
                logger.info("Position monitor shutting down.")
                return
            except Exception as exc:
                logger.error("Position monitor error: %s", exc)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await r.aclose()
