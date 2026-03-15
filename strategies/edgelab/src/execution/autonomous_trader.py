"""
Autonomous Trader — Core execution engine for EdgeLab.

Subscribes to all opportunity channels, runs the full guardrail stack,
executes trades (paper or live), monitors positions, and enforces circuit breakers.

Loop: every 30 seconds, drain queued opportunities and evaluate each.

Guardrail stack (enforced in order):
    Layer 1: Trade quality filter
    Layer 2: Position sizing (quarter-Kelly with caps)
    Layer 3: Portfolio exposure limits
    Layer 4: Circuit breaker checks

Exports (importable for simulation engine):
    check_trade_quality(opp, capital) -> (pass: bool, reason: str)
    calculate_position_size(opp, capital, portfolio) -> (size: float, reason: str)
    check_portfolio_exposure(opp, size, portfolio, capital) -> (pass: bool, reason: str)
    check_circuit_breakers(strategy) -> (pass: bool, reason: str)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis

from src.calibration.tracker import (
    init_db,
    record_opportunity,
    get_open_trades,
    get_closed_trades,
)
from src.risk.sizer import _kelly_fraction, calculate_ev, PLATFORM_FEE
from src.execution.circuit_breaker import (
    check_all as cb_check_all,
    check_strategy_allowed,
    record_api_error,
)

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TRADING_MODE = os.getenv("TRADING_MODE", "paper").lower()
TOTAL_CAPITAL = float(os.getenv("TOTAL_CAPITAL", "500"))
KALSHI_ENABLED = os.getenv("KALSHI_ENABLED", "false").lower() in ("true", "1", "yes")

OPPORTUNITY_CHANNELS = [
    "opportunities:resolution_lag",
    "opportunities:threshold",
    "opportunities:arb",
    "opportunities:whale",
]

LOOP_INTERVAL = 30  # seconds

# Layer 1 constants — relaxed in paper mode to validate pipeline flow
_is_paper = os.getenv("TRADING_MODE", "paper").lower() != "live"
MIN_NET_RETURN_AFTER_FEE = 0.001 if _is_paper else 0.015
MIN_LIQUIDITY = 50.0 if _is_paper else 1000.0
MAX_DAYS_TO_RESOLUTION = 30 if _is_paper else 9
MIN_RESOLUTION_CONFIDENCE = 0.85

# Layer 2 constants
KELLY_MULTIPLIER = 0.30
MAX_TRADE_SIZE = 50.0
MIN_TRADE_SIZE = 10.0
HARD_STOP_TRADE_SIZE = 100.0       # Bug guard

# Layer 3 constants
MAX_DEPLOYED_FRACTION = 0.20
MAX_CATEGORY_FRACTION = 0.20       # 20% of total capital per category
MAX_CATEGORY_CONCENTRATION = 0.40  # No more than 40% of open positions in same category
MAX_SINGLE_MARKET_EXPOSURE = 100.0
MAX_OPEN_POSITIONS = 3

# Category mapping heuristics (keywords in market question -> category)
_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "politics": ["election", "president", "senate", "congress", "governor", "vote", "democrat", "republican", "trump", "biden", "harris"],
    "crypto": ["bitcoin", "btc", "ethereum", "eth", "crypto", "defi", "nft", "solana", "sol", "token"],
    "sports": ["nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball", "baseball", "championship", "super bowl", "world cup"],
    "macro": ["inflation", "fed", "interest rate", "gdp", "recession", "unemployment", "cpi", "fomc"],
}


def _infer_category(opp: dict[str, Any]) -> str:
    """Infer market category from opportunity metadata."""
    question = (opp.get("market_question") or "").lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in question for kw in keywords):
            return category
    return "other"


# ---------------------------------------------------------------------------
# Layer 1 — Trade Quality Filter
# ---------------------------------------------------------------------------

def check_trade_quality(
    opp: dict[str, Any],
    capital: float = TOTAL_CAPITAL,
) -> tuple[bool, str]:
    """
    Layer 1: Trade quality filter. All sub-checks must pass.

    Returns (passes: bool, reason: str).
    """
    strategy = opp.get("strategy", "unknown")
    market_id = opp.get("market_id") or opp.get("buy_yes", {}).get("market_id", "?")

    # For arb opportunities, use locked_return and composite liquidity
    if strategy == "arb":
        locked_return = float(opp.get("locked_return", 0))
        if locked_return <= MIN_NET_RETURN_AFTER_FEE:
            return False, f"Arb locked_return {locked_return:.3f} <= min {MIN_NET_RETURN_AFTER_FEE:.3f}"

        buy_yes_liq = float(opp.get("buy_yes", {}).get("liquidity") or 0)
        buy_no_liq = float(opp.get("buy_no", {}).get("liquidity") or 0)
        min_liq = min(buy_yes_liq, buy_no_liq)
        if min_liq < MIN_LIQUIDITY:
            return False, f"Arb min leg liquidity ${min_liq:.0f} < ${MIN_LIQUIDITY:.0f}"

        # Arb has no days_to_resolution in current format; skip that check
        return True, "ok"

    # Whale strategy: rely on whale's edge, use confidence score as proxy
    if strategy == "whale":
        current_price = float(opp.get("current_price", 0))
        if current_price <= 0 or current_price >= 1:
            return False, f"Invalid current_price {current_price}"
        confidence = float(opp.get("confidence_score", 0))
        if confidence < 40:
            return False, f"Whale confidence {confidence:.0f} < 40 minimum"
        liquidity = float(opp.get("liquidity", 0))
        if liquidity < MIN_LIQUIDITY:
            return False, f"Liquidity ${liquidity:.0f} < ${MIN_LIQUIDITY:.0f}"
        days_to_resolution = float(opp.get("days_to_resolution", 999))
        if days_to_resolution > MAX_DAYS_TO_RESOLUTION:
            return False, f"days_to_resolution {days_to_resolution:.1f} > {MAX_DAYS_TO_RESOLUTION}"
        return True, "ok"

    # Standard single-market opportunity
    current_price = float(opp.get("current_price", 0))
    if current_price <= 0 or current_price >= 1:
        return False, f"Invalid current_price {current_price}"

    # Hard floor: never trade markets priced below 88 cents regardless of mode
    # Raised from 0.84 — borderline 84-88c markets cause catastrophic loss asymmetry
    if current_price < 0.88:
        return False, f"current_price {current_price:.3f} below 0.88 hard floor"

    implied_true_prob = float(opp.get("implied_true_prob", 0))
    if implied_true_prob <= 0 or implied_true_prob > 1:
        return False, f"Invalid implied_true_prob {implied_true_prob}"

    # Hard minimum: never trade below 88% implied true probability
    # Raised from 0.84 — low true_prob trades have unacceptable loss/win asymmetry
    MIN_ENTRY_PROB = 0.88
    if implied_true_prob < MIN_ENTRY_PROB:
        return False, f"implied_true_prob {implied_true_prob:.3f} < {MIN_ENTRY_PROB} minimum"

    # Net return after 0.5% fee
    net_return = implied_true_prob * (1.0 - PLATFORM_FEE) / current_price - 1.0
    if net_return <= MIN_NET_RETURN_AFTER_FEE:
        return False, f"Net return {net_return:.3f} <= min {MIN_NET_RETURN_AFTER_FEE:.3f} after {PLATFORM_FEE:.1%} fee"

    # Liquidity check
    liquidity = float(opp.get("liquidity", 0))
    if liquidity < MIN_LIQUIDITY:
        return False, f"Liquidity ${liquidity:.0f} < ${MIN_LIQUIDITY:.0f}"

    # Days to resolution
    days_to_resolution = float(opp.get("days_to_resolution", 999))
    if days_to_resolution > MAX_DAYS_TO_RESOLUTION:
        return False, f"days_to_resolution {days_to_resolution:.1f} > {MAX_DAYS_TO_RESOLUTION}"

    # Positive EV
    ev = calculate_ev(current_price, implied_true_prob)
    if ev <= 0:
        return False, f"EV {ev:.4f} <= 0"

    # Resolution lag confidence check
    if strategy == "resolution_lag":
        confidence = float(opp.get("confidence", 0)) / 100.0  # stored as 0-100
        if confidence < MIN_RESOLUTION_CONFIDENCE:
            return False, f"Resolution lag confidence {confidence:.2f} < {MIN_RESOLUTION_CONFIDENCE}"

    return True, "ok"


# ---------------------------------------------------------------------------
# Layer 2 — Position Sizing
# ---------------------------------------------------------------------------

def calculate_position_size(
    opp: dict[str, Any],
    capital: float = TOTAL_CAPITAL,
    portfolio: dict[str, Any] | None = None,
) -> tuple[float, str]:
    """
    Layer 2: Quarter-Kelly position sizing with hard caps.

    Returns (size: float, reason: str).
    size == 0 means skip this trade.
    """
    strategy = opp.get("strategy", "unknown")

    if strategy == "whale":
        # Whale sizing: use suggested_size from tracker (1-2% of whale bet)
        suggested = float(opp.get("suggested_size", 0))
        if suggested <= 0:
            return 0.0, "No whale suggested_size"
        size = max(MIN_TRADE_SIZE, min(suggested, MAX_TRADE_SIZE))
        return size, f"whale sizing: suggested={suggested:.2f} capped={size:.2f}"

    if strategy == "arb":
        # For arb, size by locked_return scaled conservatively
        locked_return = float(opp.get("locked_return", 0))
        # Kelly analog for arb: f = locked_return (no prob uncertainty)
        # Quarter-Kelly: 0.25 * locked_return * capital
        raw_size = KELLY_MULTIPLIER * locked_return * capital
        size = max(MIN_TRADE_SIZE, min(raw_size, MAX_TRADE_SIZE))
        return size, f"arb quarter-Kelly: raw={raw_size:.2f} capped={size:.2f}"

    current_price = float(opp.get("current_price", 0))
    true_prob = float(opp.get("implied_true_prob", 0))

    kelly = _kelly_fraction(current_price, true_prob)
    if kelly <= 0:
        return 0.0, f"Kelly <= 0 ({kelly:.4f}) — no edge, skip"

    kelly_q = KELLY_MULTIPLIER * kelly
    raw_size = kelly_q * capital

    # Hard cap at $50, hard floor at $10
    if raw_size < MIN_TRADE_SIZE:
        return 0.0, f"Kelly size ${raw_size:.2f} < floor ${MIN_TRADE_SIZE:.0f} — skip"

    size = min(raw_size, MAX_TRADE_SIZE)

    # Partial sizing: scale down borderline entries to cap downside
    # 0.88-0.92 true_prob → 25% size, 0.92-0.96 → 50%, 0.96+ → 100%
    if true_prob < 0.92:
        size_multiplier = 0.25
    elif true_prob < 0.96:
        size_multiplier = 0.50
    else:
        size_multiplier = 1.0

    size *= size_multiplier
    if size < MIN_TRADE_SIZE:
        # Allow smaller trades for partial sizing (floor at $5 instead of $10)
        if size < 5.0:
            return 0.0, f"Partial-sized ${size:.2f} below $5 floor — skip"

    reason = (
        f"quarter-Kelly={kelly_q:.4f} raw=${raw_size:.2f} "
        f"partial={size_multiplier:.0%} final=${size:.2f}"
    )

    return size, reason


# ---------------------------------------------------------------------------
# Layer 3 — Portfolio Exposure
# ---------------------------------------------------------------------------

def check_portfolio_exposure(
    opp: dict[str, Any],
    size: float,
    portfolio: dict[str, Any],
    capital: float = TOTAL_CAPITAL,
) -> tuple[bool, str]:
    """
    Layer 3: Portfolio-level risk limits. Checks current state before every trade.

    portfolio dict expected keys:
        deployed_capital: float  — current total $ deployed
        category_exposure: dict[str, float]  — $ deployed per category
        market_exposure: dict[str, float]   — $ deployed per market_id
        open_position_count: int

    Returns (passes: bool, reason: str).
    """
    deployed = float(portfolio.get("deployed_capital", 0))
    category_exp: dict[str, float] = portfolio.get("category_exposure", {})
    market_exp: dict[str, float] = portfolio.get("market_exposure", {})
    open_count: int = int(portfolio.get("open_position_count", 0))

    # Total deployed capital < 60% of capital
    max_deployed = MAX_DEPLOYED_FRACTION * capital
    if deployed + size > max_deployed:
        return False, (
            f"Total deployed ${deployed + size:.2f} would exceed "
            f"{MAX_DEPLOYED_FRACTION:.0%} cap (${max_deployed:.2f})"
        )

    # Single category < 20% of capital
    category = _infer_category(opp)
    max_category = MAX_CATEGORY_FRACTION * capital
    current_cat = float(category_exp.get(category, 0))
    if current_cat + size > max_category:
        return False, (
            f"Category '{category}' exposure ${current_cat + size:.2f} would exceed "
            f"{MAX_CATEGORY_FRACTION:.0%} cap (${max_category:.2f})"
        )

    # Single market exposure < $100
    market_id = opp.get("market_id") or opp.get("buy_yes", {}).get("market_id", "")
    current_market = float(market_exp.get(market_id, 0))
    if current_market + size > MAX_SINGLE_MARKET_EXPOSURE:
        return False, (
            f"Market '{market_id}' exposure ${current_market + size:.2f} would exceed "
            f"${MAX_SINGLE_MARKET_EXPOSURE:.0f} cap"
        )

    # Open positions < 20
    if open_count >= MAX_OPEN_POSITIONS:
        return False, f"Open positions {open_count} at max ({MAX_OPEN_POSITIONS})"

    # Category concentration: no more than 40% of open positions in same category
    if open_count > 0:
        # Count positions per category (including this new one)
        cat_counts: dict[str, int] = {}
        cat_exp_items = portfolio.get("category_exposure", {})
        # Estimate position count per category from $ exposure / avg size
        # More accurate: use category_position_counts if available
        cat_pos_counts: dict[str, int] = portfolio.get("category_position_counts", {})
        if cat_pos_counts:
            new_count = cat_pos_counts.get(category, 0) + 1
            total_after = open_count + 1
            concentration = new_count / total_after
            if concentration > MAX_CATEGORY_CONCENTRATION:
                return False, (
                    f"Category '{category}' concentration {concentration:.0%} "
                    f"({new_count}/{total_after}) would exceed {MAX_CATEGORY_CONCENTRATION:.0%} cap"
                )

    return True, "ok"


# ---------------------------------------------------------------------------
# Layer 4 — Circuit Breakers
# ---------------------------------------------------------------------------

async def check_circuit_breakers(
    strategy: str,
    redis: aioredis.Redis | None = None,
) -> tuple[bool, str]:
    """
    Layer 4: Check all circuit breakers (global + strategy-specific).

    Returns (passes: bool, reason: str).
    """
    can_trade, reason = await cb_check_all(redis)
    if not can_trade:
        return False, reason

    allowed, strat_reason = await check_strategy_allowed(strategy, redis)
    if not allowed:
        return False, strat_reason

    return True, "ok"


# ---------------------------------------------------------------------------
# Portfolio state builder
# ---------------------------------------------------------------------------

async def _build_portfolio_state(paper: bool) -> dict[str, Any]:
    """Build current portfolio state from open trades for Layer 3 checks."""
    open_trades = await get_open_trades(paper=paper)
    deployed = sum(float(t["size"]) for t in open_trades)
    category_exposure: dict[str, float] = defaultdict(float)
    market_exposure: dict[str, float] = defaultdict(float)
    category_position_counts: dict[str, int] = defaultdict(int)

    for trade in open_trades:
        # We don't store category in the trade, so infer from a stub opp
        stub = {"market_question": trade.get("market_id", "")}
        cat = _infer_category(stub)
        category_exposure[cat] += float(trade["size"])
        category_position_counts[cat] += 1
        market_exposure[trade["market_id"]] += float(trade["size"])

    return {
        "deployed_capital": deployed,
        "category_exposure": dict(category_exposure),
        "category_position_counts": dict(category_position_counts),
        "market_exposure": dict(market_exposure),
        "open_position_count": len(open_trades),
    }


# ---------------------------------------------------------------------------
# Trade execution
# ---------------------------------------------------------------------------

async def _execute_paper_trade(
    opp: dict[str, Any],
    size: float,
) -> int | None:
    """Record a paper trade in the DB. Returns trade id."""
    strategy = opp.get("strategy", "unknown")
    market_id = opp.get("market_id") or opp.get("buy_yes", {}).get("market_id", "unknown")
    entry_price = float(opp.get("current_price") or opp.get("buy_yes", {}).get("price", 0.5))

    trade_id = await record_opportunity(
        market_id=market_id,
        entry_price=entry_price,
        strategy=strategy,
        size=size,
        paper=True,
    )
    logger.info(
        "[PAPER] Trade recorded: id=%d market=%s strategy=%s size=%.2f price=%.3f",
        trade_id, market_id, strategy, size, entry_price,
    )
    return trade_id


async def _execute_kalshi_trade(
    opp: dict[str, Any],
    size: float,
) -> int | None:
    """
    Place a real order on Kalshi via kalshi_executor.
    Falls back to paper mode if KALSHI_API_KEY is not set.
    """
    from src.execution.kalshi_executor import place_kalshi_order

    api_key = os.getenv("KALSHI_API_KEY")
    if not api_key:
        logger.warning(
            "KALSHI_API_KEY not set — falling back to paper mode for market %s",
            opp.get("market_id"),
        )
        return await _execute_paper_trade(opp, size)

    market_ticker = opp.get("kalshi_ticker") or opp.get("market_id", "")
    side = opp.get("side", "yes")
    size_cents = int(size * 100)

    try:
        result = await place_kalshi_order(market_ticker, side, size_cents)
        if result.get("success"):
            logger.info(
                "[LIVE-KALSHI] Trade executed: market=%s side=%s size=%d¢ order_id=%s",
                market_ticker, side, size_cents, result.get("order_id"),
            )
            trade_id = await record_opportunity(
                market_id=market_ticker,
                entry_price=float(opp.get("current_price", 0.5)),
                strategy=opp.get("strategy", "unknown"),
                size=size,
                paper=False,
            )
            return trade_id
        else:
            logger.error("Kalshi order failed: %s", result.get("error"))
            await record_api_error()
            return None
    except Exception as exc:
        logger.error("Kalshi execution error for %s: %s", market_ticker, exc)
        await record_api_error()
        return None


async def _execute_live_trade(
    opp: dict[str, Any],
    size: float,
) -> int | None:
    """
    Place a real order on Polymarket CLOB API.
    Falls back to paper mode with a warning if API key is not configured.
    """
    # Route to Kalshi if enabled and opportunity is Kalshi-sourced or arb Kalshi leg
    if KALSHI_ENABLED and opp.get("platform") == "kalshi":
        return await _execute_kalshi_trade(opp, size)

    api_key = os.getenv("POLYMARKET_API_KEY")
    if not api_key:
        logger.warning(
            "POLYMARKET_API_KEY not set — falling back to paper mode for market %s",
            opp.get("market_id"),
        )
        return await _execute_paper_trade(opp, size)

    market_id = opp.get("market_id", "")
    price = float(opp.get("current_price", 0.5))
    # Polymarket CLOB API order placement (simplified — production would use py-clob-client)
    order_payload = {
        "market": market_id,
        "outcome": "YES",
        "price": price,
        "amount": size,
        "type": "LIMIT",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://clob.polymarket.com/order",
                json=order_payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )
            if resp.status_code in (200, 201):
                order_data = resp.json()
                logger.info(
                    "[LIVE] Order placed: market=%s size=%.2f price=%.3f order_id=%s",
                    market_id, size, price, order_data.get("id"),
                )
                # Record in live table
                trade_id = await record_opportunity(
                    market_id=market_id,
                    entry_price=price,
                    strategy=opp.get("strategy", "unknown"),
                    size=size,
                    paper=False,
                )
                return trade_id
            else:
                logger.error(
                    "Polymarket CLOB API error %d for market %s: %s",
                    resp.status_code, market_id, resp.text[:200],
                )
                await record_api_error()
                return None
    except httpx.RequestError as exc:
        logger.error("Polymarket CLOB API request failed for market %s: %s", market_id, exc)
        await record_api_error()
        return None


# ---------------------------------------------------------------------------
# Main opportunity processor
# ---------------------------------------------------------------------------

async def _process_opportunity(
    opp: dict[str, Any],
    paper: bool,
    redis: aioredis.Redis,
) -> bool:
    """
    Run one opportunity through the full guardrail stack and execute if passes.
    Returns True if a trade was placed.
    """
    strategy = opp.get("strategy", "unknown")
    market_id = opp.get("market_id") or str(opp)[:40]

    # Bug guard: reject any attempt to trade > hard stop amount early
    # (The sizer enforces this too but we check here for extra safety)
    raw_size_hint = float(opp.get("liquidity", 999))  # just a placeholder; checked per layer
    # Actual size computed in Layer 2

    # Layer 1: Trade quality
    passes, reason = check_trade_quality(opp)
    if not passes:
        logger.debug("L1 reject [%s] %s: %s", strategy, market_id, reason)
        return False

    # Layer 2: Position sizing
    size, size_reason = calculate_position_size(opp, capital=TOTAL_CAPITAL)
    if size <= 0:
        logger.debug("L2 skip [%s] %s: %s", strategy, market_id, size_reason)
        return False

    # Hard bug guard: should never reach this but protect against regressions
    if size > HARD_STOP_TRADE_SIZE:
        logger.error(
            "HARD STOP: size %.2f > %.0f for market %s. Bug detected — trade blocked.",
            size, HARD_STOP_TRADE_SIZE, market_id,
        )
        return False

    # Layer 3: Portfolio exposure
    portfolio = await _build_portfolio_state(paper)
    passes, reason = check_portfolio_exposure(opp, size, portfolio, capital=TOTAL_CAPITAL)
    if not passes:
        logger.debug("L3 reject [%s] %s: %s", strategy, market_id, reason)
        return False

    # Layer 4: Circuit breakers
    passes, reason = await check_circuit_breakers(strategy, redis)
    if not passes:
        logger.warning("L4 circuit breaker [%s] %s: %s", strategy, market_id, reason)
        return False

    # All guardrails passed — execute
    logger.info(
        "Executing trade: strategy=%s market=%s size=%.2f [%s]",
        strategy, market_id, size, "PAPER" if paper else "LIVE",
    )

    if paper:
        trade_id = await _execute_paper_trade(opp, size)
    else:
        trade_id = await _execute_live_trade(opp, size)

    if trade_id is not None:
        try:
            await redis.publish("alerts", json.dumps({
                "type": "TRADE_PLACED",
                "details": {
                    "market": opp.get("market_question", opp.get("market_id", "?")),
                    "size": size,
                    "prob": opp.get("current_price", 0) * 100,
                },
            }))
        except Exception as exc:
            logger.debug("Failed to publish TRADE_PLACED alert: %s", exc)

    return trade_id is not None


# ---------------------------------------------------------------------------
# Opportunity collector (Redis pubsub)
# ---------------------------------------------------------------------------

class _OpportunityCollector:
    """
    Background task that subscribes to opportunity channels
    and queues incoming messages.
    """

    def __init__(self, redis: aioredis.Redis):
        self.redis = redis
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=500)
        self._pubsub: aioredis.client.PubSub | None = None

    async def start(self) -> None:
        self._pubsub = self.redis.pubsub()
        await self._pubsub.subscribe(*OPPORTUNITY_CHANNELS)
        logger.info("Subscribed to opportunity channels: %s", OPPORTUNITY_CHANNELS)

    async def listen(self) -> None:
        """Continuously listen and enqueue messages."""
        if not self._pubsub:
            return
        try:
            async for message in self._pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    opp = json.loads(message["data"])
                    if not self.queue.full():
                        await self.queue.put(opp)
                    else:
                        logger.warning("Opportunity queue full — dropping message from %s", message["channel"])
                except json.JSONDecodeError as exc:
                    logger.warning("Failed to decode opportunity: %s", exc)
        except asyncio.CancelledError:
            logger.info("Opportunity collector shutting down.")
        finally:
            if self._pubsub:
                await self._pubsub.unsubscribe()

    def drain(self) -> list[dict[str, Any]]:
        """Return all queued opportunities without blocking."""
        items = []
        while not self.queue.empty():
            try:
                items.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return items

    async def stop(self) -> None:
        if self._pubsub:
            await self._pubsub.unsubscribe()


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------

async def run() -> None:
    """
    Autonomous trader main loop. Designed for asyncio.gather alongside
    position_monitor.run() and circuit_breaker.watchdog().
    """
    paper = TRADING_MODE != "live"
    if not paper:
        poly_key = os.getenv("POLYMARKET_API_KEY")
        kalshi_key = os.getenv("KALSHI_API_KEY")
        if not poly_key and not (KALSHI_ENABLED and kalshi_key):
            logger.warning(
                "TRADING_MODE=live but no exchange API keys set — running in paper mode."
            )
            paper = True

    start_time = datetime.now(timezone.utc).isoformat()

    logger.info(
        "Autonomous trader starting. mode=%s kalshi=%s capital=%.0f loop_interval=%ds",
        "paper" if paper else "live",
        "enabled" if KALSHI_ENABLED else "disabled",
        TOTAL_CAPITAL,
        LOOP_INTERVAL,
    )

    await init_db()

    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    collector = _OpportunityCollector(redis)
    await collector.start()

    # Start listener as a background task
    listener_task = asyncio.create_task(collector.listen())

    # Track recent trade actions for dashboard Recent Activity feed
    recent_actions: list[dict[str, Any]] = []

    async def _publish_bot_status() -> None:
        """Publish current bot status and recent activity to Redis for dashboard."""
        try:
            open_trades = await get_open_trades(paper=paper)
            closed_trades = await get_closed_trades(paper=paper)

            today = datetime.now(timezone.utc).date().isoformat()
            pnl_today = sum(
                float(t.get("pnl") or 0)
                for t in closed_trades
                if (t.get("closed_at") or "")[:10] == today
            )
            pnl_alltime = sum(float(t.get("pnl") or 0) for t in closed_trades)
            capital_deployed = sum(float(t["size"]) for t in open_trades)

            status = {
                "state": "RUNNING",
                "capital_total": TOTAL_CAPITAL,
                "capital_deployed": capital_deployed,
                "pnl_today": pnl_today,
                "pnl_alltime": pnl_alltime,
                "start_time": start_time,
                "active_positions": len(open_trades),
                "trading_mode": "paper" if paper else "live",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            await redis.set("bot:status", json.dumps(status))

            # Publish recent activity (last 20 actions)
            activity_window = recent_actions[-20:]
            await redis.set("bot:activity", json.dumps(activity_window))
        except Exception as exc:
            logger.warning("Failed to publish bot status: %s", exc)

    try:
        while True:
            await asyncio.sleep(LOOP_INTERVAL)

            # Drain all queued opportunities this cycle
            opportunities = collector.drain()

            if not opportunities:
                logger.debug("No opportunities this cycle.")
                await _publish_bot_status()
                continue

            logger.info("Processing %d opportunities this cycle.", len(opportunities))

            # Deduplicate by market_id (keep latest)
            seen: dict[str, dict[str, Any]] = {}
            for opp in opportunities:
                mid = opp.get("market_id") or json.dumps(opp, sort_keys=True)[:60]
                seen[mid] = opp

            n_traded = 0
            n_l1_reject = 0
            n_l2_reject = 0
            n_l3_reject = 0
            n_l4_reject = 0

            for opp in seen.values():
                try:
                    # Pre-check layers for debug stats
                    l1_ok, l1_reason = check_trade_quality(opp)
                    if not l1_ok:
                        n_l1_reject += 1
                        logger.debug("L1 reject: %s — %s", opp.get("market_id", "?")[:30], l1_reason)
                        continue

                    size, size_reason = calculate_position_size(opp, capital=TOTAL_CAPITAL)
                    if size <= 0:
                        n_l2_reject += 1
                        logger.debug("L2 skip: %s — %s", opp.get("market_id", "?")[:30], size_reason)
                        continue

                    traded = await _process_opportunity(opp, paper, redis)
                    if traded:
                        n_traded += 1
                        recent_actions.append({
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "action": "trade_placed",
                            "strategy": opp.get("strategy", "unknown"),
                            "market_id": opp.get("market_id") or opp.get("buy_yes", {}).get("market_id", "?"),
                            "mode": "paper" if paper else "live",
                        })
                    else:
                        # L3 or L4 rejection (already logged at debug in _process_opportunity)
                        n_l3_reject += 1  # approximate — includes L4
                except Exception as exc:
                    logger.error(
                        "Error processing opportunity for market %s: %s",
                        opp.get("market_id", "?"), exc,
                    )

            logger.info(
                "Trader cycle: %d unique opps | %d L1_reject | %d L2_reject | "
                "%d L3/L4_reject | %d TRADED",
                len(seen), n_l1_reject, n_l2_reject, n_l3_reject, n_traded,
            )

            await _publish_bot_status()

    except asyncio.CancelledError:
        logger.info("Autonomous trader shutting down.")
    finally:
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass
        await collector.stop()
        await redis.aclose()
