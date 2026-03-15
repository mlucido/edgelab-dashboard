"""
Circuit Breaker — Redis-backed trading halts and health monitoring.

All state is persisted in Redis so circuit breakers survive restarts.

Exports:
    check_all()          -> (can_trade: bool, reason: str)
    reset_strategy(name) -> manual reset of paused strategy
    reset_all(pause)     -> pause or unpause everything
    get_status()         -> full status dict for dashboard
    watchdog()           -> async loop monitoring health every 30s
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Redis key namespace
_PREFIX = "edgelab:cb:"
_KEY_GLOBAL_PAUSE = f"{_PREFIX}global_pause"
_KEY_GLOBAL_PAUSE_REASON = f"{_PREFIX}global_pause_reason"
_KEY_DAILY_PNL = f"{_PREFIX}daily_pnl"
_KEY_TRADE_OUTCOMES = f"{_PREFIX}trade_outcomes"      # JSON list, last 20 results
_KEY_STRATEGY_LOSSES = f"{_PREFIX}strategy_losses"    # JSON dict: strategy -> consecutive_losses
_KEY_STRATEGY_PAUSED = f"{_PREFIX}strategy_paused"    # JSON dict: strategy -> resume_epoch
_KEY_API_ERRORS = f"{_PREFIX}api_errors"              # JSON list of epoch timestamps
_KEY_WATCHDOG_LAST = f"{_PREFIX}watchdog_last_run"
_KEY_DRAWDOWN_PEAK = f"{_PREFIX}drawdown_peak"        # High water mark for drawdown tracking
_KEY_CATEGORY_LOSSES = f"{_PREFIX}category_losses"    # JSON dict: category -> consecutive_losses
_KEY_HOURLY_TRADE_COUNT = f"{_PREFIX}hourly_trades"   # JSON list of epoch timestamps
_KEY_ALLTIME_PNL = f"{_PREFIX}alltime_pnl"            # Running all-time P&L

# Thresholds
DAILY_LOSS_LIMIT = -20.0
WIN_RATE_MIN = 0.45
WIN_RATE_LOOKBACK = 20
CONSECUTIVE_LOSS_LIMIT = 3
STRATEGY_PAUSE_SECONDS = 3600  # 60 minutes
API_ERROR_WINDOW = 600         # 10 minutes
API_ERROR_RATE_MAX = 0.10      # 10% of calls
MAX_SINGLE_TRADE = 100.0

# --- New guardrails from stress testing (Agent RISKLAB) ---
MAX_DRAWDOWN_PCT = 0.20              # 20% drawdown → global halt (Scenario D finding)
CATEGORY_CONSECUTIVE_LOSS_LIMIT = 2  # Pause category after 2 losses (Scenario F)
CATEGORY_PAUSE_SECONDS = 7200        # 2 hours per category pause
MAX_HOURLY_TRADES = 10               # Rate limit: max 10 trades/hour (prevents runaway)
HALF_LOSS_THRESHOLD = -100.0         # At -$100 daily, cut position sizes 50% (early warning)


async def _get_redis() -> aioredis.Redis:
    return await aioredis.from_url(REDIS_URL, decode_responses=True)


async def check_all(redis: aioredis.Redis | None = None) -> tuple[bool, str]:
    """
    Check all circuit breakers. Returns (can_trade, reason).
    If can_trade is False, trading must be halted.
    """
    r = redis or await _get_redis()
    try:
        # --- Global pause flag ---
        paused = await r.get(_KEY_GLOBAL_PAUSE)
        if paused == "1":
            reason = await r.get(_KEY_GLOBAL_PAUSE_REASON) or "Manual pause active"
            return False, reason

        # --- Daily P&L limit ---
        daily_pnl_raw = await r.get(_KEY_DAILY_PNL)
        if daily_pnl_raw is not None:
            daily_pnl = float(daily_pnl_raw)
            if daily_pnl < DAILY_LOSS_LIMIT:
                reason = f"Daily P&L ${daily_pnl:.2f} below limit ${DAILY_LOSS_LIMIT:.2f} — manual reset required"
                logger.warning("CIRCUIT BREAKER: %s", reason)
                await r.set(_KEY_GLOBAL_PAUSE, "1")
                await r.set(_KEY_GLOBAL_PAUSE_REASON, reason)
                try:
                    await r.publish("alerts", json.dumps({
                        "type": "CIRCUIT_BREAKER_TRIGGERED",
                        "details": {"reason": reason},
                    }))
                except Exception:
                    pass
                return False, reason

        # --- Win rate check (last 20 trades) ---
        outcomes_raw = await r.get(_KEY_TRADE_OUTCOMES)
        if outcomes_raw:
            outcomes: list[int] = json.loads(outcomes_raw)  # 1 = win, 0 = loss
            if len(outcomes) >= WIN_RATE_LOOKBACK:
                recent = outcomes[-WIN_RATE_LOOKBACK:]
                win_rate = sum(recent) / len(recent)
                if win_rate < WIN_RATE_MIN:
                    reason = (
                        f"Win rate {win_rate:.1%} over last {WIN_RATE_LOOKBACK} trades "
                        f"below {WIN_RATE_MIN:.0%} — calibration drift suspected"
                    )
                    logger.warning("CIRCUIT BREAKER: %s", reason)
                    await r.set(_KEY_GLOBAL_PAUSE, "1")
                    await r.set(_KEY_GLOBAL_PAUSE_REASON, reason)
                    try:
                        await r.publish("alerts", json.dumps({
                            "type": "CIRCUIT_BREAKER_TRIGGERED",
                            "details": {"reason": reason},
                        }))
                    except Exception:
                        pass
                    return False, reason

        # --- Drawdown check (added by stress test — Scenario D) ---
        peak_raw = await r.get(_KEY_DRAWDOWN_PEAK)
        alltime_raw = await r.get(_KEY_ALLTIME_PNL)
        if peak_raw is not None and alltime_raw is not None:
            peak = float(peak_raw)
            alltime = float(alltime_raw)
            if peak > 0:
                drawdown = (peak - alltime) / peak
                if drawdown >= MAX_DRAWDOWN_PCT:
                    reason = (
                        f"Drawdown {drawdown:.1%} exceeds {MAX_DRAWDOWN_PCT:.0%} limit "
                        f"(peak ${peak:.2f}, current ${alltime:.2f}) — manual review required"
                    )
                    logger.warning("CIRCUIT BREAKER: %s", reason)
                    await r.set(_KEY_GLOBAL_PAUSE, "1")
                    await r.set(_KEY_GLOBAL_PAUSE_REASON, reason)
                    try:
                        await r.publish("alerts", json.dumps({
                            "type": "CIRCUIT_BREAKER_TRIGGERED",
                            "details": {"reason": reason, "drawdown_pct": drawdown},
                        }))
                    except Exception:
                        pass
                    return False, reason

        # --- Trade rate limit (prevents runaway trading) ---
        trades_raw = await r.get(_KEY_HOURLY_TRADE_COUNT)
        if trades_raw:
            now = time.time()
            trade_times: list[float] = json.loads(trades_raw)
            recent_trades = [t for t in trade_times if now - t <= 3600]
            if len(recent_trades) >= MAX_HOURLY_TRADES:
                reason = (
                    f"{len(recent_trades)} trades in last hour — "
                    f"rate limit {MAX_HOURLY_TRADES}/hr exceeded"
                )
                logger.warning("CIRCUIT BREAKER: %s", reason)
                return False, reason

        # --- API error rate ---
        errors_raw = await r.get(_KEY_API_ERRORS)
        if errors_raw:
            error_times: list[float] = json.loads(errors_raw)
            now = time.time()
            recent_errors = [t for t in error_times if now - t <= API_ERROR_WINDOW]
            # We don't have total call count here; use a reasonable proxy:
            # if more than 10 errors in 10 minutes, trigger the breaker
            if len(recent_errors) >= 10:
                reason = (
                    f"{len(recent_errors)} API errors in last "
                    f"{API_ERROR_WINDOW // 60} minutes — pausing execution"
                )
                logger.warning("CIRCUIT BREAKER: %s", reason)
                await r.set(_KEY_GLOBAL_PAUSE, "1")
                await r.set(_KEY_GLOBAL_PAUSE_REASON, reason)
                try:
                    await r.publish("alerts", json.dumps({
                        "type": "CIRCUIT_BREAKER_TRIGGERED",
                        "details": {"reason": reason},
                    }))
                except Exception:
                    pass
                return False, reason

        return True, "ok"
    finally:
        if redis is None:
            await r.aclose()


async def check_strategy_allowed(strategy: str, redis: aioredis.Redis | None = None) -> tuple[bool, str]:
    """
    Check whether a specific strategy is currently paused.
    Returns (allowed, reason).
    """
    r = redis or await _get_redis()
    try:
        paused_raw = await r.get(_KEY_STRATEGY_PAUSED)
        if not paused_raw:
            return True, "ok"
        paused: dict[str, float] = json.loads(paused_raw)
        resume_epoch = paused.get(strategy, 0)
        if resume_epoch > time.time():
            remaining = int(resume_epoch - time.time())
            return False, f"Strategy '{strategy}' paused — {remaining}s remaining"
        return True, "ok"
    finally:
        if redis is None:
            await r.aclose()


async def record_trade_outcome(
    win: bool,
    strategy: str,
    pnl: float,
    category: str = "",
    redis: aioredis.Redis | None = None,
) -> None:
    """
    Record a trade result for circuit breaker tracking.
    Called after a trade closes with its outcome.
    """
    r = redis or await _get_redis()
    try:
        # Update win/loss list (keep last 100 for buffer)
        outcomes_raw = await r.get(_KEY_TRADE_OUTCOMES)
        outcomes: list[int] = json.loads(outcomes_raw) if outcomes_raw else []
        outcomes.append(1 if win else 0)
        if len(outcomes) > 100:
            outcomes = outcomes[-100:]
        await r.set(_KEY_TRADE_OUTCOMES, json.dumps(outcomes))

        # Update daily P&L and check for milestones
        daily_raw = await r.get(_KEY_DAILY_PNL)
        prev_pnl = float(daily_raw) if daily_raw else 0.0
        daily_pnl = prev_pnl + pnl
        await r.set(_KEY_DAILY_PNL, str(daily_pnl))

        # --- Drawdown tracking (added by stress test) ---
        alltime_raw = await r.get(_KEY_ALLTIME_PNL)
        alltime_pnl = float(alltime_raw) if alltime_raw else 0.0
        alltime_pnl += pnl
        await r.set(_KEY_ALLTIME_PNL, str(alltime_pnl))
        # Update high water mark
        peak_raw = await r.get(_KEY_DRAWDOWN_PEAK)
        peak = float(peak_raw) if peak_raw else 0.0
        if alltime_pnl > peak:
            await r.set(_KEY_DRAWDOWN_PEAK, str(alltime_pnl))

        # --- Trade rate tracking ---
        trades_raw = await r.get(_KEY_HOURLY_TRADE_COUNT)
        trade_times: list[float] = json.loads(trades_raw) if trades_raw else []
        now = time.time()
        trade_times.append(now)
        trade_times = [t for t in trade_times if now - t <= 3600]
        await r.set(_KEY_HOURLY_TRADE_COUNT, json.dumps(trade_times))

        # Milestone check: fire alert when daily P&L crosses $25, $50, $100
        for milestone in (25.0, 50.0, 100.0):
            if prev_pnl < milestone <= daily_pnl:
                try:
                    await r.publish("alerts", json.dumps({
                        "type": "DAILY_MILESTONE",
                        "details": {
                            "daily_pnl": daily_pnl,
                            "alltime_pnl": daily_pnl,  # best available without alltime store
                        },
                    }))
                except Exception:
                    pass
                break  # only fire the highest crossed milestone once per trade

        # Update consecutive strategy losses
        losses_raw = await r.get(_KEY_STRATEGY_LOSSES)
        losses: dict[str, int] = json.loads(losses_raw) if losses_raw else {}
        if win:
            losses[strategy] = 0
        else:
            losses[strategy] = losses.get(strategy, 0) + 1
            if losses[strategy] >= CONSECUTIVE_LOSS_LIMIT:
                # Pause this strategy for 60 minutes
                paused_raw = await r.get(_KEY_STRATEGY_PAUSED)
                paused: dict[str, float] = json.loads(paused_raw) if paused_raw else {}
                resume_at = time.time() + STRATEGY_PAUSE_SECONDS
                paused[strategy] = resume_at
                await r.set(_KEY_STRATEGY_PAUSED, json.dumps(paused))
                logger.warning(
                    "Strategy '%s' paused for %d minutes after %d consecutive losses",
                    strategy, STRATEGY_PAUSE_SECONDS // 60, losses[strategy],
                )
                try:
                    await r.publish("alerts", json.dumps({
                        "type": "STRATEGY_PAUSED",
                        "details": {"strategy": strategy},
                    }))
                except Exception:
                    pass
                losses[strategy] = 0  # reset counter after pause
        await r.set(_KEY_STRATEGY_LOSSES, json.dumps(losses))

        # --- Category consecutive loss tracking (added by stress test — Scenario F) ---
        if category:
            cat_losses_raw = await r.get(_KEY_CATEGORY_LOSSES)
            cat_losses: dict[str, int] = json.loads(cat_losses_raw) if cat_losses_raw else {}
            if win:
                cat_losses[category] = 0
            else:
                cat_losses[category] = cat_losses.get(category, 0) + 1
                if cat_losses[category] >= CATEGORY_CONSECUTIVE_LOSS_LIMIT:
                    paused_raw = await r.get(_KEY_STRATEGY_PAUSED)
                    paused: dict[str, float] = json.loads(paused_raw) if paused_raw else {}
                    cat_key = f"category:{category}"
                    resume_at = time.time() + CATEGORY_PAUSE_SECONDS
                    paused[cat_key] = resume_at
                    await r.set(_KEY_STRATEGY_PAUSED, json.dumps(paused))
                    logger.warning(
                        "Category '%s' paused for %d minutes after %d consecutive losses",
                        category, CATEGORY_PAUSE_SECONDS // 60, cat_losses[category],
                    )
                    try:
                        await r.publish("alerts", json.dumps({
                            "type": "CATEGORY_PAUSED",
                            "details": {"category": category,
                                       "consecutive_losses": cat_losses[category]},
                        }))
                    except Exception:
                        pass
                    cat_losses[category] = 0
            await r.set(_KEY_CATEGORY_LOSSES, json.dumps(cat_losses))
    finally:
        if redis is None:
            await r.aclose()


async def check_category_allowed(category: str, redis: aioredis.Redis | None = None) -> tuple[bool, str]:
    """
    Check whether a category is currently paused due to consecutive losses.
    Added by stress test (Scenario F — concentration risk).
    """
    if not category:
        return True, "ok"
    r = redis or await _get_redis()
    try:
        paused_raw = await r.get(_KEY_STRATEGY_PAUSED)
        if not paused_raw:
            return True, "ok"
        paused: dict[str, float] = json.loads(paused_raw)
        cat_key = f"category:{category}"
        resume_epoch = paused.get(cat_key, 0)
        if resume_epoch > time.time():
            remaining = int(resume_epoch - time.time())
            return False, f"Category '{category}' paused — {remaining}s remaining"
        return True, "ok"
    finally:
        if redis is None:
            await r.aclose()


async def should_reduce_size(redis: aioredis.Redis | None = None) -> tuple[bool, float]:
    """
    Check if position sizes should be reduced due to daily losses.
    Returns (should_reduce, multiplier). Multiplier is 0.5 when daily loss exceeds
    HALF_LOSS_THRESHOLD (-$100).
    Added by stress test — early warning system.
    """
    r = redis or await _get_redis()
    try:
        daily_raw = await r.get(_KEY_DAILY_PNL)
        if daily_raw is not None:
            daily_pnl = float(daily_raw)
            if daily_pnl <= HALF_LOSS_THRESHOLD:
                return True, 0.5
        return False, 1.0
    finally:
        if redis is None:
            await r.aclose()


async def record_api_error(redis: aioredis.Redis | None = None) -> None:
    """Record an API error timestamp for rate tracking."""
    r = redis or await _get_redis()
    try:
        errors_raw = await r.get(_KEY_API_ERRORS)
        errors: list[float] = json.loads(errors_raw) if errors_raw else []
        now = time.time()
        errors.append(now)
        # Prune old errors outside window
        errors = [t for t in errors if now - t <= API_ERROR_WINDOW]
        await r.set(_KEY_API_ERRORS, json.dumps(errors))
    finally:
        if redis is None:
            await r.aclose()


async def reset_strategy(name: str, redis: aioredis.Redis | None = None) -> None:
    """Manually unpause a specific strategy."""
    r = redis or await _get_redis()
    try:
        paused_raw = await r.get(_KEY_STRATEGY_PAUSED)
        paused: dict[str, float] = json.loads(paused_raw) if paused_raw else {}
        paused.pop(name, None)
        await r.set(_KEY_STRATEGY_PAUSED, json.dumps(paused))
        logger.info("Strategy '%s' manually unpaused.", name)
    finally:
        if redis is None:
            await r.aclose()


async def reset_all(pause: bool = False, redis: aioredis.Redis | None = None) -> None:
    """
    If pause=True: pause everything (set global pause).
    If pause=False: unpause everything (clear all circuit breakers).
    """
    r = redis or await _get_redis()
    try:
        if pause:
            await r.set(_KEY_GLOBAL_PAUSE, "1")
            await r.set(_KEY_GLOBAL_PAUSE_REASON, "Manual global pause")
            logger.info("Global pause ACTIVATED.")
        else:
            await r.delete(_KEY_GLOBAL_PAUSE)
            await r.delete(_KEY_GLOBAL_PAUSE_REASON)
            await r.delete(_KEY_STRATEGY_PAUSED)
            await r.delete(_KEY_STRATEGY_LOSSES)
            # Reset daily P&L counter
            await r.set(_KEY_DAILY_PNL, "0")
            logger.info("All circuit breakers CLEARED.")
    finally:
        if redis is None:
            await r.aclose()


async def get_status(redis: aioredis.Redis | None = None) -> dict[str, Any]:
    """Return full circuit breaker status dict for dashboard consumption."""
    r = redis or await _get_redis()
    try:
        global_pause = (await r.get(_KEY_GLOBAL_PAUSE)) == "1"
        pause_reason = await r.get(_KEY_GLOBAL_PAUSE_REASON) or ""
        daily_pnl_raw = await r.get(_KEY_DAILY_PNL)
        daily_pnl = float(daily_pnl_raw) if daily_pnl_raw else 0.0

        outcomes_raw = await r.get(_KEY_TRADE_OUTCOMES)
        outcomes: list[int] = json.loads(outcomes_raw) if outcomes_raw else []
        recent = outcomes[-WIN_RATE_LOOKBACK:] if len(outcomes) >= WIN_RATE_LOOKBACK else outcomes
        win_rate = sum(recent) / len(recent) if recent else None

        losses_raw = await r.get(_KEY_STRATEGY_LOSSES)
        consecutive_losses: dict[str, int] = json.loads(losses_raw) if losses_raw else {}

        paused_raw = await r.get(_KEY_STRATEGY_PAUSED)
        paused_strategies: dict[str, float] = json.loads(paused_raw) if paused_raw else {}
        now = time.time()
        active_pauses = {
            s: {"resume_at": t, "seconds_remaining": max(0, int(t - now))}
            for s, t in paused_strategies.items()
            if t > now
        }

        errors_raw = await r.get(_KEY_API_ERRORS)
        error_times: list[float] = json.loads(errors_raw) if errors_raw else []
        recent_errors = [t for t in error_times if now - t <= API_ERROR_WINDOW]

        can_trade, trade_reason = await check_all(r)

        # Drawdown info
        alltime_raw = await r.get(_KEY_ALLTIME_PNL)
        alltime_pnl = float(alltime_raw) if alltime_raw else 0.0
        peak_raw = await r.get(_KEY_DRAWDOWN_PEAK)
        peak = float(peak_raw) if peak_raw else 0.0
        drawdown_pct = ((peak - alltime_pnl) / peak) if peak > 0 else 0.0

        # Hourly trade count
        trades_raw = await r.get(_KEY_HOURLY_TRADE_COUNT)
        trade_times: list[float] = json.loads(trades_raw) if trades_raw else []
        hourly_trades = len([t for t in trade_times if now - t <= 3600])

        return {
            "can_trade": can_trade,
            "trade_reason": trade_reason,
            "global_pause": global_pause,
            "pause_reason": pause_reason,
            "daily_pnl": round(daily_pnl, 2),
            "daily_loss_limit": DAILY_LOSS_LIMIT,
            "win_rate": round(win_rate, 4) if win_rate is not None else None,
            "win_rate_min": WIN_RATE_MIN,
            "win_rate_lookback": WIN_RATE_LOOKBACK,
            "total_outcomes_tracked": len(outcomes),
            "consecutive_losses_by_strategy": consecutive_losses,
            "paused_strategies": active_pauses,
            "api_errors_last_10min": len(recent_errors),
            "alltime_pnl": round(alltime_pnl, 2),
            "drawdown_peak": round(peak, 2),
            "drawdown_pct": round(drawdown_pct, 4),
            "max_drawdown_limit": MAX_DRAWDOWN_PCT,
            "hourly_trades": hourly_trades,
            "max_hourly_trades": MAX_HOURLY_TRADES,
        }
    finally:
        if redis is None:
            await r.aclose()


async def reset_daily_pnl(redis: aioredis.Redis | None = None) -> None:
    """Reset the daily P&L counter. Should be called at midnight UTC."""
    r = redis or await _get_redis()
    try:
        await r.set(_KEY_DAILY_PNL, "0")
        logger.info("Daily P&L counter reset.")
    finally:
        if redis is None:
            await r.aclose()


async def watchdog() -> None:
    """
    Async watchdog loop. Monitors health metrics every 30 seconds.
    Logs current circuit breaker status and auto-resets expired strategy pauses.
    """
    r = await _get_redis()
    logger.info("Circuit breaker watchdog started.")
    try:
        while True:
            try:
                # Prune expired strategy pauses
                paused_raw = await r.get(_KEY_STRATEGY_PAUSED)
                if paused_raw:
                    paused: dict[str, float] = json.loads(paused_raw)
                    now = time.time()
                    active = {s: t for s, t in paused.items() if t > now}
                    if len(active) != len(paused):
                        await r.set(_KEY_STRATEGY_PAUSED, json.dumps(active))
                        expired = set(paused.keys()) - set(active.keys())
                        for s in expired:
                            logger.info("Strategy '%s' pause expired — auto-unpaused.", s)

                # Log status
                can_trade, reason = await check_all(r)
                status = await get_status(r)
                logger.debug(
                    "Watchdog: can_trade=%s daily_pnl=%.2f win_rate=%s paused_strategies=%s api_errors=%d",
                    can_trade,
                    status["daily_pnl"],
                    f"{status['win_rate']:.1%}" if status["win_rate"] is not None else "N/A",
                    list(status["paused_strategies"].keys()),
                    status["api_errors_last_10min"],
                )
                if not can_trade:
                    logger.warning("Watchdog: trading HALTED — %s", reason)

                await r.set(_KEY_WATCHDOG_LAST, str(time.time()))
            except Exception as exc:
                logger.error("Watchdog error: %s", exc)

            await asyncio.sleep(30)
    except asyncio.CancelledError:
        logger.info("Circuit breaker watchdog shutting down.")
    finally:
        await r.aclose()
