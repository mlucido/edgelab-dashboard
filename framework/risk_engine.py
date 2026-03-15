"""
EdgeLab Risk Engine
-------------------
KellyCalculator, RiskEngine (Redis-backed), BankrollManager
"""

from __future__ import annotations

import asyncio
import json
import math
import sys
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis

REDIS_URL = "redis://127.0.0.1:6379"
KEY_PREFIX = "edgelab:risk:"

# ─── Redis connection factory ────────────────────────────────────────────────
# We do NOT use a module-level singleton because pytest-asyncio creates a new
# event loop per test, which causes "Future attached to a different loop" errors.
# Instead, each call creates (or reuses) a connection bound to the current loop.

_redis: Optional[aioredis.Redis] = None
_redis_loop = None


async def get_redis() -> aioredis.Redis:
    global _redis, _redis_loop
    current_loop = asyncio.get_event_loop()
    if _redis is None or _redis_loop is not current_loop:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        _redis_loop = current_loop
    return _redis


# ─── KellyCalculator ─────────────────────────────────────────────────────────

class KellyCalculator:
    """Fractional Kelly position sizing."""

    @staticmethod
    def fractional_kelly(
        edge: float,
        odds: float,
        bankroll: float,
        fraction: float = 0.25,
    ) -> float:
        """
        Calculate fractional Kelly bet size.

        Parameters
        ----------
        edge    : (true_prob - market_prob) / market_prob
        odds    : decimal odds (e.g. 1.0 for even money, 2.0 for 2-to-1)
        bankroll: current available bankroll in dollars
        fraction: Kelly fraction (default 0.25 = quarter-Kelly)

        Returns
        -------
        Dollar amount to bet, floored at $1.00, capped at 20% of bankroll.
        """
        if bankroll <= 0:
            return 1.0

        # Kelly formula: f* = (edge * (odds + 1) - 1) / odds
        # Simplified for binary markets: f* = edge (when odds ~ 1)
        if odds <= 0:
            return 1.0

        kelly_fraction = (edge * (odds + 1) - 1) / odds if odds > 0 else edge
        kelly_fraction = max(0.0, kelly_fraction)

        raw_bet = bankroll * kelly_fraction * fraction
        max_bet = bankroll * 0.20
        bet = min(raw_bet, max_bet)
        bet = max(bet, 1.0)
        return round(bet, 2)


# ─── RiskEngine ──────────────────────────────────────────────────────────────

class RiskEngine:
    """Redis-backed async risk engine."""

    MAX_OPEN_POSITIONS = 8
    CIRCUIT_BREAKER_PCT = -0.15      # -15% daily drawdown
    MAX_STRATEGY_ALLOC_PCT = 0.25    # 25% of bankroll per strategy

    def __init__(self):
        self._key = lambda suffix: f"{KEY_PREFIX}{suffix}"

    # ── helpers ──────────────────────────────────────────────────────────────

    async def _get_float(self, r: aioredis.Redis, key: str, default: float = 0.0) -> float:
        val = await r.get(key)
        return float(val) if val is not None else default

    async def _get_json(self, r: aioredis.Redis, key: str, default):
        val = await r.get(key)
        return json.loads(val) if val is not None else default

    async def _set_float(self, r: aioredis.Redis, key: str, value: float):
        await r.set(key, str(value))

    async def _set_json(self, r: aioredis.Redis, key: str, value):
        await r.set(key, json.dumps(value))

    # ── bankroll ─────────────────────────────────────────────────────────────

    async def set_daily_bankroll(self, bankroll: float):
        r = await get_redis()
        await self._set_float(r, self._key("daily_bankroll"), bankroll)

    async def _get_daily_bankroll(self, r: aioredis.Redis) -> float:
        return await self._get_float(r, self._key("daily_bankroll"), default=10_000.0)

    # ── core API ─────────────────────────────────────────────────────────────

    async def check_trade_allowed(
        self, strategy_name: str, size: float
    ) -> tuple[bool, str]:
        """
        Returns (True, "") if trade is allowed, (False, reason) otherwise.

        Rules:
        1. Circuit breaker: daily_pnl < -15% of daily_bankroll
        2. Open positions cap: >= 8
        3. Strategy allocation cap: > 25% of bankroll
        """
        r = await get_redis()

        daily_bankroll = await self._get_daily_bankroll(r)
        daily_pnl = await self._get_float(r, self._key("daily_pnl"))
        open_positions: list[dict] = await self._get_json(r, self._key("open_positions"), [])

        # Rule 1: circuit breaker
        if daily_bankroll > 0:
            pnl_pct = daily_pnl / daily_bankroll
            if pnl_pct < self.CIRCUIT_BREAKER_PCT:
                reason = (
                    f"Circuit breaker: daily PnL {pnl_pct:.1%} < "
                    f"{self.CIRCUIT_BREAKER_PCT:.1%} threshold"
                )
                await r.set(self._key("halt_reason"), reason)
                return False, reason

        # Rule 2: open positions cap
        if len(open_positions) >= self.MAX_OPEN_POSITIONS:
            reason = f"Open positions cap reached ({len(open_positions)}/{self.MAX_OPEN_POSITIONS})"
            return False, reason

        # Rule 3: per-strategy allocation cap
        strategy_open = sum(
            pos["size"] for pos in open_positions if pos["strategy"] == strategy_name
        )
        max_strategy_alloc = daily_bankroll * self.MAX_STRATEGY_ALLOC_PCT
        if strategy_open + size > max_strategy_alloc:
            reason = (
                f"Strategy '{strategy_name}' allocation "
                f"${strategy_open + size:.2f} would exceed "
                f"25% cap (${max_strategy_alloc:.2f})"
            )
            return False, reason

        return True, ""

    async def record_trade_open(
        self,
        strategy_name: str,
        trade_id: str,
        size: float,
        entry_price: float,
        market_id: str,
    ):
        r = await get_redis()
        positions: list[dict] = await self._get_json(r, self._key("open_positions"), [])
        positions.append(
            {
                "trade_id": trade_id,
                "strategy": strategy_name,
                "market_id": market_id,
                "size": size,
                "entry": entry_price,
                "opened_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        await self._set_json(r, self._key("open_positions"), positions)

    async def record_trade_close(
        self,
        strategy_name: str,
        trade_id: str,
        exit_price: float,
        pnl: float,
    ):
        r = await get_redis()

        # Remove from open positions
        positions: list[dict] = await self._get_json(r, self._key("open_positions"), [])
        positions = [p for p in positions if p["trade_id"] != trade_id]
        await self._set_json(r, self._key("open_positions"), positions)

        # Update daily PnL
        daily_pnl = await self._get_float(r, self._key("daily_pnl"))
        await self._set_float(r, self._key("daily_pnl"), daily_pnl + pnl)

        # Update per-strategy PnL
        strat_key = self._key(f"strategy_pnl:{strategy_name}")
        strat_pnl = await self._get_float(r, strat_key)
        await self._set_float(r, strat_key, strat_pnl + pnl)

    async def get_portfolio_status(self) -> dict:
        r = await get_redis()

        daily_bankroll = await self._get_daily_bankroll(r)
        daily_pnl = await self._get_float(r, self._key("daily_pnl"))
        open_positions: list[dict] = await self._get_json(r, self._key("open_positions"), [])
        halt_reason = await r.get(self._key("halt_reason")) or ""

        # Gather per-strategy PnL from open position list (unique names)
        strategy_names = {p["strategy"] for p in open_positions}
        per_strategy_pnl: dict[str, float] = {}
        for name in strategy_names:
            per_strategy_pnl[name] = await self._get_float(
                r, self._key(f"strategy_pnl:{name}")
            )

        daily_pnl_pct = (daily_pnl / daily_bankroll) if daily_bankroll else 0.0
        is_halted = bool(halt_reason) or (
            daily_bankroll > 0
            and daily_pnl / daily_bankroll < self.CIRCUIT_BREAKER_PCT
        )

        return {
            "total_bankroll": daily_bankroll + daily_pnl,
            "daily_pnl": round(daily_pnl, 4),
            "daily_pnl_pct": round(daily_pnl_pct, 6),
            "open_position_count": len(open_positions),
            "open_positions": open_positions,
            "per_strategy_pnl": per_strategy_pnl,
            "is_halted": is_halted,
            "halt_reason": halt_reason,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }


# ─── BankrollManager ─────────────────────────────────────────────────────────

class BankrollManager:
    """Hourly Sharpe-weighted bankroll allocator."""

    FLOOR_PCT = 0.05    # 5% minimum per active strategy
    CAP_PCT = 0.30      # 30% maximum per strategy
    NEW_STRAT_PCT = 0.08  # 8% fixed for new strategies (<10 trades)
    NEW_STRAT_TRADE_MIN = 10

    def __init__(self, risk_engine: RiskEngine):
        self.risk = risk_engine
        self._key = lambda suffix: f"{KEY_PREFIX}{suffix}"

    async def rebalance(self, strategy_performances: dict) -> dict[str, float]:
        """
        Sharpe-weighted allocation.

        strategy_performances: {
          strategy_name: {
            "sharpe": float,
            "trade_count": int,
            "active": bool
          }
        }

        Returns {strategy_name: dollar_amount}
        """
        r = await get_redis()
        bankroll = await self.risk._get_daily_bankroll(r)

        active = {k: v for k, v in strategy_performances.items() if v.get("active", True)}
        if not active:
            return {}

        new_strats = {k for k, v in active.items() if v.get("trade_count", 0) < self.NEW_STRAT_TRADE_MIN}
        established = {k: v for k, v in active.items() if k not in new_strats}

        # Reserve a portion for new strategies (paper mode only)
        new_alloc_per = bankroll * self.NEW_STRAT_PCT
        new_total = new_alloc_per * len(new_strats)
        remaining = bankroll - new_total

        allocations: dict[str, float] = {}

        # New strategies get fixed 8%
        for name in new_strats:
            allocations[name] = new_alloc_per

        # Established strategies: Sharpe-weighted, with floor and cap
        if established:
            raw_sharpes = {k: max(0.0, v.get("sharpe", 0.0)) for k, v in established.items()}
            total_sharpe = sum(raw_sharpes.values())

            if total_sharpe == 0:
                weight_per = 1.0 / len(established)
                raw_weights = {k: weight_per for k in established}
            else:
                raw_weights = {k: s / total_sharpe for k, s in raw_sharpes.items()}

            floor_alloc = remaining * self.FLOOR_PCT
            cap_alloc = remaining * self.CAP_PCT

            # Iterative water-filling: clamp, redistribute surplus/deficit,
            # repeat until allocations are stable. This correctly handles
            # extreme weight distributions without violating the cap.
            alloc = {k: remaining * w for k, w in raw_weights.items()}
            for _ in range(len(established) + 2):
                locked: dict[str, float] = {}
                free: dict[str, float] = {}
                for k, v in alloc.items():
                    if v >= cap_alloc:
                        locked[k] = cap_alloc
                    elif v <= floor_alloc:
                        locked[k] = floor_alloc
                    else:
                        free[k] = v

                redistributable = remaining - sum(locked.values())
                if not free:
                    # All strategies are at floor or cap — done
                    alloc = {**locked}
                    break

                free_total = sum(free.values())
                if free_total <= 0:
                    equal = redistributable / len(free)
                    alloc = {**locked, **{k: equal for k in free}}
                else:
                    scale = redistributable / free_total
                    alloc = {**locked, **{k: v * scale for k, v in free.items()}}

                if all(floor_alloc <= alloc[k] <= cap_alloc + 1e-9 for k in alloc):
                    break

            for name, amt in alloc.items():
                allocations[name] = round(amt, 2)

        await r.set(self._key("allocations"), json.dumps(allocations))
        return allocations

    async def get_allocation(self, strategy_name: str) -> float:
        r = await get_redis()
        raw = await r.get(self._key("allocations"))
        if raw is None:
            # Default: 8% of daily bankroll
            bankroll = await self.risk._get_daily_bankroll(r)
            return bankroll * self.NEW_STRAT_PCT
        allocs = json.loads(raw)
        return allocs.get(strategy_name, 0.0)


# ─── Smoke test ──────────────────────────────────────────────────────────────

async def _smoke_test():
    engine = RiskEngine()
    await engine.set_daily_bankroll(10_000.0)
    status = await engine.get_portfolio_status()
    import pprint
    pprint.pprint(status)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
    sys.exit(0)
