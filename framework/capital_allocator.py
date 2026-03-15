"""
framework/capital_allocator.py

Capital Allocator, Performance Tracker, and Promotion Manager for EdgeLab.
Async throughout; SQLite uses WAL mode; Redis for coordination.
"""

import asyncio
import json
import logging
import math
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import aiosqlite
import redis.asyncio as aioredis

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "framework.db"
REDIS_URL = "redis://127.0.0.1:6379/0"

REDIS_KEY_ALLOCATIONS = "edgelab:risk:allocations"
REDIS_KEY_LAST_REBALANCE = "edgelab:alloc:last_rebalance"
REDIS_KEY_PROMOTIONS = "edgelab:alloc:promotions"

REBALANCE_INTERVAL_SECONDS = 3600  # 1 hour

FLOOR_PCT = 0.05        # 5% floor per active strategy
CAP_PCT = 0.30          # 30% cap per strategy
NEW_STRAT_CAP_PCT = 0.08  # 8% cap for new strategies (trade_count < 10)
NEW_STRAT_THRESHOLD = 10  # trade count below which a strategy is "new"
DEFAULT_WEIGHT_FLOOR = 0.1  # minimum weight before normalisation

STARTING_BANKROLL = float(os.environ.get("STARTING_BANKROLL", "5000.0"))

logger = logging.getLogger("framework.capital_allocator")

# ---------------------------------------------------------------------------
# Strategy registry (optional import — may not exist during parallel build)
# ---------------------------------------------------------------------------
try:
    from command_center.strategy_registry import STRATEGY_REGISTRY  # type: ignore
except ImportError:
    STRATEGY_REGISTRY = {}

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _init_db(conn: aiosqlite.Connection) -> None:
    """Create tables if they don't exist and enable WAL mode."""
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id           INTEGER PRIMARY KEY,
            strategy_name TEXT,
            market_id    TEXT,
            side         TEXT,
            size         REAL,
            entry_price  REAL,
            exit_price   REAL,
            pnl          REAL,
            opened_at    TEXT,
            closed_at    TEXT
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            date          TEXT,
            strategy_name TEXT,
            trades        INTEGER,
            wins          INTEGER,
            pnl           REAL,
            sharpe        REAL,
            PRIMARY KEY (date, strategy_name)
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS allocation_history (
            ts            TEXT,
            strategy_name TEXT,
            allocated_usd REAL
        )
    """)
    await conn.commit()


async def _get_db() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(str(DB_PATH))
    conn.row_factory = aiosqlite.Row
    await _init_db(conn)
    return conn


# ---------------------------------------------------------------------------
# CapitalAllocator
# ---------------------------------------------------------------------------

class CapitalAllocator:
    """
    Allocates bankroll across strategies using Sharpe-weighted allocation
    with floors, caps, and new-strategy guardrails.
    """

    def __init__(self, redis_client: Optional[aioredis.Redis] = None) -> None:
        self._redis = redis_client
        self._owns_redis = redis_client is None

    async def _ensure_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        return self._redis

    async def close(self) -> None:
        if self._owns_redis and self._redis is not None:
            await self._redis.aclose()

    # ------------------------------------------------------------------
    # Core allocation logic
    # ------------------------------------------------------------------

    async def allocate(
        self,
        total_bankroll: float,
        strategy_performances: dict[str, dict],
    ) -> dict[str, float]:
        """
        Returns {strategy_name: dollar_amount} for all active strategies.

        Algorithm:
        1. Filter to is_active=True
        2. weight[s] = max(sharpe_7d, 0.1)
        3. Normalise to sum=1.0
        4. Apply 5% floor
        5. Apply 30% cap (8% for new strategies; force paper mode marker)
        6. Renormalise
        7. Convert to dollar amounts
        """
        # Step 1 — active strategies only
        active = {
            name: perf
            for name, perf in strategy_performances.items()
            if perf.get("is_active", False)
        }
        if not active:
            logger.warning("[ALLOC] No active strategies; returning empty allocation.")
            return {}

        n = len(active)

        # Per-strategy caps (before normalisation)
        caps: dict[str, float] = {}
        for name, perf in active.items():
            is_new = perf.get("trade_count_7d", 0) < NEW_STRAT_THRESHOLD
            caps[name] = NEW_STRAT_CAP_PCT if is_new else CAP_PCT

        # Floor: each active strategy gets at least FLOOR_PCT, but scale down
        # if n * floor would exceed 1.0 (e.g. 25 strategies at 5% = 125%).
        floor_frac = min(FLOOR_PCT, 1.0 / n)

        # Step 2 — raw weights
        raw_weights: dict[str, float] = {
            name: max(perf.get("sharpe_7d", 0.0), DEFAULT_WEIGHT_FLOOR)
            for name, perf in active.items()
        }

        # Step 3 — normalise
        weights = _normalise(raw_weights)

        # Steps 4-6 — water-filling to enforce floors and caps.
        #
        # Each pass: distribute remaining budget proportionally among free
        # strategies. Strategies that violate a bound get pinned at that bound
        # and removed from the free pool. When no free strategies remain,
        # any surplus budget is redistributed proportionally among strategies
        # not yet at their absolute cap (water fills up from the bottom).

        final_weights: dict[str, float] = {}
        free_names = set(raw_weights.keys())

        for _ in range(len(active) * 2 + 2):
            if not free_names:
                break

            pinned_sum = sum(final_weights.values())
            remaining = max(0.0, 1.0 - pinned_sum)

            # Proportionally distribute remaining budget among free strategies
            total_free_raw = sum(raw_weights[n] for n in free_names) or 1.0
            candidate: dict[str, float] = {
                n: (raw_weights[n] / total_free_raw) * remaining
                for n in free_names
            }

            # Find violations
            newly_pinned: dict[str, float] = {}
            for name in list(free_names):
                w = candidate[name]
                cap = caps[name]
                if w > cap + 1e-9:
                    newly_pinned[name] = cap
                elif w < floor_frac - 1e-9:
                    newly_pinned[name] = floor_frac

            if not newly_pinned:
                # All free strategies fit within bounds — accept and finish
                final_weights.update(candidate)
                free_names = set()
                break

            final_weights.update(newly_pinned)
            free_names -= newly_pinned.keys()

        # Distribute any surplus proportionally — iteratively cap-constrained.
        # (Handles the case where all strategies were pinned at their floors
        # but their caps allow more; e.g. only new strategies in the mix.)
        for _ in range(len(active) + 2):
            total_assigned = sum(final_weights.values())
            surplus = 1.0 - total_assigned
            if surplus <= 1e-9:
                break
            # Strategies with room above current weight, up to their cap
            below_cap = {
                n: caps[n] - w
                for n, w in final_weights.items()
                if caps[n] - w > 1e-9
            }
            if not below_cap:
                break
            total_room = sum(below_cap.values())
            for name, room in below_cap.items():
                add = min(surplus * (room / total_room), room)
                final_weights[name] += add

        weights = final_weights

        # Step 7 — convert to dollars
        allocations = {name: round(frac * total_bankroll, 2) for name, frac in weights.items()}

        logger.info(
            "[ALLOC] Allocated $%.2f across %d strategies: %s",
            total_bankroll,
            len(allocations),
            {k: f"${v:.2f}" for k, v in allocations.items()},
        )
        return allocations

    # ------------------------------------------------------------------
    # Scheduled rebalance
    # ------------------------------------------------------------------

    async def rebalance_if_due(self) -> dict[str, float]:
        """
        Check Redis timestamp; if > 1 hour old (or missing), rebalance.
        Writes new allocations to Redis and returns them.
        """
        r = await self._ensure_redis()

        last_ts_raw = await r.get(REDIS_KEY_LAST_REBALANCE)
        if last_ts_raw:
            last_ts = datetime.fromisoformat(last_ts_raw)
            elapsed = (datetime.now(timezone.utc) - last_ts).total_seconds()
            if elapsed < REBALANCE_INTERVAL_SECONDS:
                logger.debug("[ALLOC] Rebalance not due (%.0fs remaining).", REBALANCE_INTERVAL_SECONDS - elapsed)
                # Return cached allocations if available
                cached = await r.get(REDIS_KEY_ALLOCATIONS)
                if cached:
                    return json.loads(cached)
                return {}

        # Fetch performances and run allocation
        tracker = PerformanceTracker()
        try:
            performances = await tracker.get_all_performances(days=7)
        finally:
            await tracker.close()

        allocations = await self.allocate(STARTING_BANKROLL, performances)

        # Persist to Redis
        now_iso = datetime.now(timezone.utc).isoformat()
        async with r.pipeline() as pipe:
            pipe.set(REDIS_KEY_ALLOCATIONS, json.dumps(allocations))
            pipe.set(REDIS_KEY_LAST_REBALANCE, now_iso)
            await pipe.execute()

        # Log to DB
        if allocations:
            alloc_conn = await _get_db()
            try:
                rows = [(now_iso, name, amount) for name, amount in allocations.items()]
                await alloc_conn.executemany(
                    "INSERT INTO allocation_history (ts, strategy_name, allocated_usd) VALUES (?, ?, ?)",
                    rows,
                )
                await alloc_conn.commit()
            finally:
                await alloc_conn.close()

        logger.info("[ALLOC] Rebalanced at %s → %s", now_iso, allocations)
        return allocations


# ---------------------------------------------------------------------------
# PerformanceTracker
# ---------------------------------------------------------------------------

class PerformanceTracker:
    """Tracks trades and computes strategy performance metrics from SQLite."""

    def __init__(self) -> None:
        self._conn: Optional[aiosqlite.Connection] = None

    async def _db(self) -> aiosqlite.Connection:
        if self._conn is None:
            self._conn = await _get_db()
        return self._conn

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------

    async def record_trade(
        self,
        strategy_name: str,
        market_id: str,
        side: str,
        size: float,
        entry: float,
        exit_price: float,
        pnl: float,
        opened_at: str,
        closed_at: str,
    ) -> None:
        conn = await self._db()
        await conn.execute(
            """
            INSERT INTO trades
                (strategy_name, market_id, side, size, entry_price, exit_price, pnl, opened_at, closed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (strategy_name, market_id, side, size, entry, exit_price, pnl, opened_at, closed_at),
        )
        await conn.commit()
        logger.debug("[TRACKER] Recorded trade for %s: pnl=%.4f", strategy_name, pnl)

    # ------------------------------------------------------------------

    async def get_strategy_performance(
        self,
        strategy_name: str,
        days: int = 7,
    ) -> dict[str, Any]:
        """Returns a performance dict matching the strategy_performances schema."""
        conn = await self._db()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        async with conn.execute(
            """
            SELECT pnl,
                   CASE WHEN pnl > 0 THEN 1 ELSE 0 END AS win
            FROM trades
            WHERE strategy_name = ?
              AND closed_at >= ?
            ORDER BY closed_at
            """,
            (strategy_name, cutoff),
        ) as cur:
            rows = await cur.fetchall()

        trade_count = len(rows)
        wins = sum(r["win"] for r in rows)
        total_pnl = sum(r["pnl"] for r in rows)

        # Per-trade Sharpe: mean(pnl) / stddev(pnl)
        sharpe = 0.0
        if trade_count >= 2:
            pnls = [r["pnl"] for r in rows]
            mean_pnl = total_pnl / trade_count
            variance = sum((p - mean_pnl) ** 2 for p in pnls) / trade_count
            stddev = math.sqrt(variance) if variance > 0 else 0.0
            if stddev > 1e-9:
                sharpe = mean_pnl / stddev

        win_rate = (wins / trade_count) if trade_count > 0 else 0.0

        # Determine mode from registry (fall back to 'paper')
        mode = "paper"
        if STRATEGY_REGISTRY and strategy_name in STRATEGY_REGISTRY:
            mode = STRATEGY_REGISTRY[strategy_name].get("mode", "paper")

        return {
            "sharpe_7d": round(sharpe, 4),
            "win_rate_7d": round(win_rate, 4),
            "trade_count_7d": trade_count,
            "pnl_7d": round(total_pnl, 4),
            "is_active": True,  # tracked strategies are presumed active
            "mode": mode,
        }

    # ------------------------------------------------------------------

    async def get_all_performances(self, days: int = 7) -> dict[str, dict]:
        conn = await self._db()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        async with conn.execute(
            "SELECT DISTINCT strategy_name FROM trades WHERE closed_at >= ?",
            (cutoff,),
        ) as cur:
            rows = await cur.fetchall()

        strategy_names = [row["strategy_name"] for row in rows]
        results: dict[str, dict] = {}
        for name in strategy_names:
            results[name] = await self.get_strategy_performance(name, days=days)
        return results

    # ------------------------------------------------------------------

    async def get_recent_trades(self, limit: int = 50) -> list[dict]:
        conn = await self._db()
        async with conn.execute(
            "SELECT * FROM trades ORDER BY closed_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# PromotionManager
# ---------------------------------------------------------------------------

class PromotionManager:
    """
    Evaluates paper strategies for promotion to live trading.
    Criteria: trade_count >= 20, win_rate >= 0.55, sharpe_7d >= 0.8,
              max_drawdown_7d <= 0.20.
    """

    PROMOTION_CRITERIA = {
        "min_trades": 20,
        "min_win_rate": 0.55,
        "min_sharpe": 0.8,
        "max_drawdown": 0.20,
    }

    def __init__(self, redis_client: Optional[aioredis.Redis] = None) -> None:
        self._redis = redis_client
        self._owns_redis = redis_client is None
        self._tracker = PerformanceTracker()

    async def _ensure_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        return self._redis

    async def close(self) -> None:
        await self._tracker.close()
        if self._owns_redis and self._redis is not None:
            await self._redis.aclose()

    # ------------------------------------------------------------------

    async def _compute_max_drawdown(self, strategy_name: str, days: int = 7) -> float:
        """Compute max drawdown from cumulative PnL curve over the period."""
        conn = await self._tracker._db()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        async with conn.execute(
            "SELECT pnl FROM trades WHERE strategy_name = ? AND closed_at >= ? ORDER BY closed_at",
            (strategy_name, cutoff),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            return 0.0

        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for row in rows:
            cumulative += row["pnl"]
            if cumulative > peak:
                peak = cumulative
            if peak > 0:
                dd = (peak - cumulative) / peak
                if dd > max_dd:
                    max_dd = dd

        return round(max_dd, 4)

    # ------------------------------------------------------------------

    async def check_promotion(self, strategy_name: str) -> bool:
        """
        Returns True if the strategy meets all promotion criteria and
        was promoted (or was already live). Logs to Redis on promotion.
        """
        perf = await self._tracker.get_strategy_performance(strategy_name, days=7)
        criteria = self.PROMOTION_CRITERIA

        # Only promote paper strategies
        if perf.get("mode") == "live":
            logger.debug("[PROMO] %s is already live.", strategy_name)
            return True

        max_dd = await self._compute_max_drawdown(strategy_name, days=7)
        trade_count = perf["trade_count_7d"]

        checks = {
            "trade_count": trade_count >= criteria["min_trades"],
            "win_rate": perf["win_rate_7d"] >= criteria["min_win_rate"],
            "sharpe": perf["sharpe_7d"] >= criteria["min_sharpe"],
            "max_drawdown": max_dd <= criteria["max_drawdown"],
        }

        passed = all(checks.values())
        logger.info(
            "[PROMO] %s checks: %s | promoted=%s",
            strategy_name,
            {k: ("PASS" if v else "FAIL") for k, v in checks.items()},
            passed,
        )

        if not passed:
            return False

        # Update registry if available
        if STRATEGY_REGISTRY and strategy_name in STRATEGY_REGISTRY:
            STRATEGY_REGISTRY[strategy_name]["mode"] = "live"
            logger.info("[PROMO] Updated STRATEGY_REGISTRY['%s'].mode → 'live'", strategy_name)

        # Log to Redis
        r = await self._ensure_redis()
        promotion_record = {
            "strategy_name": strategy_name,
            "promoted_at": datetime.now(timezone.utc).isoformat(),
            "metrics": {
                "trade_count_7d": trade_count,
                "win_rate_7d": perf["win_rate_7d"],
                "sharpe_7d": perf["sharpe_7d"],
                "max_drawdown_7d": max_dd,
            },
        }
        await r.rpush(REDIS_KEY_PROMOTIONS, json.dumps(promotion_record))
        logger.info("[PROMO] %s promoted to LIVE and logged to Redis.", strategy_name)
        return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise(weights: dict[str, float]) -> dict[str, float]:
    """Normalise a dict of weights to sum to 1.0. Falls back to equal weights on zero sum."""
    total = sum(weights.values())
    if total <= 0:
        n = len(weights)
        return {k: 1.0 / n for k in weights}
    return {k: v / total for k, v in weights.items()}


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------

async def _smoke_test() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    print("\n=== EdgeLab Capital Allocator — Smoke Test ===\n")

    # 1. Ensure DB tables exist
    conn = await _get_db()
    await conn.close()
    print(f"DB initialised at {DB_PATH}")

    # 2. Fetch all performances
    tracker = PerformanceTracker()
    try:
        performances = await tracker.get_all_performances(days=7)
    finally:
        await tracker.close()

    if performances:
        print(f"\nStrategy Performances (last 7d):")
        for name, p in performances.items():
            print(
                f"  {name:30s}  trades={p['trade_count_7d']:4d}  "
                f"win_rate={p['win_rate_7d']:.2f}  sharpe={p['sharpe_7d']:.3f}  "
                f"pnl=${p['pnl_7d']:.2f}  mode={p['mode']}"
            )
    else:
        print("No strategy performance data found (no closed trades in last 7 days).")

    # 3. Run allocation
    allocator = CapitalAllocator()
    try:
        if performances:
            allocations = await allocator.allocate(STARTING_BANKROLL, performances)
            print(f"\nAllocation Table (bankroll=${STARTING_BANKROLL:.2f}):")
            for name, amount in sorted(allocations.items(), key=lambda x: -x[1]):
                pct = amount / STARTING_BANKROLL * 100
                print(f"  {name:30s}  ${amount:8.2f}  ({pct:.1f}%)")
        else:
            print(f"\nNo active strategies to allocate (bankroll=${STARTING_BANKROLL:.2f}).")
    finally:
        await allocator.close()

    print("\n=== Smoke test complete ===\n")
    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(_smoke_test())
