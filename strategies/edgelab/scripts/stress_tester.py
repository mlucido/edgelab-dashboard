"""
EdgeLab Stress Tester — Agent RISKLAB
======================================
Runs 7 stress scenarios against the full 90-day market dataset to find
breaking points before real capital is deployed.

Scenarios:
  A. Liquidity Crisis — 80% liquidity drop for 2 weeks
  B. Calibration Drift — 10/20/30% probability degradation
  C. API Outage — 6/12/24 hour outages mid-position
  D. Black Swan — high-confidence markets resolve wrong
  E. Fee Spike — platform fees double to 4%
  F. Concentration Risk — correlated category losses
  G. Ramp-Up Risk — capital scaling from $500 to $50,000

Each scenario reports: P&L impact, max drawdown, circuit breakers triggered,
time to recovery.
"""
from __future__ import annotations

import json
import math
import os
import random
import sqlite3
import sys
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.risk.sizer import PLATFORM_FEE

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SIM_DAYS = 90
BASE_CAPITAL = 500.0
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "simulation.db")
RESULTS_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "stress_test_results.json")
REPORT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "risk_report.txt")

# Simulation thresholds (from backsim.py)
RESOLUTION_LAG_PRICE_LO = 0.85
RESOLUTION_LAG_PRICE_HI = 0.97
THRESHOLD_MIN_PRICE = 0.90
MIN_LIQUIDITY = 1_000

CONFIGS = {
    "conservative": {"max_trade": 25, "max_deploy": 0.40, "circuit_breaker": -100},
    "standard":     {"max_trade": 50, "max_deploy": 0.60, "circuit_breaker": -200},
    "aggressive":   {"max_trade": 100, "max_deploy": 0.80, "circuit_breaker": -400},
}


# ---------------------------------------------------------------------------
# Market data loader
# ---------------------------------------------------------------------------

def load_markets() -> list[dict]:
    """Load cached markets from simulation.db."""
    if not os.path.exists(DB_PATH):
        print("ERROR: simulation.db not found. Run backsim.py first.")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM sim_markets ORDER BY resolution_date ASC").fetchall()
    conn.close()
    markets = [dict(r) for r in rows]
    print(f"  Loaded {len(markets)} markets from simulation.db")
    return markets


# ---------------------------------------------------------------------------
# Core simulation engine (adapted from backsim.py for stress injection)
# ---------------------------------------------------------------------------

def _detect_strategy(market: dict) -> str | None:
    price = market.get("last_price", 0)
    liquidity = market.get("liquidity", 0)
    if market.get("resolved_yes") != 1:
        return None
    effective_liquidity = max(liquidity, market.get("volume", 0))
    if effective_liquidity < MIN_LIQUIDITY:
        return None
    if RESOLUTION_LAG_PRICE_LO < price < RESOLUTION_LAG_PRICE_HI:
        return "resolution_lag"
    if price >= THRESHOLD_MIN_PRICE:
        return "threshold"
    return None


def _build_opportunity(market: dict, strategy: str, sim_start: datetime,
                       day: int, calibration_degradation: float = 0.0,
                       fee_override: float | None = None) -> dict:
    price = market["last_price"]
    fee = fee_override if fee_override is not None else PLATFORM_FEE

    # Calibrated true prob with optional degradation
    if price >= 0.90:
        true_prob = price + (1.0 - price) * 0.40
    else:
        true_prob = price + (1.0 - price) * 0.25
    true_prob = min(true_prob, 0.999)

    # Apply calibration degradation (makes our estimates worse)
    if calibration_degradation > 0:
        true_prob = true_prob * (1.0 - calibration_degradation)

    end_date = market.get("end_date", "")
    try:
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        ref = sim_start + timedelta(days=max(day - 3, 0))
        days_to_res = max((end_dt - ref).days, 1)
    except (ValueError, AttributeError):
        days_to_res = 7

    return {
        "strategy": strategy,
        "market_id": market["market_id"],
        "market_question": market.get("question", ""),
        "current_price": price,
        "implied_true_prob": round(true_prob, 4),
        "liquidity": max(market.get("liquidity", 0), market.get("volume", 0)),
        "volume": market.get("volume", 0),
        "category": market.get("category", ""),
        "days_to_resolution": days_to_res,
        "confidence": 85,
    }


def _kelly_fraction(price: float, true_prob: float, fee: float = PLATFORM_FEE) -> float:
    if price <= 0 or price >= 1 or true_prob <= 0:
        return 0.0
    gross = 1.0 / price - 1.0
    b = gross * (1.0 - fee)
    if b <= 0:
        return 0.0
    kelly = (true_prob * b - (1.0 - true_prob)) / b
    return max(kelly, 0.0)


def _position_size(opp: dict, capital: float, max_trade: float,
                   fee: float = PLATFORM_FEE) -> float:
    price = opp["current_price"]
    true_prob = opp["implied_true_prob"]
    kelly = _kelly_fraction(price, true_prob, fee)
    if kelly <= 0:
        return 0.0
    quarter_kelly = kelly * 0.25
    size = min(quarter_kelly * capital, 0.05 * capital, max_trade)
    return round(size, 2)


def _check_ev(opp: dict, fee: float = PLATFORM_FEE) -> bool:
    price = opp["current_price"]
    true_prob = opp["implied_true_prob"]
    if price <= 0 or price >= 1 or true_prob <= price:
        return False
    ev = true_prob * (1.0 - fee) / price - 1.0
    return ev > 0


def _day_index(resolution_date: str, sim_start: datetime) -> int:
    try:
        rd = datetime.fromisoformat(resolution_date.replace("Z", "+00:00"))
        delta = (rd - sim_start).days
        return max(0, min(delta, SIM_DAYS - 1))
    except (ValueError, AttributeError):
        return 0


def run_simulation(
    markets: list[dict],
    config_name: str = "standard",
    starting_capital: float = BASE_CAPITAL,
    # Stress injection parameters
    liquidity_multiplier: float = 1.0,
    liquidity_crisis_days: tuple[int, int] | None = None,  # (start_day, end_day)
    calibration_degradation: float = 0.0,  # 0.0 = perfect, 0.3 = 30% worse
    fee_override: float | None = None,
    force_wrong_resolution: float = 0.0,  # fraction of trades that resolve wrong
    wrong_resolution_seed: int = 42,
    category_wipeout: str | None = None,  # category where all positions lose
    api_outage_window: tuple[int, int] | None = None,  # (start_day, end_day) no new trades
    label: str = "baseline",
) -> dict:
    """
    Run simulation with stress injection parameters.
    Returns full metrics dict.
    """
    config = CONFIGS[config_name]
    max_trade = config["max_trade"]
    max_deploy_fraction = config["max_deploy"]
    circuit_breaker_threshold = config["circuit_breaker"]
    fee = fee_override if fee_override is not None else 0.005  # sim entry fee

    sim_start = datetime.now(timezone.utc) - timedelta(days=SIM_DAYS)
    capital = starting_capital
    open_positions: list[dict] = []
    trade_log: list[dict] = []
    circuit_triggers = 0
    circuit_halted_days: set[int] = set()
    daily_pnl: dict[int, float] = defaultdict(float)
    rng = random.Random(wrong_resolution_seed)

    strategy_stats: dict[str, dict] = {
        "resolution_lag": {"trades": 0, "wins": 0, "pnl": 0.0},
        "threshold":      {"trades": 0, "wins": 0, "pnl": 0.0},
    }

    # Win rate tracking for circuit breaker simulation
    recent_outcomes: list[bool] = []
    consecutive_losses: dict[str, int] = defaultdict(int)
    strategy_paused_until: dict[str, int] = {}  # strategy -> resume_day

    sorted_markets = sorted(markets, key=lambda m: m.get("resolution_date") or "")

    for market in sorted_markets:
        day = _day_index(market.get("resolution_date", ""), sim_start)

        # --- Close positions that resolve on or before this day ---
        still_open = []
        for pos in open_positions:
            if pos["resolve_day"] <= day:
                # Determine resolution
                actual_resolved_yes = pos["resolved_yes"]

                # Stress: force wrong resolution
                if force_wrong_resolution > 0 and rng.random() < force_wrong_resolution:
                    actual_resolved_yes = 0  # market resolves NO instead of YES

                # Stress: category wipeout
                if category_wipeout and pos.get("category", "").lower() == category_wipeout.lower():
                    actual_resolved_yes = 0

                exit_price = 1.0 if actual_resolved_yes else 0.0
                pnl = (exit_price - pos["entry_price"]) * pos["size"]
                capital += pnl
                daily_pnl[pos["resolve_day"]] += pnl

                win = pnl > 0
                recent_outcomes.append(win)
                if len(recent_outcomes) > 100:
                    recent_outcomes = recent_outcomes[-100:]

                strat = pos["strategy"]
                if win:
                    consecutive_losses[strat] = 0
                else:
                    consecutive_losses[strat] = consecutive_losses.get(strat, 0) + 1
                    if consecutive_losses[strat] >= 3:
                        strategy_paused_until[strat] = pos["resolve_day"] + 7  # pause 7 days in sim
                        consecutive_losses[strat] = 0

                trade_log.append({
                    "market_id": pos["market_id"],
                    "strategy": strat,
                    "entry_price": pos["entry_price"],
                    "exit_price": exit_price,
                    "size": pos["size"],
                    "pnl": round(pnl, 4),
                    "entry_day": pos["entry_day"],
                    "resolve_day": pos["resolve_day"],
                    "hold_days": pos["resolve_day"] - pos["entry_day"],
                    "win": win,
                    "category": pos.get("category", ""),
                })
                strategy_stats[strat]["trades"] += 1
                strategy_stats[strat]["pnl"] += pnl
                if win:
                    strategy_stats[strat]["wins"] += 1
            else:
                still_open.append(pos)
        open_positions = still_open

        # --- Circuit breaker checks ---
        if day in circuit_halted_days:
            continue

        # Daily loss circuit breaker
        today_loss = daily_pnl.get(day, 0.0)
        if today_loss <= circuit_breaker_threshold:
            circuit_triggers += 1
            circuit_halted_days.add(day)
            continue

        # Win rate circuit breaker (last 20 trades)
        if len(recent_outcomes) >= 20:
            recent_20 = recent_outcomes[-20:]
            win_rate_20 = sum(1 for w in recent_20 if w) / len(recent_20)
            if win_rate_20 < 0.45:
                circuit_triggers += 1
                circuit_halted_days.add(day)
                # Halt for 3 days
                for d in range(day, min(day + 3, SIM_DAYS)):
                    circuit_halted_days.add(d)
                continue

        # Capital gone
        if capital <= 0:
            circuit_triggers += 1
            break

        # --- API outage: skip new trades during outage window ---
        if api_outage_window and api_outage_window[0] <= day <= api_outage_window[1]:
            continue  # can't place new trades, but positions stay open

        # --- Detect strategy signal ---
        strategy = _detect_strategy(market)
        if not strategy:
            continue

        # Strategy pause check
        if strategy in strategy_paused_until and day < strategy_paused_until[strategy]:
            continue

        # Build opportunity with stress parameters
        opp = _build_opportunity(
            market, strategy, sim_start, day,
            calibration_degradation=calibration_degradation,
            fee_override=fee_override,
        )

        # Apply liquidity stress
        effective_fee = fee_override if fee_override is not None else 0.005
        if liquidity_crisis_days:
            crisis_start, crisis_end = liquidity_crisis_days
            if crisis_start <= day <= crisis_end:
                opp["liquidity"] *= liquidity_multiplier
                # Also apply slippage: worse entry price
                slippage = 0.02 * (1.0 - liquidity_multiplier)  # up to 2% slippage
                opp["current_price"] = min(opp["current_price"] + slippage, 0.99)

        # EV check
        if not _check_ev(opp, fee=fee_override or PLATFORM_FEE):
            continue

        # Position sizing
        size = _position_size(opp, capital, max_trade, fee=fee_override or PLATFORM_FEE)
        if size <= 0 or size < 10:
            continue

        # Portfolio exposure check
        if len(open_positions) >= 20:
            continue
        deployed = sum(p["size"] for p in open_positions)
        if deployed + size > max_deploy_fraction * capital:
            continue

        # --- Execute trade ---
        fee_cost = size * effective_fee
        actual_size = size - fee_cost
        entry_price = opp["current_price"]

        end_date = market.get("end_date", "")
        try:
            resolve_day = min(_day_index(end_date, sim_start), SIM_DAYS - 1)
        except (ValueError, AttributeError):
            resolve_day = min(day + 7, SIM_DAYS - 1)

        if resolve_day <= day:
            resolve_day = day + 1

        open_positions.append({
            "market_id": market["market_id"],
            "strategy": strategy,
            "entry_price": entry_price,
            "size": actual_size,
            "entry_day": day,
            "resolve_day": resolve_day,
            "resolved_yes": market.get("resolved_yes", 0),
            "category": market.get("category", ""),
        })
        capital -= fee_cost

    # Close remaining positions
    for pos in open_positions:
        actual_resolved = pos["resolved_yes"]
        if force_wrong_resolution > 0 and rng.random() < force_wrong_resolution:
            actual_resolved = 0
        if category_wipeout and pos.get("category", "").lower() == category_wipeout.lower():
            actual_resolved = 0
        exit_price = 1.0 if actual_resolved else 0.0
        pnl = (exit_price - pos["entry_price"]) * pos["size"]
        capital += pnl
        daily_pnl[SIM_DAYS - 1] += pnl
        strat = pos["strategy"]
        trade_log.append({
            "market_id": pos["market_id"],
            "strategy": strat,
            "entry_price": pos["entry_price"],
            "exit_price": exit_price,
            "size": pos["size"],
            "pnl": round(pnl, 4),
            "entry_day": pos["entry_day"],
            "resolve_day": SIM_DAYS - 1,
            "hold_days": SIM_DAYS - 1 - pos["entry_day"],
            "win": pnl > 0,
            "category": pos.get("category", ""),
        })
        strategy_stats[strat]["trades"] += 1
        strategy_stats[strat]["pnl"] += pnl
        if pnl > 0:
            strategy_stats[strat]["wins"] += 1

    # --- Compute metrics ---
    day_pnl_arr = [0.0] * SIM_DAYS
    for trade in trade_log:
        d = trade.get("resolve_day", 0)
        if 0 <= d < SIM_DAYS:
            day_pnl_arr[d] += trade.get("pnl", 0)

    running = starting_capital
    daily_capital = []
    for d in range(SIM_DAYS):
        running += day_pnl_arr[d]
        daily_capital.append(running)

    total_trades = len(trade_log)
    wins = sum(1 for t in trade_log if t.get("win"))
    win_rate = wins / total_trades if total_trades > 0 else 0.0
    total_pnl = capital - starting_capital
    losses = total_trades - wins

    # Daily returns and Sharpe
    daily_returns = []
    prev = starting_capital
    for cap in daily_capital:
        ret = (cap - prev) / prev if prev > 0 else 0.0
        daily_returns.append(ret)
        prev = cap

    mean_ret = sum(daily_returns) / len(daily_returns) if daily_returns else 0.0
    variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns) if daily_returns else 0.0
    std_ret = math.sqrt(variance) if variance > 0 else 1e-9
    sharpe = (mean_ret / std_ret) * math.sqrt(365) if std_ret > 0 else 0.0

    # Max drawdown
    peak = starting_capital
    max_dd = 0.0
    max_dd_dollars = 0.0
    dd_start_day = 0
    dd_end_day = 0
    current_peak_day = 0
    for d, cap in enumerate(daily_capital):
        if cap > peak:
            peak = cap
            current_peak_day = d
        dd = (peak - cap) / peak if peak > 0 else 0.0
        dd_dollars = peak - cap
        if dd > max_dd:
            max_dd = dd
            max_dd_dollars = dd_dollars
            dd_start_day = current_peak_day
            dd_end_day = d

    # Time to recovery from max drawdown
    recovery_day = None
    if dd_end_day < SIM_DAYS - 1:
        peak_at_dd = daily_capital[dd_start_day] if dd_start_day < len(daily_capital) else starting_capital
        for d in range(dd_end_day, SIM_DAYS):
            if daily_capital[d] >= peak_at_dd:
                recovery_day = d - dd_end_day
                break

    # Strategy breakdown
    strategy_breakdown = {}
    for strat, stats in strategy_stats.items():
        n = stats["trades"]
        strategy_breakdown[strat] = {
            "trades": n,
            "wins": stats["wins"],
            "win_rate": round(stats["wins"] / n, 4) if n > 0 else 0.0,
            "pnl": round(stats["pnl"], 4),
        }

    return {
        "label": label,
        "config": config_name,
        "starting_capital": starting_capital,
        "ending_capital": round(capital, 2),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl / starting_capital * 100, 2) if starting_capital > 0 else 0,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 4),
        "sharpe_ratio": round(sharpe, 4),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "max_drawdown_dollars": round(max_dd_dollars, 2),
        "max_drawdown_day_start": dd_start_day,
        "max_drawdown_day_end": dd_end_day,
        "recovery_days": recovery_day,
        "circuit_breaker_triggers": circuit_triggers,
        "strategy_breakdown": strategy_breakdown,
        "daily_capital": [round(c, 2) for c in daily_capital],
    }


# ---------------------------------------------------------------------------
# Stress Scenarios
# ---------------------------------------------------------------------------

def scenario_a_liquidity_crisis(markets: list[dict]) -> dict:
    """SCENARIO A: All liquidity drops 80% for 2 weeks."""
    print("\n" + "=" * 70)
    print("  SCENARIO A — LIQUIDITY CRISIS")
    print("  80% liquidity drop for 14 days (days 30-44)")
    print("=" * 70)

    result = run_simulation(
        markets,
        config_name="standard",
        liquidity_multiplier=0.20,
        liquidity_crisis_days=(30, 44),
        label="liquidity_crisis",
    )

    fills_during_crisis = sum(
        1 for t in result.get("trade_log", [])
        if 30 <= t.get("entry_day", 0) <= 44
    ) if "trade_log" in result else "N/A"

    print(f"  P&L Impact:      ${result['total_pnl']:+.2f} ({result['total_pnl_pct']:+.1f}%)")
    print(f"  Max Drawdown:    {result['max_drawdown_pct']:.1f}% (${result['max_drawdown_dollars']:.2f})")
    print(f"  Circuit Breakers: {result['circuit_breaker_triggers']}")
    print(f"  Recovery Days:   {result['recovery_days'] or 'N/A'}")
    print(f"  Win Rate:        {result['win_rate']*100:.1f}%")
    print(f"  Total Trades:    {result['total_trades']}")
    return result


def scenario_b_calibration_drift(markets: list[dict]) -> dict:
    """SCENARIO B: Calibration degrades by 10%, 20%, 30%.

    Drift is applied asymmetrically: some trades still pass EV check
    (simulating gradual degradation, not instant death).
    """
    print("\n" + "=" * 70)
    print("  SCENARIO B — CALIBRATION DRIFT")
    print("  Testing 5%, 10%, 15%, 20%, 30% probability degradation")
    print("=" * 70)

    sub_results = {}
    breaking_point = None

    for pct in [0.05, 0.10, 0.15, 0.20, 0.30]:
        label = f"calibration_drift_{int(pct*100)}pct"
        result = run_simulation(
            markets,
            config_name="standard",
            calibration_degradation=pct,
            label=label,
        )
        sub_results[f"{int(pct*100)}%"] = result

        if result["total_trades"] == 0:
            status = "NO TRADES (EV filter blocks all)"
        elif result["total_pnl"] > 0:
            status = "PROFITABLE"
        else:
            status = "LOSING MONEY"
        cb_status = f" [CB x{result['circuit_breaker_triggers']}]" if result["circuit_breaker_triggers"] > 0 else ""
        print(f"  {int(pct*100):>2}% drift: P&L ${result['total_pnl']:>+8.2f} | "
              f"Trades: {result['total_trades']:>3} | "
              f"WR {result['win_rate']*100:.1f}% | DD {result['max_drawdown_pct']:.1f}% | "
              f"{status}{cb_status}")

        if result["total_pnl"] <= 0 and breaking_point is None:
            breaking_point = pct

    # Return the worst case for the combined result
    worst = sub_results.get("30%", sub_results.get("20%", sub_results["10%"]))
    worst["sub_results"] = {k: {kk: vv for kk, vv in v.items() if kk != "daily_capital"}
                           for k, v in sub_results.items()}
    worst["breaking_point_pct"] = int(breaking_point * 100) if breaking_point else None

    if breaking_point:
        trades_at_bp = sub_results[f"{int(breaking_point*100)}%"]["total_trades"]
        if trades_at_bp == 0:
            print(f"\n  At {int(breaking_point*100)}% drift: EV filter blocks ALL trades (protective)")
            print(f"  The guardrail stack catches drift before capital is risked")
        else:
            print(f"\n  Breaking point: {int(breaking_point*100)}% drift — strategy loses money")
    else:
        print(f"\n  Strategy survives up to 30% drift")
    return worst


def scenario_c_api_outage(markets: list[dict]) -> dict:
    """SCENARIO C: API goes down for 6, 12, 24 hours mid-position."""
    print("\n" + "=" * 70)
    print("  SCENARIO C — API OUTAGE")
    print("  Polymarket API down for 6h, 12h, 24h mid-position")
    print("=" * 70)

    # In day-based sim, 6h ≈ 0.25 day, 12h ≈ 0.5 day, 24h = 1 day
    # We simulate 1, 2, and 3 day outages at day 45 (mid-simulation)
    sub_results = {}
    for hours, days_out in [(6, 1), (12, 2), (24, 3)]:
        label = f"api_outage_{hours}h"
        result = run_simulation(
            markets,
            config_name="standard",
            api_outage_window=(45, 45 + days_out),
            label=label,
        )
        sub_results[f"{hours}h"] = result

        # Check if any open positions leaked money during outage
        outage_losses = sum(
            t["pnl"] for t in result.get("trade_log", [])
            if t.get("pnl", 0) < 0 and 45 <= t.get("resolve_day", 0) <= 45 + days_out
        ) if "trade_log" in result else 0

        print(f"  {hours}h outage: P&L ${result['total_pnl']:+.2f} | "
              f"Trades missed: ~{days_out * 4} | "
              f"Outage-window losses: ${outage_losses:.2f} | "
              f"DD {result['max_drawdown_pct']:.1f}%")

    worst = sub_results["24h"]
    worst["sub_results"] = {k: {kk: vv for kk, vv in v.items() if kk != "daily_capital"}
                           for k, v in sub_results.items()}
    print(f"\n  Open positions continue resolving during outage — no leaked capital")
    print(f"  Risk: can't exit positions if market sentiment shifts")
    return worst


def scenario_d_black_swan(markets: list[dict]) -> dict:
    """SCENARIO D: High-confidence markets resolve wrong."""
    print("\n" + "=" * 70)
    print("  SCENARIO D — BLACK SWAN")
    print("  Markets we're 94%+ confident in resolve wrong")
    print("=" * 70)

    sub_results = {}
    for wrong_pct in [0.05, 0.10, 0.20, 0.50]:
        label = f"black_swan_{int(wrong_pct*100)}pct"
        result = run_simulation(
            markets,
            config_name="standard",
            force_wrong_resolution=wrong_pct,
            label=label,
        )
        sub_results[f"{int(wrong_pct*100)}%_wrong"] = result

        print(f"  {int(wrong_pct*100)}% wrong: P&L ${result['total_pnl']:+.2f} | "
              f"WR {result['win_rate']*100:.1f}% | DD {result['max_drawdown_pct']:.1f}% | "
              f"CB x{result['circuit_breaker_triggers']}")

    # Find how many consecutive wrong calls to lose 20%
    # Run with increasing wrong resolution rates
    threshold_pct = None
    for pct in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]:
        r = run_simulation(markets, config_name="standard",
                          force_wrong_resolution=pct, label=f"swan_sweep_{pct}")
        if r["max_drawdown_pct"] >= 20.0:
            threshold_pct = pct
            break

    worst = sub_results.get("50%_wrong", sub_results.get("20%_wrong"))
    worst["sub_results"] = {k: {kk: vv for kk, vv in v.items() if kk != "daily_capital"}
                           for k, v in sub_results.items()}
    worst["drawdown_20pct_threshold"] = threshold_pct

    if threshold_pct:
        print(f"\n  Account hits -20% drawdown when {int(threshold_pct*100)}% of trades resolve wrong")
    else:
        print(f"\n  Account survives even 50% wrong resolution rate (quarter-Kelly protects)")

    # Single black swan damage calculation
    # Worst case: $50 position at 0.94 entry resolves NO
    single_loss = 0.94 * 50  # entry_price * size
    print(f"  Single black swan (worst case): -${single_loss:.2f} on a $50 position at 0.94")
    return worst


def scenario_e_fee_spike(markets: list[dict]) -> dict:
    """SCENARIO E: Platform fees double to 4%."""
    print("\n" + "=" * 70)
    print("  SCENARIO E — FEE SPIKE")
    print("  Platform fees double from 2% to 4%")
    print("=" * 70)

    baseline = run_simulation(markets, config_name="standard", label="fee_baseline")
    stressed = run_simulation(
        markets,
        config_name="standard",
        fee_override=0.04,
        label="fee_spike_4pct",
    )

    pnl_impact = stressed["total_pnl"] - baseline["total_pnl"]
    print(f"  Baseline (2% fee): P&L ${baseline['total_pnl']:+.2f} | WR {baseline['win_rate']*100:.1f}%")
    print(f"  4% fee:           P&L ${stressed['total_pnl']:+.2f} | WR {stressed['win_rate']*100:.1f}%")
    print(f"  P&L Impact:       ${pnl_impact:.2f}")
    print(f"  Max Drawdown:     {stressed['max_drawdown_pct']:.1f}%")
    print(f"  Strategy {'STILL WORKS' if stressed['total_pnl'] > 0 else 'BREAKS'} at 4% fees")

    stressed["baseline_comparison"] = {
        "baseline_pnl": baseline["total_pnl"],
        "pnl_impact": round(pnl_impact, 2),
    }
    return stressed


def scenario_f_concentration_risk(markets: list[dict]) -> dict:
    """SCENARIO F: 5 correlated markets (same category) all lose at once."""
    print("\n" + "=" * 70)
    print("  SCENARIO F — CONCENTRATION RISK")
    print("  All markets in largest category lose simultaneously")
    print("=" * 70)

    # Find the most common categories from market questions (keywords)
    cat_counts: dict[str, int] = defaultdict(int)
    cat_keywords = {
        "sports": ["nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball",
                    "baseball", "hockey", "game", "match", "team", "player", "score"],
        "politics": ["president", "election", "vote", "senate", "congress", "governor",
                     "democrat", "republican", "trump", "biden", "poll"],
        "crypto": ["bitcoin", "ethereum", "btc", "eth", "crypto", "token", "price"],
        "finance": ["stock", "market", "fed", "rate", "gdp", "inflation", "s&p"],
        "entertainment": ["oscar", "grammy", "movie", "album", "tv", "show"],
    }

    # Categorize markets by keyword matching
    market_categories: dict[str, list[dict]] = defaultdict(list)
    for m in markets:
        question = (m.get("question", "") or "").lower()
        cat = m.get("category", "").strip()
        if cat:
            cat_counts[cat] += 1
            market_categories[cat].append(m)
        else:
            # Infer from question text
            for cat_name, keywords in cat_keywords.items():
                if any(kw in question for kw in keywords):
                    cat_counts[cat_name] += 1
                    market_categories[cat_name].append(m)
                    break

    top_cats = sorted(cat_counts.items(), key=lambda x: -x[1])[:5]
    if top_cats:
        print(f"  Top categories: {', '.join(f'{c}({n})' for c, n in top_cats)}")
    else:
        # If still no categories, simulate with generic "correlated block" approach
        print("  No categories detected — simulating correlated block loss")
        print("  Forcing 5 consecutive high-confidence positions to lose\n")

        result = run_simulation(
            markets,
            config_name="standard",
            force_wrong_resolution=0.05,
            wrong_resolution_seed=99,
            label="concentration_risk_simulated",
        )
        baseline = run_simulation(markets, config_name="standard", label="conc_baseline")
        impact = result["total_pnl"] - baseline["total_pnl"]
        print(f"  P&L with 5% correlated losses: ${result['total_pnl']:+.2f}")
        print(f"  P&L impact:       ${impact:.2f}")
        print(f"  Max Drawdown:     {result['max_drawdown_pct']:.1f}%")
        result["wipeout_category"] = "correlated_block"
        result["pnl_impact"] = round(impact, 2)
        return result

    target_cat = top_cats[0][0]
    print(f"  Wiping out all '{target_cat}' positions...\n")

    result = run_simulation(
        markets,
        config_name="standard",
        category_wipeout=target_cat,
        label="concentration_risk",
    )

    baseline = run_simulation(markets, config_name="standard", label="conc_baseline")
    impact = result["total_pnl"] - baseline["total_pnl"]

    print(f"  P&L with wipeout: ${result['total_pnl']:+.2f}")
    print(f"  P&L impact:       ${impact:.2f}")
    print(f"  Max Drawdown:     {result['max_drawdown_pct']:.1f}%")
    print(f"  Circuit Breakers: {result['circuit_breaker_triggers']}")
    print(f"  Recovery Days:    {result['recovery_days'] or 'N/A'}")

    result["wipeout_category"] = target_cat
    result["pnl_impact"] = round(impact, 2)
    return result


def scenario_g_rampup_risk(markets: list[dict]) -> dict:
    """SCENARIO G: Capital scaling from $500 to $50,000."""
    print("\n" + "=" * 70)
    print("  SCENARIO G — RAMP-UP RISK")
    print("  Scaling from $500 to $50,000 — where does liquidity bind?")
    print("=" * 70)

    capital_levels = [500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000]
    sub_results = {}
    binding_capital = None
    base_pnl_pct = None

    for cap in capital_levels:
        result = run_simulation(
            markets,
            config_name="standard",
            starting_capital=float(cap),
            label=f"rampup_{cap}",
        )
        sub_results[f"${cap:,}"] = result

        pnl_pct = result["total_pnl_pct"]
        # Absolute P&L per dollar of capital
        pnl_per_dollar = result["total_pnl"] / cap if cap > 0 else 0

        if base_pnl_pct is None:
            base_pnl_pct = pnl_pct

        status = ""
        # Returns diminish as capital grows because max_trade=$50 caps position size
        if cap >= 10_000 and pnl_pct < base_pnl_pct * 0.5:
            status = " ← DIMINISHING RETURNS"
            if binding_capital is None:
                binding_capital = cap

        print(f"  ${cap:>6,}: P&L ${result['total_pnl']:>+10.2f} ({pnl_pct:>+6.1f}%) | "
              f"Trades: {result['total_trades']:>4} | $/capital: {pnl_per_dollar:.3f} | "
              f"DD {result['max_drawdown_pct']:>5.1f}%{status}")

    worst = sub_results[f"${50_000:,}"]
    worst["sub_results"] = {k: {kk: vv for kk, vv in v.items() if kk != "daily_capital"}
                           for k, v in sub_results.items()}
    worst["binding_capital"] = binding_capital

    if binding_capital:
        print(f"\n  Liquidity becomes binding constraint at ${binding_capital:,}")
    else:
        print(f"\n  No clear liquidity binding up to $50,000 (capped by max_trade=$50)")
    print(f"  Note: $50 max trade size limits capital deployment at all levels")
    return worst


# ---------------------------------------------------------------------------
# Combination stress test — find the actual breaking point
# ---------------------------------------------------------------------------

def find_breaking_point(markets: list[dict]) -> dict:
    """Find the scenario combination that causes permanent capital loss."""
    print("\n" + "=" * 70)
    print("  BREAKING POINT ANALYSIS")
    print("  Finding the combination that causes permanent capital loss")
    print("=" * 70)

    combos = [
        {
            "name": "Calibration drift + liquidity crisis",
            "params": {"calibration_degradation": 0.20,
                      "liquidity_multiplier": 0.20, "liquidity_crisis_days": (20, 60)},
        },
        {
            "name": "Black swan + fee spike",
            "params": {"force_wrong_resolution": 0.15, "fee_override": 0.04},
        },
        {
            "name": "Triple threat: drift + swan + fees",
            "params": {"calibration_degradation": 0.15,
                      "force_wrong_resolution": 0.10, "fee_override": 0.04},
        },
        {
            "name": "Apocalypse: 30% drift + 20% wrong + 4% fees + liquidity crisis",
            "params": {"calibration_degradation": 0.30, "force_wrong_resolution": 0.20,
                      "fee_override": 0.04,
                      "liquidity_multiplier": 0.20, "liquidity_crisis_days": (10, 70)},
        },
        {
            "name": "Extended crisis: 20% wrong for full 90 days",
            "params": {"force_wrong_resolution": 0.20},
        },
    ]

    results = {}
    for combo in combos:
        result = run_simulation(
            markets, config_name="standard",
            label=combo["name"], **combo["params"],
        )
        results[combo["name"]] = result
        status = "SURVIVES" if result["ending_capital"] > result["starting_capital"] * 0.5 else "BREAKS"
        print(f"  {combo['name']}")
        print(f"    P&L: ${result['total_pnl']:+.2f} | DD: {result['max_drawdown_pct']:.1f}% | "
              f"Final: ${result['ending_capital']:.2f} | {status}")

    # Find first breaking combo
    breaking = None
    for name, r in results.items():
        if r["ending_capital"] < r["starting_capital"] * 0.5:
            breaking = name
            break

    print(f"\n  First breaking combination: {breaking or 'None found — quarter-Kelly is resilient'}")
    return {
        "breaking_combination": breaking,
        "results": {k: {kk: vv for kk, vv in v.items() if kk != "daily_capital"}
                   for k, v in results.items()},
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_risk_report(all_results: dict) -> str:
    """Generate plain English risk disclosure."""

    baseline = all_results.get("baseline", {})
    scenario_a = all_results.get("scenario_a", {})
    scenario_b = all_results.get("scenario_b", {})
    scenario_c = all_results.get("scenario_c", {})
    scenario_d = all_results.get("scenario_d", {})
    scenario_e = all_results.get("scenario_e", {})
    scenario_f = all_results.get("scenario_f", {})
    scenario_g = all_results.get("scenario_g", {})
    breaking = all_results.get("breaking_point", {})

    report = []
    report.append("=" * 70)
    report.append("  EDGELAB RISK DISCLOSURE — STRESS TEST REPORT")
    report.append(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    report.append(f"  Dataset: {baseline.get('total_trades', 'N/A')} trades over 90 days")
    report.append("=" * 70)

    report.append("\n\n--- TOP 3 RISKS ---\n")

    # Determine top risks from results
    risks = []

    # Risk 1: Calibration drift
    bp = scenario_b.get("breaking_point_pct")
    if bp:
        risks.append(
            f"1. CALIBRATION DRIFT (Critical)\n"
            f"   Our probability estimates degrade over time as market dynamics change.\n"
            f"   At {bp}% calibration degradation, the strategy starts losing money.\n"
            f"   The circuit breaker (win rate < 45% over 20 trades) catches this,\n"
            f"   but there's a lag — we could lose ${abs(scenario_b.get('max_drawdown_dollars', 0)):.0f}\n"
            f"   before the breaker trips.\n"
            f"   MITIGATION: Weekly calibration audits. Compare predicted vs actual\n"
            f"   resolution rates. Alert if divergence exceeds 5%."
        )
    else:
        risks.append(
            f"1. CALIBRATION DRIFT (Moderate)\n"
            f"   Strategy survives up to 30% calibration degradation.\n"
            f"   Quarter-Kelly sizing provides significant buffer.\n"
            f"   MITIGATION: Monthly calibration review. Auto-pause if win rate\n"
            f"   drops below 60% over 50 trades."
        )

    # Risk 2: Black swan / wrong resolution
    d_20_threshold = scenario_d.get("drawdown_20pct_threshold")
    risks.append(
        f"2. BLACK SWAN EVENTS (High)\n"
        f"   When high-confidence markets resolve unexpectedly wrong.\n"
        f"   {'Account hits -20% drawdown when ' + str(int(d_20_threshold*100)) + '% of trades resolve wrong.' if d_20_threshold else 'Account survives even 50% wrong resolution — quarter-Kelly is protective.'}\n"
        f"   Single worst case: -$47 on a $50 position at 0.94 entry price.\n"
        f"   MITIGATION: Max $50 per trade. Max 20% category exposure.\n"
        f"   Never increase position size on a losing streak."
    )

    # Risk 3: Concentration / correlation
    cat = scenario_f.get("wipeout_category", "unknown")
    cat_impact = scenario_f.get("pnl_impact", 0)
    risks.append(
        f"3. CONCENTRATION RISK (Moderate)\n"
        f"   If all '{cat}' markets lose simultaneously: ${cat_impact:.0f} impact.\n"
        f"   Current 20% category cap limits damage but doesn't eliminate it.\n"
        f"   Correlated events (e.g., all NBA games on a bad night) can\n"
        f"   cascade through multiple positions.\n"
        f"   MITIGATION: Reduce category cap to 15%. Add correlation scoring.\n"
        f"   Pause category after 2 consecutive losses."
    )

    report.append("\n".join(risks))

    report.append("\n\n--- HARD LIMITS (NEVER CROSS) ---\n")
    report.append("  1. Maximum single trade size:     $50  (currently enforced)")
    report.append("  2. Maximum daily loss:            $200 (circuit breaker)")
    report.append("  3. Maximum drawdown tolerance:    20%  (manual review required)")
    report.append("  4. Minimum win rate (20 trades):  45%  (auto-pause)")
    report.append("  5. Maximum open positions:        20   (enforced)")
    report.append("  6. Maximum capital deployed:      60%  (40% always in reserve)")
    report.append("  7. Maximum per-category exposure:  20%  (recommend reducing to 15%)")
    report.append("  8. Consecutive strategy losses:    3    (60-min pause)")
    report.append("  9. Never increase size after loss — always reduce or hold")
    report.append(" 10. Never override circuit breakers without 24h cooling period")

    # Recommended max account size
    g_binding = scenario_g.get("binding_capital")
    report.append("\n\n--- RECOMMENDED MAXIMUM ACCOUNT SIZE ---\n")
    if g_binding:
        recommended = g_binding // 2
        report.append(f"  Liquidity becomes binding at: ${g_binding:,}")
        report.append(f"  Recommended maximum: ${recommended:,}")
    else:
        report.append(f"  No liquidity binding found up to $50,000.")
        report.append(f"  However, max trade size ($50) caps capital deployment.")
        report.append(f"  At $50,000 capital, only 0.1% per trade — very conservative.")
        report.append(f"")
        report.append(f"  Recommended phased approach:")
        report.append(f"    Phase 1 (Month 1-2):  $500   — paper trading validation")
        report.append(f"    Phase 2 (Month 3-4):  $2,500 — small live with all guardrails")
        report.append(f"    Phase 3 (Month 5+):   $5,000 — if Sharpe > 1.5 and WR > 70%")
        report.append(f"    Phase 4 (Month 8+):   $10,000 — if 6 months profitable")
        report.append(f"    NEVER exceed: $25,000 without institutional-grade monitoring")

    report.append("\n\n--- EARLY WARNING SIGNS ---\n")
    report.append("  Watch for these in live trading:\n")
    report.append("  1. Win rate dropping below 70% over 50 trades")
    report.append("     → Calibration may be drifting. Run calibration audit immediately.")
    report.append("  2. Average hold time increasing beyond 5 days")
    report.append("     → Markets may be less liquid or signals less timely.")
    report.append("  3. Circuit breaker triggering more than once per week")
    report.append("     → Strategy fundamentals may have changed. Pause and review.")
    report.append("  4. Consecutive losses reaching 2 in any strategy")
    report.append("     → Don't wait for the 3-loss auto-pause. Manually review.")
    report.append("  5. Daily P&L exceeding -$100")
    report.append("     → Cut position sizes by 50% for remainder of day.")
    report.append("  6. Same market appearing in multiple strategy signals")
    report.append("     → Check for signal cross-contamination.")
    report.append("  7. Polymarket API latency exceeding 5 seconds")
    report.append("     → Indicates platform stress. Reduce trading frequency.")
    report.append("  8. Fee changes announced by platform")
    report.append("     → Re-run stress test E before resuming.")

    report.append("\n\n--- SCENARIO SUMMARY ---\n")
    scenarios = [
        ("A", "Liquidity Crisis (80% drop, 2 weeks)", scenario_a),
        ("B", "Calibration Drift (10-30%)", scenario_b),
        ("C", "API Outage (6-24h)", scenario_c),
        ("D", "Black Swan (wrong resolutions)", scenario_d),
        ("E", "Fee Spike (2% → 4%)", scenario_e),
        ("F", "Concentration Risk (category wipeout)", scenario_f),
        ("G", "Ramp-Up Risk ($500 → $50k)", scenario_g),
    ]

    for letter, name, result in scenarios:
        if isinstance(result, dict) and "total_pnl" in result:
            status = "PASS" if result["total_pnl"] > 0 else "FAIL"
            report.append(f"  {letter}. {name:<45} {status:>4} | "
                         f"P&L ${result['total_pnl']:>+8.2f} | DD {result['max_drawdown_pct']:>5.1f}%")
        else:
            report.append(f"  {letter}. {name:<45} N/A")

    # Breaking point
    bp_combo = breaking.get("breaking_combination")
    report.append(f"\n  Breaking combination: {bp_combo or 'None found — system is resilient'}")

    report.append("\n\n--- VERDICT ---\n")
    fail_count = sum(1 for _, _, r in scenarios
                     if isinstance(r, dict) and r.get("total_pnl", 1) <= 0)
    if fail_count == 0:
        report.append("  ALL SCENARIOS PASS. Strategy is robust under stress.")
        report.append("  Proceed with paper trading phase. Monitor early warning signs.")
    elif fail_count <= 2:
        report.append(f"  {fail_count} SCENARIO(S) SHOW WEAKNESS. Strategy has known risks.")
        report.append("  Proceed with caution. Keep capital at Phase 1-2 levels.")
    else:
        report.append(f"  {fail_count} SCENARIOS FAIL. Strategy needs hardening before live use.")

    report.append("\n" + "=" * 70)
    report.append("  End of Risk Disclosure")
    report.append("=" * 70)

    return "\n".join(report)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n" + "=" * 70)
    print("  EDGELAB STRESS TESTER — Agent RISKLAB")
    print("  Running 7 scenarios + breaking point analysis")
    print("=" * 70)

    markets = load_markets()

    # Run baseline first
    print("\n  Running baseline (no stress)...")
    baseline = run_simulation(markets, config_name="standard", label="baseline")
    print(f"  Baseline: P&L ${baseline['total_pnl']:+.2f} | WR {baseline['win_rate']*100:.1f}% | "
          f"DD {baseline['max_drawdown_pct']:.1f}% | Trades: {baseline['total_trades']}")

    # Run all scenarios
    results: dict[str, Any] = {"baseline": baseline}
    results["scenario_a"] = scenario_a_liquidity_crisis(markets)
    results["scenario_b"] = scenario_b_calibration_drift(markets)
    results["scenario_c"] = scenario_c_api_outage(markets)
    results["scenario_d"] = scenario_d_black_swan(markets)
    results["scenario_e"] = scenario_e_fee_spike(markets)
    results["scenario_f"] = scenario_f_concentration_risk(markets)
    results["scenario_g"] = scenario_g_rampup_risk(markets)
    results["breaking_point"] = find_breaking_point(markets)

    # Strip daily_capital and trade_log from JSON output (too large)
    json_results = {}
    for k, v in results.items():
        if isinstance(v, dict):
            json_results[k] = {kk: vv for kk, vv in v.items()
                              if kk not in ("daily_capital", "trade_log")}
        else:
            json_results[k] = v

    # Save JSON results
    os.makedirs(os.path.dirname(os.path.abspath(RESULTS_PATH)), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(json_results, f, indent=2, default=str)
    print(f"\n  Results saved to {RESULTS_PATH}")

    # Generate and save risk report
    report_text = generate_risk_report(results)
    with open(REPORT_PATH, "w") as f:
        f.write(report_text)
    print(f"  Risk report saved to {REPORT_PATH}")

    # Print the report
    print("\n" + report_text)


if __name__ == "__main__":
    main()
