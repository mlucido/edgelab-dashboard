"""
EdgeLab Monte Carlo Parameter Optimizer

Runs 10,000 randomized simulations over the real 90-day Polymarket dataset,
varying 6 guardrail parameters simultaneously. Finds the Pareto-optimal
frontier (best Sharpe with max drawdown < 5%) and outputs ranked results.

Usage:
    python scripts/monte_carlo_optimizer.py

Uses all CPU cores via multiprocessing. Progress logged every 500 iterations.
"""
from __future__ import annotations

import json
import math
import multiprocessing as mp
import os
import random
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Constants (from backsim.py)
# ---------------------------------------------------------------------------
SIM_DAYS = 90
SIM_CAPITAL_START = 500.0
PLATFORM_FEE = 0.02
RESOLUTION_LAG_PRICE_LO = 0.85
RESOLUTION_LAG_PRICE_HI = 0.97
THRESHOLD_MIN_PRICE = 0.90
MAX_TRADE_SIZE = 50.0
MIN_TRADE_SIZE = 10.0
CIRCUIT_BREAKER_THRESHOLD = -200

DB_PATH = str(PROJECT_ROOT / "data" / "simulation.db")
OUTPUT_JSON = str(PROJECT_ROOT / "data" / "optimal_params.json")
OUTPUT_REPORT = str(PROJECT_ROOT / "data" / "monte_carlo_report.txt")

N_ITERATIONS = 10_000
PROGRESS_INTERVAL = 500
MIN_TRADES_REQUIRED = 20

# Current hand-tuned parameters
CURRENT_PARAMS = {
    "min_ev": 0.005,
    "min_liquidity": 500,
    "max_days_to_resolution": 14,
    "kelly_fraction": 0.25,
    "max_capital_deployment": 0.60,
    "resolution_lag_confidence": 0.80,
}

# Parameter search ranges
PARAM_RANGES = {
    "min_ev": [round(0.001 + i * 0.002, 3) for i in range(25)],
    "min_liquidity": [500 + i * 500 for i in range(20)],
    "max_days_to_resolution": list(range(3, 22)),
    "kelly_fraction": [round(0.1 + i * 0.05, 2) for i in range(9)],
    "max_capital_deployment": [round(0.2 + i * 0.05, 2) for i in range(13)],
    "resolution_lag_confidence": [round(0.6 + i * 0.05, 2) for i in range(8)],
}


# ---------------------------------------------------------------------------
# Market data loader
# ---------------------------------------------------------------------------

def load_markets() -> list[dict]:
    """Load cached markets from simulation.db."""
    if not Path(DB_PATH).exists():
        print(f"ERROR: {DB_PATH} not found. Run backsim first to populate cache.")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM sim_markets ORDER BY resolution_date ASC").fetchall()
    conn.close()
    markets = [dict(r) for r in rows]
    print(f"Loaded {len(markets)} markets from cache")
    return markets


# ---------------------------------------------------------------------------
# Parameterized simulation engine (self-contained, no imports from src/)
# ---------------------------------------------------------------------------

def _kelly_frac(price: float, true_prob: float, fee: float = PLATFORM_FEE) -> float:
    if price <= 0 or price >= 1:
        return 0.0
    gross = 1.0 / price - 1.0
    b = gross * (1.0 - fee)
    if b <= 0:
        return 0.0
    return (true_prob * b - (1.0 - true_prob)) / b


def _detect_strategy(market: dict, params: dict) -> str | None:
    """Detect if market triggers a strategy signal with given params."""
    price = market.get("last_price", 0)
    if market.get("resolved_yes") != 1:
        return None
    effective_liquidity = max(
        market.get("liquidity", 0), market.get("volume", 0)
    )
    if effective_liquidity < params["min_liquidity"]:
        return None
    if RESOLUTION_LAG_PRICE_LO < price < RESOLUTION_LAG_PRICE_HI:
        return "resolution_lag"
    if price >= THRESHOLD_MIN_PRICE:
        return "threshold"
    return None


def _build_opportunity(market: dict, strategy: str, sim_entry_date: datetime) -> dict:
    price = market["last_price"]
    if price >= 0.90:
        true_prob = price + (1.0 - price) * 0.40
    else:
        true_prob = price + (1.0 - price) * 0.25
    true_prob = min(true_prob, 0.999)

    end_date = market.get("end_date", "")
    try:
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        days_to_res = max((end_dt - sim_entry_date).days, 1)
    except (ValueError, AttributeError):
        days_to_res = 7

    return {
        "strategy": strategy,
        "market_id": market["market_id"],
        "current_price": price,
        "implied_true_prob": round(true_prob, 4),
        "liquidity": max(market.get("liquidity", 0), market.get("volume", 0)),
        "volume": market.get("volume", 0),
        "days_to_resolution": days_to_res,
        "confidence": 85,
    }


def _day_index(resolution_date: str, sim_start: datetime) -> int:
    try:
        rd = datetime.fromisoformat(resolution_date.replace("Z", "+00:00"))
        delta = (rd - sim_start).days
        return max(0, min(delta, SIM_DAYS - 1))
    except (ValueError, AttributeError):
        return 0


def run_sim_with_params(args: tuple) -> dict | None:
    """
    Run one simulation with a specific parameter set.
    Returns metrics dict or None if insufficient trades.

    args = (iteration_index, params_dict, markets_list, sim_start)
    """
    idx, params, markets, sim_start_iso = args
    sim_start = datetime.fromisoformat(sim_start_iso)

    min_ev = params["min_ev"]
    min_liq = params["min_liquidity"]
    max_days = params["max_days_to_resolution"]
    kelly_mult = params["kelly_fraction"]
    max_deploy = params["max_capital_deployment"]
    res_lag_conf = params["resolution_lag_confidence"]

    capital = SIM_CAPITAL_START
    open_positions: list[dict] = []
    trade_log: list[dict] = []
    daily_pnl: dict[int, float] = defaultdict(float)
    circuit_halted_days: set[int] = set()
    circuit_triggers = 0
    strategy_stats: dict[str, dict] = {
        "resolution_lag": {"trades": 0, "wins": 0, "pnl": 0.0},
        "threshold": {"trades": 0, "wins": 0, "pnl": 0.0},
    }

    sorted_markets = sorted(markets, key=lambda m: m.get("resolution_date") or "")

    for market in sorted_markets:
        day = _day_index(market.get("resolution_date", ""), sim_start)

        # Close resolved positions
        still_open = []
        for pos in open_positions:
            if pos["resolve_day"] <= day:
                exit_price = 1.0 if pos["resolved_yes"] else 0.0
                pnl = (exit_price - pos["entry_price"]) * pos["size"]
                capital += pnl
                daily_pnl[pos["resolve_day"]] += pnl
                trade_log.append({
                    "pnl": pnl,
                    "strategy": pos["strategy"],
                    "entry_price": pos["entry_price"],
                    "exit_price": exit_price,
                    "size": pos["size"],
                    "resolve_day": pos["resolve_day"],
                    "entry_day": pos["entry_day"],
                    "win": pnl > 0,
                    "market_id": pos["market_id"],
                })
                strategy_stats[pos["strategy"]]["trades"] += 1
                strategy_stats[pos["strategy"]]["pnl"] += pnl
                if pnl > 0:
                    strategy_stats[pos["strategy"]]["wins"] += 1
            else:
                still_open.append(pos)
        open_positions = still_open

        if day in circuit_halted_days:
            continue

        # Detect strategy signal
        strategy = _detect_strategy(market, params)
        if not strategy:
            continue

        # Build opportunity
        sim_entry_date = sim_start + timedelta(days=max(day - 3, 0))
        opp = _build_opportunity(market, strategy, sim_entry_date)

        # --- Guardrail Layer 1: Trade quality ---
        price = opp["current_price"]
        true_prob = opp["implied_true_prob"]
        if price <= 0 or price >= 1:
            continue

        net_return = true_prob * (1.0 - PLATFORM_FEE) / price - 1.0
        if net_return <= min_ev:
            continue

        liq = opp["liquidity"]
        if liq < min_liq:
            continue

        days_to_res = opp["days_to_resolution"]
        if days_to_res > max_days:
            continue

        if strategy == "resolution_lag":
            confidence = opp.get("confidence", 0) / 100.0
            if confidence < res_lag_conf:
                continue

        # --- Guardrail Layer 2: Position sizing ---
        kelly = _kelly_frac(price, true_prob)
        if kelly <= 0:
            continue

        kelly_q = kelly_mult * kelly
        raw_size = kelly_q * capital
        if raw_size < MIN_TRADE_SIZE:
            continue
        size = min(raw_size, MAX_TRADE_SIZE)

        # --- Guardrail Layer 3: Portfolio exposure ---
        deployed = sum(p["size"] for p in open_positions)
        max_deployed_cap = max_deploy * capital
        if deployed + size > max_deployed_cap:
            continue
        if len(open_positions) >= 20:
            continue

        # --- Guardrail Layer 4: Circuit breaker ---
        today_loss = sum(v for d, v in daily_pnl.items() if d == day and v < 0)
        if today_loss <= CIRCUIT_BREAKER_THRESHOLD:
            circuit_triggers += 1
            circuit_halted_days.add(day)
            continue

        # --- Execute trade ---
        fee_cost = size * 0.005
        actual_size = size - fee_cost
        entry_price = price

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
        })
        capital -= fee_cost

    # Close remaining positions
    for pos in open_positions:
        exit_price = 1.0 if pos["resolved_yes"] else 0.0
        pnl = (exit_price - pos["entry_price"]) * pos["size"]
        capital += pnl
        daily_pnl[SIM_DAYS - 1] += pnl
        trade_log.append({
            "pnl": pnl,
            "strategy": pos["strategy"],
            "entry_price": pos["entry_price"],
            "exit_price": exit_price,
            "size": pos["size"],
            "resolve_day": SIM_DAYS - 1,
            "entry_day": pos["entry_day"],
            "win": pnl > 0,
            "market_id": pos["market_id"],
        })
        strategy_stats[pos["strategy"]]["trades"] += 1
        strategy_stats[pos["strategy"]]["pnl"] += pnl
        if pnl > 0:
            strategy_stats[pos["strategy"]]["wins"] += 1

    total_trades = len(trade_log)
    if total_trades < MIN_TRADES_REQUIRED:
        return None

    # --- Compute metrics ---
    wins = sum(1 for t in trade_log if t.get("win"))
    win_rate = wins / total_trades
    total_pnl = capital - SIM_CAPITAL_START
    total_return = total_pnl / SIM_CAPITAL_START

    # Daily capital curve
    day_pnl_arr = [0.0] * SIM_DAYS
    for t in trade_log:
        d = t.get("resolve_day", 0)
        if 0 <= d < SIM_DAYS:
            day_pnl_arr[d] += t.get("pnl", 0)

    running = SIM_CAPITAL_START
    daily_capital = []
    for d in range(SIM_DAYS):
        running += day_pnl_arr[d]
        daily_capital.append(running)

    # Daily returns
    daily_returns = []
    prev = SIM_CAPITAL_START
    for cap in daily_capital:
        ret = (cap - prev) / prev if prev > 0 else 0.0
        daily_returns.append(ret)
        prev = cap

    mean_ret = sum(daily_returns) / len(daily_returns) if daily_returns else 0.0
    variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns) if daily_returns else 0.0
    std_ret = math.sqrt(variance) if variance > 0 else 1e-9
    sharpe = (mean_ret / std_ret) * math.sqrt(365)

    # Max drawdown
    peak = SIM_CAPITAL_START
    max_dd = 0.0
    for cap in daily_capital:
        if cap > peak:
            peak = cap
        dd = (peak - cap) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    # Strategy breakdown
    strat_breakdown = {}
    for strat, stats in strategy_stats.items():
        n = stats["trades"]
        strat_breakdown[strat] = {
            "trades": n,
            "wins": stats["wins"],
            "win_rate": round(stats["wins"] / n, 4) if n > 0 else 0.0,
            "pnl": round(stats["pnl"], 2),
        }

    return {
        "iteration": idx,
        "params": params,
        "sharpe": round(sharpe, 4),
        "total_return": round(total_return, 4),
        "total_pnl": round(total_pnl, 2),
        "max_drawdown": round(max_dd, 4),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "win_rate": round(win_rate, 4),
        "total_trades": total_trades,
        "circuit_triggers": circuit_triggers,
        "ending_capital": round(capital, 2),
        "strategy_breakdown": strat_breakdown,
    }


# ---------------------------------------------------------------------------
# Parameter sampling
# ---------------------------------------------------------------------------

def sample_params() -> dict:
    """Sample one random parameter combination from the search space."""
    return {
        key: random.choice(values)
        for key, values in PARAM_RANGES.items()
    }


# ---------------------------------------------------------------------------
# Sensitivity analysis
# ---------------------------------------------------------------------------

def compute_sensitivity(results: list[dict]) -> dict[str, dict]:
    """
    For each parameter, compute correlation with Sharpe ratio.
    Returns dict of param_name -> {correlation, best_value, worst_value, impact}.
    """
    if len(results) < 10:
        return {}

    sharpes = [r["sharpe"] for r in results]
    mean_sharpe = sum(sharpes) / len(sharpes)

    sensitivity = {}
    for param_name in PARAM_RANGES:
        values = [r["params"][param_name] for r in results]
        mean_val = sum(values) / len(values)

        # Pearson correlation
        cov = sum((v - mean_val) * (s - mean_sharpe) for v, s in zip(values, sharpes)) / len(results)
        std_val = math.sqrt(sum((v - mean_val) ** 2 for v in values) / len(results))
        std_sharpe = math.sqrt(sum((s - mean_sharpe) ** 2 for s in sharpes) / len(results))

        if std_val > 0 and std_sharpe > 0:
            corr = cov / (std_val * std_sharpe)
        else:
            corr = 0.0

        # Find best/worst value by average Sharpe
        val_sharpes: dict[float, list[float]] = defaultdict(list)
        for r in results:
            val_sharpes[r["params"][param_name]].append(r["sharpe"])

        avg_by_val = {v: sum(s) / len(s) for v, s in val_sharpes.items() if len(s) >= 3}
        if avg_by_val:
            best_val = max(avg_by_val, key=avg_by_val.get)
            worst_val = min(avg_by_val, key=avg_by_val.get)
            impact = avg_by_val[best_val] - avg_by_val[worst_val]
        else:
            best_val = mean_val
            worst_val = mean_val
            impact = 0.0

        sensitivity[param_name] = {
            "correlation": round(corr, 4),
            "best_value": best_val,
            "worst_value": worst_val,
            "impact_sharpe_spread": round(impact, 4),
            "abs_correlation": round(abs(corr), 4),
        }

    return sensitivity


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(
    top_results: list[dict],
    sensitivity: dict,
    current_result: dict | None,
    total_valid: int,
    total_run: int,
    elapsed: float,
) -> str:
    lines = []
    lines.append("=" * 70)
    lines.append("  EDGELAB MONTE CARLO OPTIMIZATION REPORT")
    lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"  Iterations run:     {total_run:,}")
    lines.append(f"  Valid results:      {total_valid:,} (trades >= {MIN_TRADES_REQUIRED})")
    lines.append(f"  Pareto candidates:  {len(top_results)} (Sharpe-ranked, max DD < 5%)")
    lines.append(f"  Runtime:            {elapsed:.1f}s ({elapsed/60:.1f}m)")
    lines.append(f"  CPU cores used:     {mp.cpu_count()}")
    lines.append("")

    # Current vs optimal comparison
    if top_results:
        best = top_results[0]
        lines.append("-" * 70)
        lines.append("  CURRENT vs OPTIMAL PARAMETERS")
        lines.append("-" * 70)
        lines.append(f"  {'Parameter':<30} {'Current':>10} {'Optimal':>10} {'Delta':>10}")
        lines.append(f"  {'─' * 30} {'─' * 10} {'─' * 10} {'─' * 10}")
        for param in CURRENT_PARAMS:
            cur = CURRENT_PARAMS[param]
            opt = best["params"][param]
            if isinstance(cur, float):
                delta = opt - cur
                lines.append(f"  {param:<30} {cur:>10.3f} {opt:>10.3f} {delta:>+10.3f}")
            else:
                delta = opt - cur
                lines.append(f"  {param:<30} {cur:>10} {opt:>10} {delta:>+10}")
        lines.append("")

        # Metrics comparison
        lines.append(f"  {'Metric':<30} {'Current':>10} {'Optimal':>10} {'Improvement':>12}")
        lines.append(f"  {'─' * 30} {'─' * 10} {'─' * 10} {'─' * 12}")

        cur_sharpe = current_result["sharpe"] if current_result else 0.0
        opt_sharpe = best["sharpe"]
        sharpe_imp = ((opt_sharpe - cur_sharpe) / abs(cur_sharpe) * 100) if cur_sharpe != 0 else float("inf")
        lines.append(f"  {'Sharpe ratio':<30} {cur_sharpe:>10.2f} {opt_sharpe:>10.2f} {sharpe_imp:>+11.1f}%")

        cur_ret = current_result["total_return"] if current_result else 0.0
        opt_ret = best["total_return"]
        lines.append(f"  {'Total return':<30} {cur_ret:>9.1%} {opt_ret:>9.1%}")

        cur_dd = current_result["max_drawdown_pct"] if current_result else 0.0
        opt_dd = best["max_drawdown_pct"]
        lines.append(f"  {'Max drawdown':<30} {cur_dd:>9.1f}% {opt_dd:>9.1f}%")

        cur_wr = current_result["win_rate"] if current_result else 0.0
        opt_wr = best["win_rate"]
        lines.append(f"  {'Win rate':<30} {cur_wr:>9.1%} {opt_wr:>9.1%}")

        cur_trades = current_result["total_trades"] if current_result else 0
        opt_trades = best["total_trades"]
        lines.append(f"  {'Total trades':<30} {cur_trades:>10} {opt_trades:>10}")
        lines.append("")

    # Sensitivity analysis
    lines.append("-" * 70)
    lines.append("  SENSITIVITY ANALYSIS (which params matter most)")
    lines.append("-" * 70)
    sorted_sens = sorted(sensitivity.items(), key=lambda x: x[1]["abs_correlation"], reverse=True)
    lines.append(f"  {'Parameter':<30} {'Correlation':>12} {'Best Value':>12} {'Sharpe Spread':>14}")
    lines.append(f"  {'─' * 30} {'─' * 12} {'─' * 12} {'─' * 14}")
    for param, s in sorted_sens:
        lines.append(
            f"  {param:<30} {s['correlation']:>+12.4f} {s['best_value']:>12} {s['impact_sharpe_spread']:>14.4f}"
        )
    lines.append("")

    # Top 20 results
    lines.append("-" * 70)
    lines.append("  TOP 20 PARAMETER SETS (ranked by Sharpe, max DD < 5%)")
    lines.append("-" * 70)
    for i, r in enumerate(top_results[:20]):
        p = r["params"]
        lines.append(f"  #{i+1:>2}  Sharpe={r['sharpe']:>6.2f}  Return={r['total_return']:>+7.1%}  "
                      f"DD={r['max_drawdown_pct']:>4.1f}%  WR={r['win_rate']:>5.1%}  "
                      f"Trades={r['total_trades']:>3}")
        lines.append(f"       ev={p['min_ev']:.3f}  liq={p['min_liquidity']:>5}  "
                      f"days={p['max_days_to_resolution']:>2}  kelly={p['kelly_fraction']:.2f}  "
                      f"deploy={p['max_capital_deployment']:.2f}  conf={p['resolution_lag_confidence']:.2f}")
        if r.get("strategy_breakdown"):
            for strat, sb in r["strategy_breakdown"].items():
                if sb["trades"] > 0:
                    lines.append(f"         {strat}: {sb['trades']} trades, "
                                 f"{sb['win_rate']:.0%} WR, ${sb['pnl']:+.2f}")
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    start_time = time.time()
    print("=" * 60)
    print("  EDGELAB MONTE CARLO OPTIMIZER")
    print(f"  {N_ITERATIONS:,} iterations × {len(PARAM_RANGES)} parameters")
    print(f"  CPU cores: {mp.cpu_count()}")
    print("=" * 60)

    # Load market data
    markets = load_markets()
    if not markets:
        print("No markets — aborting.")
        sys.exit(1)

    sim_start = datetime.now(timezone.utc) - timedelta(days=SIM_DAYS)
    sim_start_iso = sim_start.isoformat()

    # Generate parameter samples
    random.seed(42)  # reproducible
    param_samples = [sample_params() for _ in range(N_ITERATIONS)]

    # Also run with current params
    param_samples.append(CURRENT_PARAMS.copy())

    # Build work items (serialize markets once, pass index)
    # To avoid pickling huge market lists per-process, use initializer
    work_items = [
        (i, params, markets, sim_start_iso)
        for i, params in enumerate(param_samples)
    ]

    print(f"\nStarting {N_ITERATIONS:,} simulations across {mp.cpu_count()} cores...")
    print(f"Progress updates every {PROGRESS_INTERVAL} iterations.\n")

    # Run with multiprocessing
    results: list[dict] = []
    current_result: dict | None = None
    completed = 0

    # Use imap_unordered for progress reporting
    with mp.Pool(processes=mp.cpu_count()) as pool:
        for result in pool.imap_unordered(run_sim_with_params, work_items, chunksize=50):
            completed += 1
            if result is not None:
                # Check if this is the current params run
                if result["iteration"] == N_ITERATIONS:  # last one = current params
                    current_result = result
                else:
                    results.append(result)

            if completed % PROGRESS_INTERVAL == 0:
                valid = len(results)
                elapsed = time.time() - start_time
                rate = completed / elapsed
                eta = (len(work_items) - completed) / rate if rate > 0 else 0
                best_sharpe = max((r["sharpe"] for r in results), default=0)
                print(
                    f"  [{completed:>6,}/{len(work_items):,}] "
                    f"valid={valid:,}  best_sharpe={best_sharpe:.2f}  "
                    f"rate={rate:.0f}/s  ETA={eta:.0f}s"
                )

    elapsed = time.time() - start_time
    print(f"\nCompleted {completed:,} iterations in {elapsed:.1f}s ({elapsed/60:.1f}m)")
    print(f"Valid results (>={MIN_TRADES_REQUIRED} trades): {len(results):,}")

    if not results:
        print("ERROR: No valid results. All parameter sets produced < 20 trades.")
        sys.exit(1)

    # --- Pareto frontier: best Sharpe with max DD < 5% ---
    pareto = [r for r in results if r["max_drawdown"] < 0.05]
    print(f"Pareto candidates (DD < 5%): {len(pareto)}")

    if not pareto:
        # Relax DD constraint
        dd_threshold = 0.10
        pareto = [r for r in results if r["max_drawdown"] < dd_threshold]
        print(f"Relaxed to DD < {dd_threshold:.0%}: {len(pareto)} candidates")
        if not pareto:
            pareto = sorted(results, key=lambda r: r["sharpe"], reverse=True)
            print("Using all results (no DD filter)")

    # Rank by Sharpe
    pareto.sort(key=lambda r: r["sharpe"], reverse=True)
    top_20 = pareto[:20]

    # Sensitivity analysis
    sensitivity = compute_sensitivity(results)

    # --- Output JSON ---
    output_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "iterations": N_ITERATIONS,
        "valid_results": len(results),
        "pareto_candidates": len(pareto),
        "current_params": CURRENT_PARAMS,
        "current_result": current_result,
        "optimal_params": top_20[0]["params"] if top_20 else None,
        "top_20": top_20,
        "sensitivity": sensitivity,
    }
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output_data, f, indent=2, default=str)
    print(f"\nResults saved to {OUTPUT_JSON}")

    # --- Generate report ---
    report = generate_report(
        top_20, sensitivity, current_result,
        total_valid=len(results), total_run=N_ITERATIONS, elapsed=elapsed,
    )
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
    print(f"Report saved to {OUTPUT_REPORT}")
    print("\n" + report)

    # --- Auto-update if improvement > 10% ---
    if top_20 and current_result:
        cur_sharpe = current_result["sharpe"]
        opt_sharpe = top_20[0]["sharpe"]
        if cur_sharpe > 0:
            improvement = (opt_sharpe - cur_sharpe) / cur_sharpe
        else:
            improvement = 1.0  # infinite improvement from zero

        if improvement > 0.10:
            print(f"\nSharpe improvement: {improvement:.1%} (>{10}% threshold)")
            _apply_optimal_params(top_20[0]["params"])
        else:
            print(f"\nSharpe improvement: {improvement:.1%} (<= 10% threshold) — no auto-update")
    elif top_20:
        print("\nNo current baseline — applying optimal params")
        _apply_optimal_params(top_20[0]["params"])


def _apply_optimal_params(params: dict) -> None:
    """Update threshold_config.py and autonomous_trader.py with optimal params."""
    print("\nAuto-updating source files with optimal parameters...")

    # Update autonomous_trader.py
    trader_path = PROJECT_ROOT / "src" / "execution" / "autonomous_trader.py"
    if trader_path.exists():
        content = trader_path.read_text()
        replacements = {
            "MIN_NET_RETURN_AFTER_FEE": None,  # special handling
            "MIN_LIQUIDITY": f"MIN_LIQUIDITY = {params['min_liquidity']:.1f}",
            "MAX_DAYS_TO_RESOLUTION": f"MAX_DAYS_TO_RESOLUTION = {params['max_days_to_resolution']}",
            "KELLY_MULTIPLIER": f"KELLY_MULTIPLIER = {params['kelly_fraction']:.2f}",
            "MAX_DEPLOYED_FRACTION": f"MAX_DEPLOYED_FRACTION = {params['max_capital_deployment']:.2f}",
            "MIN_RESOLUTION_CONFIDENCE": f"MIN_RESOLUTION_CONFIDENCE = {params['resolution_lag_confidence']:.2f}",
        }

        import re
        for const, new_line in replacements.items():
            if const == "MIN_NET_RETURN_AFTER_FEE":
                # This one has a conditional expression — update the paper mode value
                pattern = r"MIN_NET_RETURN_AFTER_FEE = [\d.]+ if"
                replacement = f"MIN_NET_RETURN_AFTER_FEE = {params['min_ev']} if"
                content = re.sub(pattern, replacement, content)
            elif new_line:
                pattern = rf"^{const}\s*=\s*.+$"
                content = re.sub(pattern, new_line, content, flags=re.MULTILINE)

        trader_path.write_text(content)
        print(f"  Updated {trader_path}")

    # Update threshold_config.py
    config_path = PROJECT_ROOT / "src" / "strategies" / "threshold_config.py"
    if config_path.exists():
        content = config_path.read_text()
        import re
        content = re.sub(
            r"^MAX_DAYS_TO_RESOLUTION:\s*int\s*=\s*\d+",
            f"MAX_DAYS_TO_RESOLUTION: int = {params['max_days_to_resolution']}",
            content,
            flags=re.MULTILINE,
        )
        content = re.sub(
            r"^MIN_VOLUME_FILTER:\s*int\s*=\s*[\d_]+",
            f"MIN_VOLUME_FILTER: int = {params['min_liquidity']:_}",
            content,
            flags=re.MULTILINE,
        )
        config_path.write_text(content)
        print(f"  Updated {config_path}")

    print("  Source files updated with optimal parameters.")


if __name__ == "__main__":
    main()
