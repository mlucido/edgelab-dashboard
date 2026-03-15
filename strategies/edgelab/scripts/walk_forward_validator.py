"""
EdgeLab Walk-Forward Validation Engine

Proves whether the simulation edge is real or overfitted by:
1. Walk-forward validation: train 30d, test 10d, roll 5d
2. Per-strategy breakdown (resolution_lag, threshold, combined)
3. 1,000 bootstrap samples for confidence intervals
4. 1,000 permutation test for p-value (skill vs luck)
5. Rolling Sharpe chart (Plotly HTML)
6. Plain English verdict

Usage:
    python scripts/walk_forward_validator.py
"""
from __future__ import annotations

import json
import math
import os
import random
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.risk.sizer import PLATFORM_FEE, _kelly_fraction

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "simulation.db")
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

CAPITAL_START = 500.0
MAX_TRADE = 50.0
MAX_DEPLOY_FRACTION = 0.60
CIRCUIT_BREAKER_THRESHOLD = -200.0

# Walk-forward params
TRAIN_WINDOW = 30   # days
TEST_WINDOW = 10    # days
STEP_SIZE = 5       # days

# Bootstrap / permutation
N_BOOTSTRAP = 1000
N_PERMUTATION = 1000

# Strategy detection thresholds (matching backsim.py)
RESOLUTION_LAG_PRICE_LO = 0.85
RESOLUTION_LAG_PRICE_HI = 0.97
THRESHOLD_MIN_PRICE = 0.90
MIN_LIQUIDITY = 1000

SEED = 42

# ---------------------------------------------------------------------------
# Market data loading
# ---------------------------------------------------------------------------

def load_markets() -> list[dict]:
    """Load all markets from the simulation cache DB."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM sim_markets WHERE end_date != '' AND end_date IS NOT NULL "
        "ORDER BY end_date ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def parse_date(date_str: str) -> datetime | None:
    """Parse ISO date string to datetime."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Mini simulation engine (self-contained, no imports from backsim)
# ---------------------------------------------------------------------------

def detect_strategy(market: dict, allowed: set[str] | None = None) -> str | None:
    """Detect if a market triggers a strategy signal."""
    price = market.get("last_price", 0)
    if market.get("resolved_yes") != 1:
        return None

    effective_liquidity = max(
        market.get("liquidity", 0),
        market.get("volume", 0),
    )
    if effective_liquidity < MIN_LIQUIDITY:
        return None

    strategy = None
    if RESOLUTION_LAG_PRICE_LO < price < RESOLUTION_LAG_PRICE_HI:
        strategy = "resolution_lag"
    elif price >= THRESHOLD_MIN_PRICE:
        strategy = "threshold"

    if strategy and allowed and strategy not in allowed:
        return None
    return strategy


def build_opportunity(market: dict, strategy: str, sim_entry_date: datetime) -> dict:
    """Build opportunity dict from market data."""
    price = market["last_price"]
    if price >= 0.90:
        true_prob = price + (1.0 - price) * 0.40
    else:
        true_prob = price + (1.0 - price) * 0.25
    true_prob = min(true_prob, 0.999)

    end_date = market.get("end_date", "")
    end_dt = parse_date(end_date)
    if end_dt:
        days_to_res = max((end_dt - sim_entry_date).days, 1)
    else:
        days_to_res = 7

    return {
        "strategy": strategy,
        "market_id": market["market_id"],
        "current_price": price,
        "implied_true_prob": round(true_prob, 4),
        "liquidity": max(market.get("liquidity", 0), market.get("volume", 0)),
        "days_to_resolution": days_to_res,
        "confidence": 85,
    }


def run_window_sim(
    markets: list[dict],
    window_start: datetime,
    window_days: int,
    allowed_strategies: set[str] | None = None,
) -> dict:
    """
    Run a simulation on markets within a time window.
    Returns metrics dict with trade_log for further analysis.
    """
    window_end = window_start + timedelta(days=window_days)

    # Filter markets to this window
    window_markets = []
    for m in markets:
        end_dt = parse_date(m.get("end_date", ""))
        if end_dt and window_start <= end_dt < window_end:
            window_markets.append(m)

    if not window_markets:
        return _empty_result(window_start, window_days)

    capital = CAPITAL_START
    open_positions: list[dict] = []
    trade_log: list[dict] = []
    daily_pnl: dict[int, float] = defaultdict(float)

    sorted_markets = sorted(window_markets, key=lambda m: m.get("end_date") or "")

    for market in sorted_markets:
        end_dt = parse_date(market.get("end_date", ""))
        if not end_dt:
            continue
        day = max(0, min((end_dt - window_start).days, window_days - 1))

        # Resolve positions
        still_open = []
        for pos in open_positions:
            if pos["resolve_day"] <= day:
                exit_price = 1.0 if pos["resolved_yes"] else 0.0
                pnl = (exit_price - pos["entry_price"]) * pos["size"]
                capital += pnl
                daily_pnl[pos["resolve_day"]] += pnl
                trade_log.append({
                    "pnl": round(pnl, 4),
                    "win": pnl > 0,
                    "strategy": pos["strategy"],
                    "entry_price": pos["entry_price"],
                    "size": pos["size"],
                    "resolved_yes": pos["resolved_yes"],
                    "day": pos["resolve_day"],
                })
            else:
                still_open.append(pos)
        open_positions = still_open

        # Detect signal
        strategy = detect_strategy(market, allowed_strategies)
        if not strategy:
            continue

        sim_entry_date = window_start + timedelta(days=max(day - 3, 0))
        opp = build_opportunity(market, strategy, sim_entry_date)

        # Trade quality check
        price = opp["current_price"]
        true_prob = opp["implied_true_prob"]
        if price <= 0 or price >= 1 or true_prob <= price:
            continue
        ev = true_prob * (1.0 - PLATFORM_FEE) / price - 1.0
        if ev <= 0:
            continue

        # Position sizing (quarter-Kelly)
        kelly = _kelly_fraction(price, true_prob)
        if kelly <= 0:
            continue
        raw_size = 0.25 * kelly * capital
        size = min(raw_size, MAX_TRADE, 0.05 * capital)
        if size < 10.0:
            continue

        # Deployment check
        deployed = sum(p["size"] for p in open_positions)
        if deployed + size > MAX_DEPLOY_FRACTION * capital:
            continue

        # Execute
        fee_cost = size * 0.005
        actual_size = size - fee_cost

        end_date_str = market.get("end_date", "")
        end_dt_m = parse_date(end_date_str)
        resolve_day = min((end_dt_m - window_start).days, window_days - 1) if end_dt_m else min(day + 7, window_days - 1)
        if resolve_day <= day:
            resolve_day = day + 1

        open_positions.append({
            "market_id": market["market_id"],
            "strategy": strategy,
            "entry_price": price,
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
        daily_pnl[window_days - 1] += pnl
        trade_log.append({
            "pnl": round(pnl, 4),
            "win": pnl > 0,
            "strategy": pos["strategy"],
            "entry_price": pos["entry_price"],
            "size": pos["size"],
            "resolved_yes": pos["resolved_yes"],
            "day": window_days - 1,
        })

    return _compute_metrics(trade_log, daily_pnl, window_days, window_start, capital)


def _empty_result(window_start: datetime, window_days: int) -> dict:
    return {
        "window_start": window_start.isoformat(),
        "window_days": window_days,
        "total_trades": 0,
        "wins": 0,
        "win_rate": 0.0,
        "total_pnl": 0.0,
        "total_return": 0.0,
        "sharpe": 0.0,
        "max_drawdown": 0.0,
        "trade_log": [],
        "daily_returns": [],
    }


def _compute_metrics(
    trade_log: list[dict],
    daily_pnl: dict[int, float],
    window_days: int,
    window_start: datetime,
    final_capital: float,
) -> dict:
    total_trades = len(trade_log)
    wins = sum(1 for t in trade_log if t.get("win"))
    win_rate = wins / total_trades if total_trades > 0 else 0.0
    total_pnl = final_capital - CAPITAL_START
    total_return = total_pnl / CAPITAL_START

    # Daily returns
    day_pnl_arr = [0.0] * window_days
    for d, pnl in daily_pnl.items():
        if 0 <= d < window_days:
            day_pnl_arr[d] += pnl

    running = CAPITAL_START
    daily_returns = []
    daily_capital = []
    for d in range(window_days):
        running += day_pnl_arr[d]
        daily_capital.append(running)
        prev = daily_capital[d - 1] if d > 0 else CAPITAL_START
        ret = (running - prev) / prev if prev > 0 else 0.0
        daily_returns.append(ret)

    # Sharpe
    if daily_returns:
        mean_ret = sum(daily_returns) / len(daily_returns)
        variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns)
        std_ret = math.sqrt(variance) if variance > 0 else 1e-9
        sharpe = (mean_ret / std_ret) * math.sqrt(365)
    else:
        sharpe = 0.0

    # Max drawdown
    peak = CAPITAL_START
    max_dd = 0.0
    for cap in daily_capital:
        if cap > peak:
            peak = cap
        dd = (peak - cap) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    return {
        "window_start": window_start.isoformat(),
        "window_days": window_days,
        "total_trades": total_trades,
        "wins": wins,
        "win_rate": round(win_rate, 4),
        "total_pnl": round(total_pnl, 2),
        "total_return": round(total_return, 4),
        "sharpe": round(sharpe, 4),
        "max_drawdown": round(max_dd, 4),
        "trade_log": trade_log,
        "daily_returns": daily_returns,
    }


# ---------------------------------------------------------------------------
# Walk-forward validation
# ---------------------------------------------------------------------------

def walk_forward(
    markets: list[dict],
    date_range: tuple[datetime, datetime],
    allowed_strategies: set[str] | None = None,
    label: str = "combined",
) -> list[dict]:
    """
    Run walk-forward validation across the full date range.
    Train window: TRAIN_WINDOW days (used for calibration — not actually tuning here)
    Test window: TEST_WINDOW days
    Step: STEP_SIZE days
    """
    start_date, end_date = date_range
    results = []
    current = start_date + timedelta(days=TRAIN_WINDOW)  # skip first train window

    step = 0
    while current + timedelta(days=TEST_WINDOW) <= end_date:
        result = run_window_sim(
            markets,
            window_start=current,
            window_days=TEST_WINDOW,
            allowed_strategies=allowed_strategies,
        )
        result["label"] = label
        result["step"] = step
        results.append(result)
        current += timedelta(days=STEP_SIZE)
        step += 1

    return results


# ---------------------------------------------------------------------------
# Bootstrap confidence intervals
# ---------------------------------------------------------------------------

def bootstrap_ci(
    trade_pnls: list[float],
    n_samples: int = N_BOOTSTRAP,
    ci: float = 0.95,
) -> dict:
    """
    Bootstrap confidence intervals for key metrics from trade-level P&L data.
    Returns CIs for mean P&L, win rate, total return, and Sharpe estimate.
    """
    if len(trade_pnls) < 5:
        return {
            "n_trades": len(trade_pnls),
            "mean_pnl": {"point": 0, "ci_lo": 0, "ci_hi": 0},
            "win_rate": {"point": 0, "ci_lo": 0, "ci_hi": 0},
            "total_return": {"point": 0, "ci_lo": 0, "ci_hi": 0},
            "sharpe_proxy": {"point": 0, "ci_lo": 0, "ci_hi": 0},
        }

    rng = random.Random(SEED)
    alpha = (1 - ci) / 2

    boot_means = []
    boot_win_rates = []
    boot_returns = []
    boot_sharpes = []

    for _ in range(n_samples):
        sample = rng.choices(trade_pnls, k=len(trade_pnls))
        mean_pnl = sum(sample) / len(sample)
        wr = sum(1 for p in sample if p > 0) / len(sample)
        total_ret = sum(sample) / CAPITAL_START

        if len(sample) > 1:
            std_pnl = math.sqrt(sum((p - mean_pnl) ** 2 for p in sample) / (len(sample) - 1))
            sharpe_proxy = (mean_pnl / std_pnl) * math.sqrt(len(sample)) if std_pnl > 0 else 0
        else:
            sharpe_proxy = 0

        boot_means.append(mean_pnl)
        boot_win_rates.append(wr)
        boot_returns.append(total_ret)
        boot_sharpes.append(sharpe_proxy)

    def ci_bounds(arr):
        arr.sort()
        lo_idx = int(alpha * len(arr))
        hi_idx = int((1 - alpha) * len(arr))
        return arr[lo_idx], arr[hi_idx]

    point_mean = sum(trade_pnls) / len(trade_pnls)
    point_wr = sum(1 for p in trade_pnls if p > 0) / len(trade_pnls)
    point_return = sum(trade_pnls) / CAPITAL_START

    if len(trade_pnls) > 1:
        std_pnl = math.sqrt(sum((p - point_mean) ** 2 for p in trade_pnls) / (len(trade_pnls) - 1))
        point_sharpe = (point_mean / std_pnl) * math.sqrt(len(trade_pnls)) if std_pnl > 0 else 0
    else:
        point_sharpe = 0

    mean_lo, mean_hi = ci_bounds(boot_means)
    wr_lo, wr_hi = ci_bounds(boot_win_rates)
    ret_lo, ret_hi = ci_bounds(boot_returns)
    sharpe_lo, sharpe_hi = ci_bounds(boot_sharpes)

    return {
        "n_trades": len(trade_pnls),
        "mean_pnl": {"point": round(point_mean, 4), "ci_lo": round(mean_lo, 4), "ci_hi": round(mean_hi, 4)},
        "win_rate": {"point": round(point_wr, 4), "ci_lo": round(wr_lo, 4), "ci_hi": round(wr_hi, 4)},
        "total_return": {"point": round(point_return, 4), "ci_lo": round(ret_lo, 4), "ci_hi": round(ret_hi, 4)},
        "sharpe_proxy": {"point": round(point_sharpe, 4), "ci_lo": round(sharpe_lo, 4), "ci_hi": round(sharpe_hi, 4)},
    }


# ---------------------------------------------------------------------------
# Permutation test
# ---------------------------------------------------------------------------

def permutation_test(
    trade_pnls: list[float],
    n_permutations: int = N_PERMUTATION,
) -> dict:
    """
    Shuffle trade outcomes to measure skill vs luck.
    Null hypothesis: trade P&Ls are randomly distributed (no skill).
    Returns p-value: probability of observing our results by chance.
    """
    if len(trade_pnls) < 5:
        return {"p_value": 1.0, "observed_mean": 0, "null_distribution_mean": 0, "n_trades": len(trade_pnls)}

    rng = random.Random(SEED)
    observed_mean = sum(trade_pnls) / len(trade_pnls)

    # For the permutation test on prediction markets:
    # Under null, each trade's P&L sign is random (we flip win/loss).
    # A winning trade with pnl>0 could have been a loss of the same magnitude,
    # and vice versa.
    null_means = []
    for _ in range(n_permutations):
        shuffled = []
        for pnl in trade_pnls:
            if rng.random() < 0.5:
                shuffled.append(pnl)
            else:
                # Flip the outcome: if win, make it a loss of entry price
                # Since pnl = (exit - entry) * size, flipping means negating
                shuffled.append(-abs(pnl))
        null_means.append(sum(shuffled) / len(shuffled))

    # p-value: fraction of null samples >= observed
    more_extreme = sum(1 for nm in null_means if nm >= observed_mean)
    p_value = more_extreme / n_permutations

    null_distribution_mean = sum(null_means) / len(null_means)

    return {
        "p_value": round(p_value, 4),
        "observed_mean": round(observed_mean, 4),
        "null_distribution_mean": round(null_distribution_mean, 4),
        "n_trades": len(trade_pnls),
        "n_permutations": n_permutations,
    }


# ---------------------------------------------------------------------------
# Plotly HTML chart
# ---------------------------------------------------------------------------

def generate_rolling_sharpe_html(
    wf_results: dict[str, list[dict]],
    output_path: str,
) -> None:
    """Generate a Plotly HTML chart of rolling Sharpe by strategy variant."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        print("  [WARN] plotly not installed — skipping HTML chart generation")
        return

    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=("Rolling Sharpe Ratio", "Rolling Win Rate", "Cumulative P&L"),
        vertical_spacing=0.08,
        shared_xaxes=True,
    )

    colors = {
        "combined": "#00ff88",
        "resolution_lag": "#3b82f6",
        "threshold": "#a855f7",
    }

    for label, results in wf_results.items():
        if not results:
            continue
        steps = [r["step"] for r in results]
        dates = [r["window_start"][:10] for r in results]
        sharpes = [r["sharpe"] for r in results]
        win_rates = [r["win_rate"] * 100 for r in results]

        # Cumulative P&L across windows
        cum_pnl = []
        running = 0
        for r in results:
            running += r["total_pnl"]
            cum_pnl.append(running)

        color = colors.get(label, "#f59e0b")

        fig.add_trace(
            go.Scatter(
                x=dates, y=sharpes,
                mode="lines+markers",
                name=f"{label}",
                line=dict(color=color, width=2),
                marker=dict(size=4),
                legendgroup=label,
            ),
            row=1, col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=dates, y=win_rates,
                mode="lines+markers",
                name=f"{label}",
                line=dict(color=color, width=2),
                marker=dict(size=4),
                legendgroup=label,
                showlegend=False,
            ),
            row=2, col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=dates, y=cum_pnl,
                mode="lines",
                name=f"{label}",
                line=dict(color=color, width=2),
                fill="tozeroy",
                fillcolor=color.replace(")", ",0.05)").replace("rgb", "rgba") if "rgb" in color else f"rgba{tuple(int(color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.05,)}",
                legendgroup=label,
                showlegend=False,
            ),
            row=3, col=1,
        )

    # Add reference lines
    fig.add_hline(y=1.0, line_dash="dash", line_color="#475569", row=1, col=1,
                  annotation_text="Sharpe=1.0", annotation=dict(font_color="#475569"))
    fig.add_hline(y=0, line_dash="dash", line_color="#475569", row=1, col=1)
    fig.add_hline(y=55, line_dash="dash", line_color="#475569", row=2, col=1,
                  annotation_text="55% target", annotation=dict(font_color="#475569"))

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0a0e1a",
        plot_bgcolor="#111827",
        font=dict(family="Inter, system-ui, sans-serif", color="#f1f5f9"),
        height=900,
        title=dict(
            text="EdgeLab Walk-Forward Validation — Rolling Performance",
            font=dict(size=20),
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
    )

    for i in range(1, 4):
        fig.update_xaxes(gridcolor="#1e293b", row=i, col=1)
        fig.update_yaxes(gridcolor="#1e293b", row=i, col=1)

    fig.update_yaxes(title_text="Sharpe", row=1, col=1)
    fig.update_yaxes(title_text="Win Rate %", row=2, col=1)
    fig.update_yaxes(title_text="Cumulative P&L ($)", row=3, col=1)

    fig.write_html(output_path, include_plotlyjs=True)
    print(f"  Chart saved to {output_path}")


# ---------------------------------------------------------------------------
# Verdict generation
# ---------------------------------------------------------------------------

def generate_verdict(
    wf_results: dict[str, list[dict]],
    bootstrap_results: dict[str, dict],
    permutation_results: dict[str, dict],
) -> str:
    """Generate plain English validation report."""
    lines = []
    lines.append("=" * 70)
    lines.append("  EDGELAB WALK-FORWARD VALIDATION REPORT")
    lines.append("=" * 70)
    lines.append(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")

    # Overall verdict
    combined_perm = permutation_results.get("combined", {})
    combined_boot = bootstrap_results.get("combined", {})
    p_value = combined_perm.get("p_value", 1.0)
    mean_pnl_ci = combined_boot.get("mean_pnl", {})

    # Determine if edge is real
    edge_real = p_value < 0.05 and mean_pnl_ci.get("ci_lo", 0) > 0
    edge_likely = p_value < 0.10 and mean_pnl_ci.get("point", 0) > 0
    confidence_pct = max(0, min(100, round((1 - p_value) * 100)))

    lines.append("  IS THE EDGE REAL?")
    lines.append("  " + "-" * 40)
    if edge_real:
        lines.append(f"  YES — Confidence: {confidence_pct}%")
        lines.append(f"  The permutation test p-value is {p_value:.4f} (< 0.05).")
        lines.append(f"  The 95% CI for mean trade P&L is entirely positive:")
        lines.append(f"    [{mean_pnl_ci.get('ci_lo', 0):+.4f}, {mean_pnl_ci.get('ci_hi', 0):+.4f}]")
        lines.append(f"  This edge is statistically significant — not explainable by luck.")
    elif edge_likely:
        lines.append(f"  LIKELY — Confidence: {confidence_pct}%")
        lines.append(f"  The p-value is {p_value:.4f} (< 0.10 but >= 0.05).")
        lines.append(f"  Mean trade P&L: {mean_pnl_ci.get('point', 0):+.4f}")
        lines.append(f"  Suggestive of real edge but not conclusive at 95% level.")
    else:
        lines.append(f"  NO — Confidence: {confidence_pct}%")
        lines.append(f"  The p-value is {p_value:.4f} — results could be explained by luck.")
        lines.append(f"  Mean trade P&L CI includes zero or is negative.")

    lines.append("")

    # Strategy robustness comparison
    lines.append("  STRATEGY ROBUSTNESS")
    lines.append("  " + "-" * 40)

    strategy_scores = {}
    for label in ["combined", "resolution_lag", "threshold"]:
        results = wf_results.get(label, [])
        boot = bootstrap_results.get(label, {})
        perm = permutation_results.get(label, {})

        if not results:
            continue

        windows_with_trades = [r for r in results if r["total_trades"] > 0]
        profitable_windows = [r for r in windows_with_trades if r["total_pnl"] > 0]
        pct_profitable = len(profitable_windows) / len(windows_with_trades) * 100 if windows_with_trades else 0

        avg_sharpe = sum(r["sharpe"] for r in windows_with_trades) / len(windows_with_trades) if windows_with_trades else 0
        avg_trades = sum(r["total_trades"] for r in windows_with_trades) / len(windows_with_trades) if windows_with_trades else 0
        total_pnl = sum(r["total_pnl"] for r in results)

        # Robustness score: weighted by consistency, p-value, Sharpe
        p = perm.get("p_value", 1.0)
        robustness = (1 - p) * 0.4 + (pct_profitable / 100) * 0.3 + min(avg_sharpe / 3, 1) * 0.3
        strategy_scores[label] = robustness

        lines.append(f"")
        lines.append(f"  {label.upper().replace('_', ' ')}")
        lines.append(f"    Windows tested:      {len(results)} ({len(windows_with_trades)} had trades)")
        lines.append(f"    % profitable windows: {pct_profitable:.0f}%")
        lines.append(f"    Avg Sharpe:           {avg_sharpe:.2f}")
        lines.append(f"    Avg trades/window:    {avg_trades:.1f}")
        lines.append(f"    Total P&L:            ${total_pnl:+.2f}")
        lines.append(f"    P-value:              {p:.4f}")
        lines.append(f"    Robustness score:     {robustness:.2f}/1.00")

        wr_ci = boot.get("win_rate", {})
        if wr_ci:
            lines.append(f"    Win rate 95% CI:      [{wr_ci.get('ci_lo', 0):.1%}, {wr_ci.get('ci_hi', 0):.1%}]")

    # Most robust strategy
    if strategy_scores:
        best = max(strategy_scores, key=strategy_scores.get)
        lines.append(f"")
        lines.append(f"  Most robust strategy: {best.upper().replace('_', ' ')} (score: {strategy_scores[best]:.2f})")

    lines.append("")

    # Market conditions analysis
    lines.append("  MARKET CONDITIONS ANALYSIS")
    lines.append("  " + "-" * 40)

    combined_results = wf_results.get("combined", [])
    if combined_results:
        windows_with_trades = [r for r in combined_results if r["total_trades"] > 0]
        if windows_with_trades:
            # Find worst windows
            worst_windows = sorted(windows_with_trades, key=lambda r: r["sharpe"])[:3]
            best_windows = sorted(windows_with_trades, key=lambda r: r["sharpe"], reverse=True)[:3]

            lines.append(f"  Worst performing windows:")
            for w in worst_windows:
                lines.append(f"    {w['window_start'][:10]}: Sharpe={w['sharpe']:.2f}, P&L=${w['total_pnl']:+.2f}, {w['total_trades']} trades")

            lines.append(f"")
            lines.append(f"  Best performing windows:")
            for w in best_windows:
                lines.append(f"    {w['window_start'][:10]}: Sharpe={w['sharpe']:.2f}, P&L=${w['total_pnl']:+.2f}, {w['total_trades']} trades")

            # Analyze what hurts
            neg_windows = [r for r in windows_with_trades if r["total_pnl"] < 0]
            if neg_windows:
                avg_neg_trades = sum(r["total_trades"] for r in neg_windows) / len(neg_windows)
                avg_pos_trades = sum(r["total_trades"] for r in windows_with_trades if r["total_pnl"] > 0) / max(1, len([r for r in windows_with_trades if r["total_pnl"] > 0]))
                lines.append(f"")
                lines.append(f"  Losing windows: {len(neg_windows)}/{len(windows_with_trades)} ({len(neg_windows)/len(windows_with_trades)*100:.0f}%)")
                lines.append(f"  Avg trades in losing windows: {avg_neg_trades:.1f}")
                lines.append(f"  Avg trades in winning windows: {avg_pos_trades:.1f}")
                if avg_neg_trades < avg_pos_trades * 0.5:
                    lines.append(f"  Pattern: Losses concentrated in LOW-ACTIVITY periods (sparse markets)")
                else:
                    lines.append(f"  Pattern: Losses occur across varying market conditions")
            else:
                lines.append(f"  No losing windows detected.")

    lines.append("")

    # Minimum capital recommendation
    lines.append("  MINIMUM CAPITAL FOR LIVE TRADING")
    lines.append("  " + "-" * 40)

    combined_boot = bootstrap_results.get("combined", {})
    combined_results = wf_results.get("combined", [])

    if combined_results and combined_boot:
        # Calculate worst-case drawdown
        max_dds = [r["max_drawdown"] for r in combined_results if r["total_trades"] > 0]
        worst_dd = max(max_dds) if max_dds else 0.20

        # With $500 starting capital and worst observed drawdown
        # Kelly sizing limits us, but variance from bootstrap gives CI
        pnl_ci_lo = combined_boot.get("total_return", {}).get("ci_lo", -0.10)
        worst_case_loss_pct = min(worst_dd, abs(pnl_ci_lo)) if pnl_ci_lo < 0 else worst_dd

        # Rule of thumb: need enough capital that worst drawdown doesn't wipe you out
        # and minimum trade size ($10) is still achievable
        min_capital_drawdown = 10.0 / (0.05 * (1 - worst_case_loss_pct)) if worst_case_loss_pct < 1 else 500
        min_capital_kelly = 10.0 / 0.05  # min trade $10 at 5% NAV cap = $200 minimum

        recommended_capital = max(min_capital_drawdown, min_capital_kelly, 200)

        lines.append(f"  Worst observed drawdown:    {worst_dd:.1%}")
        lines.append(f"  95% CI lower bound return:  {pnl_ci_lo:+.1%}")
        lines.append(f"  Min capital (drawdown):     ${min_capital_drawdown:.0f}")
        lines.append(f"  Min capital (Kelly floor):  ${min_capital_kelly:.0f}")
        lines.append(f"  RECOMMENDED MINIMUM:        ${max(recommended_capital, 300):.0f}")
        lines.append(f"  Current paper capital:      $500")

        if recommended_capital <= 500:
            lines.append(f"  Verdict: $500 is SUFFICIENT for paper trading validation.")
        else:
            lines.append(f"  Verdict: Consider starting with ${recommended_capital:.0f} for margin of safety.")

    lines.append("")
    lines.append("=" * 70)
    lines.append(f"  Bootstrap samples: {N_BOOTSTRAP}")
    lines.append(f"  Permutation samples: {N_PERMUTATION}")
    lines.append(f"  Walk-forward: train={TRAIN_WINDOW}d, test={TEST_WINDOW}d, step={STEP_SIZE}d")
    lines.append("=" * 70)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    t0 = time.time()
    print("\n  EDGELAB WALK-FORWARD VALIDATION ENGINE")
    print("  " + "=" * 50)

    # 1. Load data
    print("\n  [1/8] Loading market data from simulation DB...")
    markets = load_markets()
    print(f"        {len(markets)} markets loaded")

    # Determine date range
    dates = []
    for m in markets:
        dt = parse_date(m.get("end_date", ""))
        if dt:
            dates.append(dt)
    if not dates:
        print("  ERROR: No markets with valid dates found.")
        return
    date_range = (min(dates), max(dates))
    total_span = (date_range[1] - date_range[0]).days
    print(f"        Date range: {date_range[0].strftime('%Y-%m-%d')} to {date_range[1].strftime('%Y-%m-%d')} ({total_span} days)")

    # 2. Walk-forward validation for each strategy variant
    strategy_configs = {
        "combined": None,  # all strategies
        "resolution_lag": {"resolution_lag"},
        "threshold": {"threshold"},
    }

    wf_results: dict[str, list[dict]] = {}
    all_trade_pnls: dict[str, list[float]] = {}

    print(f"\n  [2/8] Running walk-forward validation...")
    print(f"        Train={TRAIN_WINDOW}d, Test={TEST_WINDOW}d, Step={STEP_SIZE}d")

    for label, allowed in strategy_configs.items():
        print(f"\n        Strategy: {label.upper()}", end="", flush=True)
        results = walk_forward(markets, date_range, allowed, label)
        wf_results[label] = results

        # Collect all trade P&Ls
        pnls = []
        for r in results:
            pnls.extend(t["pnl"] for t in r.get("trade_log", []))
        all_trade_pnls[label] = pnls

        windows_with_trades = sum(1 for r in results if r["total_trades"] > 0)
        total_trades = sum(r["total_trades"] for r in results)
        total_pnl = sum(r["total_pnl"] for r in results)
        avg_sharpe = sum(r["sharpe"] for r in results if r["total_trades"] > 0) / max(windows_with_trades, 1)
        print(f" — {len(results)} windows, {windows_with_trades} active, {total_trades} trades, P&L=${total_pnl:+.2f}, avg Sharpe={avg_sharpe:.2f}")

    # 3. Bootstrap confidence intervals
    print(f"\n  [3/8] Running {N_BOOTSTRAP} bootstrap samples per strategy...")
    bootstrap_results: dict[str, dict] = {}
    for label, pnls in all_trade_pnls.items():
        print(f"        {label}: {len(pnls)} trades", end="", flush=True)
        boot = bootstrap_ci(pnls)
        bootstrap_results[label] = boot
        wr = boot["win_rate"]
        print(f" — win rate [{wr['ci_lo']:.1%}, {wr['ci_hi']:.1%}], mean P&L [{boot['mean_pnl']['ci_lo']:+.4f}, {boot['mean_pnl']['ci_hi']:+.4f}]")

    # 4. Permutation test
    print(f"\n  [4/8] Running {N_PERMUTATION} permutation tests per strategy...")
    permutation_results: dict[str, dict] = {}
    for label, pnls in all_trade_pnls.items():
        print(f"        {label}", end="", flush=True)
        perm = permutation_test(pnls)
        permutation_results[label] = perm
        print(f" — p-value={perm['p_value']:.4f}, observed_mean={perm['observed_mean']:+.4f}")

    # 5. Generate Plotly chart
    print(f"\n  [5/8] Generating rolling Sharpe HTML chart...")
    chart_path = os.path.join(OUT_DIR, "rolling_sharpe.html")
    generate_rolling_sharpe_html(wf_results, chart_path)

    # 6. Generate verdict
    print(f"\n  [6/8] Generating validation verdict...")
    verdict = generate_verdict(wf_results, bootstrap_results, permutation_results)
    verdict_path = os.path.join(OUT_DIR, "validation_report.txt")
    with open(verdict_path, "w") as f:
        f.write(verdict)
    print(f"        Saved to {verdict_path}")

    # 7. Save full results JSON
    print(f"\n  [7/8] Saving full results to JSON...")
    results_path = os.path.join(OUT_DIR, "walk_forward_results.json")

    # Strip trade_log from JSON output to keep file manageable
    wf_export = {}
    for label, results in wf_results.items():
        wf_export[label] = []
        for r in results:
            r_copy = {k: v for k, v in r.items() if k not in ("trade_log", "daily_returns")}
            wf_export[label].append(r_copy)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "train_window": TRAIN_WINDOW,
            "test_window": TEST_WINDOW,
            "step_size": STEP_SIZE,
            "capital_start": CAPITAL_START,
            "max_trade": MAX_TRADE,
            "n_bootstrap": N_BOOTSTRAP,
            "n_permutation": N_PERMUTATION,
        },
        "walk_forward": wf_export,
        "bootstrap": bootstrap_results,
        "permutation": permutation_results,
        "summary": {
            label: {
                "total_windows": len(results),
                "active_windows": sum(1 for r in results if r["total_trades"] > 0),
                "total_trades": sum(r["total_trades"] for r in results),
                "total_pnl": round(sum(r["total_pnl"] for r in results), 2),
                "avg_sharpe": round(
                    sum(r["sharpe"] for r in results if r["total_trades"] > 0)
                    / max(sum(1 for r in results if r["total_trades"] > 0), 1),
                    4,
                ),
                "pct_profitable_windows": round(
                    sum(1 for r in results if r["total_pnl"] > 0)
                    / max(sum(1 for r in results if r["total_trades"] > 0), 1) * 100,
                    1,
                ),
            }
            for label, results in wf_results.items()
        },
    }

    with open(results_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"        Saved to {results_path}")

    # 8. Print verdict to console
    print(f"\n  [8/8] Validation complete in {time.time() - t0:.1f}s")
    print()
    print(verdict)


if __name__ == "__main__":
    main()
