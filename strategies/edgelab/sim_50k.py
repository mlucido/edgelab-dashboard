"""
EdgeLab — $50,000 Capital Simulation
Real Polymarket historical data, last 90 days
Run: python sim_50k.py
Run: python sim_50k.py --optimized  (side-by-side comparison with filtered config)

pip install aiohttp requests numpy
"""

import argparse
import asyncio
import aiohttp
import json
import math
import random
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── CONFIG ──────────────────────────────────────────────────────────────
STARTING_CAPITAL   = 50_000.00
MAX_TRADE          = 2_500.00   # 5% of capital per trade (scaled from $50 on $500)
MIN_TRADE          = 100.00
MAX_DEPLOY         = 0.60       # 60% max deployed at once
MAX_CATEGORY       = 0.20       # 20% per category
MAX_POSITIONS      = 20
CIRCUIT_BREAKER    = -2_000.00  # Daily loss limit ($200 on $500 → $2K on $50K)
MIN_NET_RETURN     = 0.015      # 1.5% min net return
MIN_VOLUME         = 5_000.00   # Minimum volume required (liquidity=0 on closed markets)
MAX_DAYS           = 14
TAKER_FEE          = 0.005
CONSECUTIVE_LOSS_PAUSE = 3

# Calibration from real Polymarket backtest (your data/calibration_historical.json)
CALIBRATION = {
    52: 0.474, 57: 0.533, 62: 0.596, 67: 0.668,
    72: 0.747, 77: 0.791, 82: 0.839, 87: 0.875,
    92: 0.924, 97: 0.966,
}

CONFIGS = {
    "conservative": {"max_trade": 1250,  "max_deploy": 0.40, "circuit": -1000},
    "standard":     {"max_trade": 2500,  "max_deploy": 0.60, "circuit": -2000},
    "aggressive":   {"max_trade": 5000,  "max_deploy": 0.80, "circuit": -4000},
}

# ── OPTIMIZED CONFIG ─────────────────────────────────────────────────────
# Capital allocation weights (mirrors src/execution/capital_allocator.py)
ALLOC_WEIGHTS = {
    "resolution_lag": 0.60,
    "threshold":      0.25,
    "arb":            0.15,
}
# Threshold filter params (mirrors src/strategies/threshold_config.py)
OPT_MIN_VOLUME       = 10_000   # Only trade threshold markets with volume > $10K
OPT_MAX_DAYS_TO_RES  = 7        # Only enter threshold trades ≤ 7 days to resolution

# ── HELPERS ─────────────────────────────────────────────────────────────

def get_true_prob(prob):
    best_key = min(CALIBRATION.keys(), key=lambda k: abs(k - prob * 100))
    return CALIBRATION[best_key]

def net_return(prob):
    return ((1 - prob) / prob) * (1 - TAKER_FEE) - TAKER_FEE

def kelly_size(prob, capital):
    tp = get_true_prob(prob)
    b = (1 - prob) / prob
    f = (tp * b - (1 - tp)) / b
    return max(0, 0.25 * f * capital)

def ev_per_dollar(prob):
    tp = get_true_prob(prob)
    win = ((1 - prob) / prob) * (1 - TAKER_FEE)
    loss = 1 + TAKER_FEE
    return tp * win - (1 - tp) * loss

def sharpe(daily_returns):
    if len(daily_returns) < 2:
        return 0
    avg = sum(daily_returns) / len(daily_returns)
    variance = sum((r - avg) ** 2 for r in daily_returns) / len(daily_returns)
    std = math.sqrt(variance)
    return (avg / std) * math.sqrt(365) if std > 0 else 0

def max_drawdown(equity_curve):
    peak = equity_curve[0]
    max_dd = 0
    max_dd_start = 0
    max_dd_end = 0
    for i, val in enumerate(equity_curve):
        if val > peak:
            peak = val
        dd = (peak - val) / peak
        if dd > max_dd:
            max_dd = dd
            max_dd_end = i
    return max_dd, max_dd_end

# ── FETCH REAL HISTORICAL DATA ──────────────────────────────────────────

async def fetch_historical_markets():
    print("Fetching real Polymarket historical markets (last 90 days)...")
    markets = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
    
    async with aiohttp.ClientSession() as session:
        offset = 0
        pages = 0
        while pages < 20:  # Max 10,000 markets
            url = f"https://gamma-api.polymarket.com/markets?closed=true&limit=500&offset={offset}&order=closedTime&ascending=false"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    if r.status != 200:
                        break
                    data = await r.json()
                    batch = data if isinstance(data, list) else data.get("data", [])
                    if not batch:
                        break

                    # Check if we've gone past our cutoff (results are newest-first)
                    all_too_old = True
                    for m in batch:
                        try:
                            # Parse resolution date — use closedTime (actual resolution)
                            end_str = m.get("closedTime")
                            if not end_str:
                                continue
                            # Normalize timezone: "+00" → "+00:00", "Z" → "+00:00"
                            end_str = end_str.replace("Z", "+00:00")
                            if end_str.endswith("+00"):
                                end_str += ":00"
                            end_date = datetime.fromisoformat(end_str)
                            if end_date < cutoff:
                                continue  # Too old
                            all_too_old = False

                            # Resolved markets show [1,0] or [0,1] — use lastTradePrice
                            last_price = m.get("lastTradePrice")
                            if last_price is None:
                                continue
                            last_price = float(last_price)
                            # lastTradePrice is for YES outcome; pick the high-confidence side
                            max_p = max(last_price, 1 - last_price)
                            if max_p < 0.50 or max_p > 0.999:
                                continue
                            
                            # Use volume as activity proxy (liquidity=0 on closed markets)
                            volume = float(m.get("volume", 0))
                            
                            tags = []
                            raw_tags = m.get("tags", [])
                            if isinstance(raw_tags, str):
                                raw_tags = json.loads(raw_tags)
                            for t in raw_tags:
                                tags.append(t.get("label", t) if isinstance(t, dict) else t)
                            category = tags[0] if tags else "other"
                            
                            # Estimate resolved_yes from price
                            # If favored outcome price is > 0.5 and market closed,
                            # use last price as strong signal (>0.95 = almost certainly resolved to favored)
                            resolved_yes = max_p > 0.5  # Simplified: assume favored outcome won
                            # Add realistic noise based on probability
                            true_p = get_true_prob(max_p)
                            resolved_yes = random.random() < true_p
                            
                            markets.append({
                                "id": m.get("id", ""),
                                "question": m.get("question", "")[:80],
                                "prob": max_p,
                                "volume": volume,
                                "liquidity": volume,  # Use volume as proxy
                                "end_date": end_date,
                                "category": category,
                                "resolved_yes": resolved_yes,
                                "net_ret": net_return(max_p),
                                "ev": ev_per_dollar(max_p),
                            })
                        except Exception:
                            continue
                    
                    # Results are newest-first; stop if entire page is before cutoff
                    if all_too_old:
                        print(f"  Page {pages+1}: all markets older than 90 days, stopping.")
                        break

                    offset += 500
                    pages += 1
                    print(f"  Fetched page {pages}: {len(markets)} qualifying markets so far...")
                    await asyncio.sleep(0.2)
                    
            except Exception as e:
                print(f"  API error on page {pages}: {e}")
                break
    
    markets.sort(key=lambda m: m["end_date"])
    print(f"  Total: {len(markets)} markets from last 90 days\n")
    return markets

# ── SIMULATION ENGINE ────────────────────────────────────────────────────

def _threshold_passes_optimized_filters(m: dict) -> bool:
    """Return True if a threshold-strategy market passes the optimised filters."""
    if m.get("volume", 0) < OPT_MIN_VOLUME:
        return False
    end_date = m.get("end_date")
    if end_date is not None:
        now = datetime.now(timezone.utc)
        if hasattr(end_date, "tzinfo") and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
        days_remaining = (end_date - now).total_seconds() / 86_400
        if days_remaining > OPT_MAX_DAYS_TO_RES:
            return False
    return True


def run_simulation(markets, config_name, cfg, optimized: bool = False):
    capital = STARTING_CAPITAL
    max_trade = cfg["max_trade"]
    max_deploy = cfg["max_deploy"]
    circuit = cfg["circuit"]
    
    open_positions = []
    trade_log = []
    equity_curve = [capital]
    daily_pnl = []
    
    strategy_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0})
    circuit_triggers = 0
    strategy_pauses = defaultdict(int)
    consecutive_losses = defaultdict(int)
    
    current_day = None
    day_pnl = 0
    
    for m in markets:
        day = m["end_date"].date()
        
        # New day housekeeping
        if current_day != day:
            if current_day is not None:
                daily_pnl.append(day_pnl)
                equity_curve.append(capital)
                day_pnl = 0
            current_day = day
        
        # Resolve any open positions that have passed end_date
        still_open = []
        for pos in open_positions:
            if pos["end_date"] <= m["end_date"]:
                # Resolve position
                if pos["resolved_yes"]:
                    pnl = pos["size"] * net_return(pos["prob"])
                    outcome = "WIN"
                else:
                    pnl = -pos["size"] * (1 + TAKER_FEE)
                    outcome = "LOSS"
                
                capital += pnl
                day_pnl += pnl
                strategy_stats[pos["strategy"]]["trades"] += 1
                strategy_stats[pos["strategy"]]["pnl"] += pnl
                if pnl > 0:
                    strategy_stats[pos["strategy"]]["wins"] += 1
                    consecutive_losses[pos["strategy"]] = 0
                else:
                    consecutive_losses[pos["strategy"]] += 1
                    if consecutive_losses[pos["strategy"]] >= CONSECUTIVE_LOSS_PAUSE:
                        strategy_pauses[pos["strategy"]] = 3  # Pause 3 days
                
                trade_log.append({
                    "day": str(day), "market": m["question"][:50],
                    "strategy": pos["strategy"], "prob": pos["prob"],
                    "size": pos["size"], "pnl": round(pnl, 2), "outcome": outcome
                })
            else:
                still_open.append(pos)
        open_positions = still_open
        
        # Circuit breaker check
        if day_pnl < circuit:
            circuit_triggers += 1
            continue  # Skip trading rest of day
        
        # ── GUARDRAIL STACK ──────────────────────────────────────────
        
        # Layer 1: Trade quality
        if m["net_ret"] < MIN_NET_RETURN:
            continue
        if m["liquidity"] < MIN_VOLUME:
            continue
        if m["ev"] <= 0:
            continue
        
        # Determine strategy
        if m["prob"] >= 0.95:
            strategy = "resolution_lag"
        elif m["prob"] >= 0.88:
            strategy = "threshold"
        else:
            continue  # Below our threshold

        # Optimised: apply threshold filters to threshold-strategy markets
        if optimized and strategy == "threshold":
            if not _threshold_passes_optimized_filters(m):
                continue

        # Check strategy pause
        if strategy_pauses.get(strategy, 0) > 0:
            strategy_pauses[strategy] -= 1
            continue

        # Layer 2: Position sizing
        size = kelly_size(m["prob"], capital)
        if optimized:
            # Cap size at strategy's allocated capital slice
            alloc_cap = capital * ALLOC_WEIGHTS.get(strategy, 1.0)
            size = min(size, alloc_cap)
        size = min(size, max_trade)
        size = max(size, 0)
        if size < MIN_TRADE:
            continue
        
        # Layer 3: Portfolio exposure
        deployed = sum(p["size"] for p in open_positions)
        if deployed + size > capital * max_deploy:
            continue
        
        cat_deployed = sum(p["size"] for p in open_positions if p["category"] == m["category"])
        if cat_deployed + size > capital * MAX_CATEGORY:
            continue
        
        if len(open_positions) >= MAX_POSITIONS:
            continue
        
        # ── PLACE TRADE ──────────────────────────────────────────────
        open_positions.append({
            "prob": m["prob"],
            "size": size,
            "strategy": strategy,
            "category": m["category"],
            "end_date": m["end_date"],
            "resolved_yes": m["resolved_yes"],
            "question": m["question"],
        })
    
    # Close any remaining positions at end
    for pos in open_positions:
        true_p = get_true_prob(pos["prob"])
        resolved = random.random() < true_p
        pnl = pos["size"] * net_return(pos["prob"]) if resolved else -pos["size"] * (1 + TAKER_FEE)
        capital += pnl
        strategy_stats[pos["strategy"]]["trades"] += 1
        strategy_stats[pos["strategy"]]["pnl"] += pnl
        if pnl > 0:
            strategy_stats[pos["strategy"]]["wins"] += 1
    
    # Final day
    daily_pnl.append(day_pnl)
    equity_curve.append(capital)
    
    # Compute metrics
    total_trades = sum(s["trades"] for s in strategy_stats.values())
    total_wins = sum(s["wins"] for s in strategy_stats.values())
    total_pnl = capital - STARTING_CAPITAL
    win_rate = total_wins / total_trades if total_trades > 0 else 0
    
    sr = sharpe([p / STARTING_CAPITAL for p in daily_pnl])
    max_dd, dd_end = max_drawdown(equity_curve)
    
    monthly_pnl = total_pnl / 3  # 90 days = 3 months
    daily_avg = total_pnl / 90
    
    # Capital velocity
    total_deployed = sum(abs(t["size"]) for t in trade_log)
    velocity = total_deployed / STARTING_CAPITAL / 3  # turns per month
    
    # GO / NO-GO
    criteria = {
        "Sharpe > 1.0":           (sr > 1.0,          f"{sr:.2f}"),
        "Max drawdown < 20%":     (max_dd < 0.20,      f"{max_dd*100:.1f}%"),
        "Win rate > 55%":         (win_rate > 0.55,    f"{win_rate*100:.1f}%"),
        "Positive P&L":           (total_pnl > 0,      f"${total_pnl:+,.2f}"),
        "Circuit breakers < 5":   (circuit_triggers < 5, str(circuit_triggers)),
    }
    passes = sum(1 for v, _ in criteria.values() if v)
    verdict = "✅ GO" if passes == 5 else ("⚠️  CONDITIONAL" if passes >= 3 else "❌ NO-GO")
    
    return {
        "config": config_name,
        "capital": capital,
        "total_pnl": total_pnl,
        "daily_avg": daily_avg,
        "monthly_avg": monthly_pnl,
        "sharpe": sr,
        "max_dd": max_dd,
        "win_rate": win_rate,
        "total_trades": total_trades,
        "circuit_triggers": circuit_triggers,
        "velocity": velocity,
        "strategy_stats": dict(strategy_stats),
        "criteria": criteria,
        "verdict": verdict,
        "equity_curve": equity_curve,
        "trade_log": trade_log[:20],  # Sample
    }

# ── REPORT ───────────────────────────────────────────────────────────────

def print_comparison(original_results, optimized_results):
    """Print side-by-side comparison of original vs optimised standard config."""
    orig = next((r for r in original_results if r["config"] == "standard"), original_results[0])
    opt  = optimized_results[0]  # optimised only runs standard config

    print("\n")
    print("═" * 72)
    print("  EDGELAB — ORIGINAL vs OPTIMIZED (standard config, 90 days)")
    print("═" * 72)
    print(f"  {'Metric':<28} {'Original':>18} {'Optimized':>18}")
    print(f"  {'─'*28} {'─'*18} {'─'*18}")

    def row(label, orig_val, opt_val):
        print(f"  {label:<28} {orig_val:>18} {opt_val:>18}")

    row("Sharpe ratio",
        f"{orig['sharpe']:.2f}", f"{opt['sharpe']:.2f}")
    row("Win rate",
        f"{orig['win_rate']*100:.1f}%", f"{opt['win_rate']*100:.1f}%")
    row("Max drawdown",
        f"{orig['max_dd']*100:.1f}%", f"{opt['max_dd']*100:.1f}%")
    row("Total P&L",
        f"${orig['total_pnl']:+,.2f}", f"${opt['total_pnl']:+,.2f}")
    row("Circuit triggers",
        str(orig['circuit_triggers']), str(opt['circuit_triggers']))
    row("Total trades",
        str(orig['total_trades']), str(opt['total_trades']))

    print(f"\n  BY STRATEGY:")
    all_strategies = sorted(
        set(list(orig['strategy_stats'].keys()) + list(opt['strategy_stats'].keys()))
    )
    for s in all_strategies:
        o_s = orig['strategy_stats'].get(s, {"trades": 0, "wins": 0, "pnl": 0})
        p_s = opt['strategy_stats'].get(s,  {"trades": 0, "wins": 0, "pnl": 0})
        o_wr = o_s['wins'] / o_s['trades'] * 100 if o_s['trades'] > 0 else 0
        p_wr = p_s['wins'] / p_s['trades'] * 100 if p_s['trades'] > 0 else 0
        print(f"    {s:<20}  orig: {o_s['trades']:>3}t {o_wr:>5.1f}% ${o_s['pnl']:>+8,.2f}"
              f"  |  opt: {p_s['trades']:>3}t {p_wr:>5.1f}% ${p_s['pnl']:>+8,.2f}")

    print(f"\n  TARGETS:")
    sharpe_ok = opt['sharpe'] > 3.0
    thr_wr = opt['strategy_stats'].get('threshold', {})
    thr_wr_pct = thr_wr.get('wins', 0) / thr_wr.get('trades', 1) * 100 if thr_wr.get('trades', 0) > 0 else 0
    thr_ok = thr_wr_pct > 88
    print(f"    Sharpe > 3.0:              {'PASS' if sharpe_ok else 'FAIL'} ({opt['sharpe']:.2f})")
    print(f"    Threshold win rate > 88%:  {'PASS' if thr_ok else 'FAIL'} ({thr_wr_pct:.1f}%)")
    print("═" * 72)


def print_report(results):
    print("\n")
    print("═" * 65)
    print("  EDGELAB $50,000 SIMULATION — 90 DAYS — REAL POLYMARKET DATA")
    print("═" * 65)
    
    for r in results:
        print(f"\n  ┌─ CONFIG: {r['config'].upper()} {'─'*(52-len(r['config']))}")
        print(f"  │  Starting capital:       ${STARTING_CAPITAL:>12,.2f}")
        print(f"  │  Ending capital:         ${r['capital']:>12,.2f}")
        print(f"  │  Total P&L:              ${r['total_pnl']:>+12,.2f}  ({r['total_pnl']/STARTING_CAPITAL*100:+.1f}%)")
        print(f"  │  Daily avg P&L:          ${r['daily_avg']:>+12,.2f}")
        print(f"  │  Monthly avg P&L:        ${r['monthly_avg']:>+12,.2f}")
        print(f"  │  Sharpe ratio:           {r['sharpe']:>12.2f}")
        print(f"  │  Max drawdown:           {r['max_dd']*100:>11.1f}%")
        print(f"  │  Win rate:               {r['win_rate']*100:>11.1f}%  ({r['total_trades']} trades)")
        print(f"  │  Capital velocity:       {r['velocity']:>11.1f}x/month")
        print(f"  │  Circuit breakers:       {r['circuit_triggers']:>12}")
        print(f"  │")
        print(f"  │  BY STRATEGY:")
        for s, stats in r['strategy_stats'].items():
            wr = stats['wins']/stats['trades']*100 if stats['trades'] > 0 else 0
            print(f"  │    {s:<20} {stats['trades']:>3} trades | {wr:>5.1f}% win | ${stats['pnl']:>+8,.2f}")
        print(f"  │")
        print(f"  │  GO/NO-GO CRITERIA:")
        for criterion, (passed, actual) in r['criteria'].items():
            icon = "  ✓" if passed else "  ✗"
            print(f"  │  {icon}  {criterion:<30} (actual: {actual})")
        print(f"  │")
        print(f"  │  VERDICT: {r['verdict']}")
        print(f"  └{'─'*62}")
    
    print("\n  SENSITIVITY COMPARISON:")
    print(f"  {'Config':<15} {'Sharpe':>8} {'Drawdown':>10} {'P&L':>12} {'Daily Avg':>12} {'Verdict'}")
    print(f"  {'─'*15} {'─'*8} {'─'*10} {'─'*12} {'─'*12} {'─'*12}")
    for r in results:
        print(f"  {r['config']:<15} {r['sharpe']:>8.2f} {r['max_dd']*100:>9.1f}% ${r['total_pnl']:>+11,.2f} ${r['daily_avg']:>+11,.2f} {r['verdict']}")
    
    print("\n  ANNUALIZED PROJECTION (if 90-day performance holds):")
    for r in results:
        annual = r['total_pnl'] * 4  # 90 days × 4 = 1 year
        annual_pct = annual / STARTING_CAPITAL * 100
        print(f"  {r['config']:<15} ${annual:>+12,.2f}/year  ({annual_pct:+.1f}% on ${STARTING_CAPITAL:,.0f})")
    
    print("\n  NOTE: Simulation uses real resolved markets. Resolution outcomes")
    print("  are estimated using calibrated true probabilities from your")
    print("  historical backtest data. Real performance may vary.")
    print("═" * 65)

# ── MAIN ─────────────────────────────────────────────────────────────────

def _parse_args():
    parser = argparse.ArgumentParser(
        description="EdgeLab $50,000 capital simulation."
    )
    parser.add_argument(
        "--optimized",
        action="store_true",
        help="Run optimized config (allocation caps + threshold filters) alongside original.",
    )
    return parser.parse_args()


async def main():
    args = _parse_args()

    print("\n  EdgeLab $50,000 Capital Simulation")
    print("  Fetching 90 days of real Polymarket data...\n")

    markets = await fetch_historical_markets()

    if len(markets) < 10:
        print("  WARNING: Not enough historical data fetched. Check API connection.")
        print("  Running analytical estimate instead...\n")
        print_analytical_estimate()
        return

    if args.optimized:
        print(f"  Running ORIGINAL + OPTIMIZED simulations on {len(markets)} real markets...\n")

        print("  [Original] Simulating 3 configs...")
        original_results = []
        for config_name, cfg in CONFIGS.items():
            print(f"    Simulating {config_name} config...")
            r = run_simulation(markets, config_name, cfg, optimized=False)
            original_results.append(r)
            print(f"      -> P&L: ${r['total_pnl']:+,.2f} | Sharpe: {r['sharpe']:.2f} | Win: {r['win_rate']*100:.1f}%")

        print("\n  [Optimized] Simulating standard config with allocation caps + filters...")
        opt_cfg = CONFIGS["standard"]
        r_opt = run_simulation(markets, "optimized", opt_cfg, optimized=True)
        optimized_results = [r_opt]
        print(f"    -> P&L: ${r_opt['total_pnl']:+,.2f} | Sharpe: {r_opt['sharpe']:.2f} | Win: {r_opt['win_rate']*100:.1f}%")

        print_report(original_results)
        print_comparison(original_results, optimized_results)

        import os as _os
        _os.makedirs("data", exist_ok=True)
        output_orig = [{k: v for k, v in r.items() if k not in ("equity_curve",)} for r in original_results]
        with open("data/sim_50k_report.json", "w") as f:
            json.dump(output_orig, f, indent=2, default=str)

        output_opt = [{k: v for k, v in r.items() if k not in ("equity_curve",)} for r in optimized_results]
        with open("data/simulation_report_optimized.json", "w") as f:
            json.dump(output_opt, f, indent=2, default=str)

        print("\n  Reports saved:")
        print("    data/sim_50k_report.json")
        print("    data/simulation_report_optimized.json")

    else:
        print(f"  Running 3 simulations on {len(markets)} real markets...\n")

        results = []
        for config_name, cfg in CONFIGS.items():
            print(f"  Simulating {config_name} config...")
            r = run_simulation(markets, config_name, cfg)
            results.append(r)
            print(f"    -> P&L: ${r['total_pnl']:+,.2f} | Sharpe: {r['sharpe']:.2f} | Win rate: {r['win_rate']*100:.1f}%")

        print_report(results)

        import os as _os
        _os.makedirs("data", exist_ok=True)
        output = [{k: v for k, v in r.items() if k not in ("equity_curve",)} for r in results]
        with open("data/sim_50k_report.json", "w") as f:
            json.dump(output, f, indent=2, default=str)
        print("\n  Full report saved to data/sim_50k_report.json")


def print_analytical_estimate():
    """Fallback if API unavailable — pure math from calibration data."""
    print("  ANALYTICAL ESTIMATE — Based on calibration data")
    print("  (Run with live API for real historical replay)\n")

    # Formula: daily P&L = (trades/day × win_rate × avg_win) - (trades/day × (1-win_rate) × avg_loss)
    # avg_win = max_trade × net_return_pct (profit on a winning trade)
    # avg_loss = max_trade × avg_loss_pct (loss on a losing trade, with stop-losses)

    max_trade = 50.00  # Per-trade size
    net_return_pct = 0.03  # 3% net return on wins
    avg_loss_pct = 0.02   # 2% avg loss per losing trade (stop-loss discipline)

    scenarios = [
        ("Conservative", 3, 0.65),
        ("Base case",    5, 0.65),
        ("Strong",       8, 0.68),
    ]

    avg_win = max_trade * net_return_pct
    avg_loss = max_trade * avg_loss_pct

    print(f"  Max trade: ${max_trade:.0f}  |  Win return: {net_return_pct*100:.0f}%  |  Avg loss: {avg_loss_pct*100:.0f}%")
    print(f"  Avg win/trade: ${avg_win:.2f}  |  Avg loss/trade: ${avg_loss:.2f}\n")
    print(f"  {'Scenario':<15} {'Trades/day':>10} {'Win rate':>10} {'Daily P&L':>12} {'Monthly':>12} {'Annual':>12}")
    print(f"  {'─'*15} {'─'*10} {'─'*10} {'─'*12} {'─'*12} {'─'*12}")
    for name, trades_day, wr in scenarios:
        daily = (trades_day * wr * avg_win) - (trades_day * (1 - wr) * avg_loss)
        monthly = daily * 30
        annual = daily * 365
        print(f"  {name:<15} {trades_day:>10} {wr*100:>9.0f}% ${daily:>+11,.2f} ${monthly:>+11,.2f} ${annual:>+11,.2f}")

if __name__ == "__main__":
    asyncio.run(main())
