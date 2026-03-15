#!/usr/bin/env python3
"""
Historical Basis Analyzer — BTC & ETH futures carry trade backtest.

Pulls 2 years of data from Binance APIs (with synthetic fallback),
simulates carry trades at various leverage/threshold combos,
and outputs carry_analysis_report.json for the live strategy module.
"""

import json
import math
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Tuple, Dict

import httpx
import numpy as np
import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parent
REPORT_PATH = OUTPUT_DIR / "carry_analysis_report.json"

# ── Data fetching ────────────────────────────────────────────────────────────

def fetch_binance_klines(symbol: str, interval: str = "1d", limit: int = 730) -> Optional[pd.DataFrame]:
    """Fetch daily OHLCV from Binance Futures API."""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        resp = httpx.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_vol", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        df["close"] = df["close"].astype(float)
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["date"] = pd.to_datetime(df["open_time"], unit="ms")
        return df[["date", "open", "high", "low", "close", "volume"]].copy()
    except Exception as e:
        print(f"  [WARN] Binance API failed for {symbol}: {e}")
        return None


def fetch_spot_klines(symbol: str, limit: int = 730) -> Optional[pd.DataFrame]:
    """Fetch daily spot OHLCV from Binance spot API."""
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": "1d", "limit": limit}
    try:
        resp = httpx.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_vol", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        df["close"] = df["close"].astype(float)
        df["date"] = pd.to_datetime(df["open_time"], unit="ms")
        return df[["date", "close"]].rename(columns={"close": "spot_close"}).copy()
    except Exception as e:
        print(f"  [WARN] Binance spot API failed for {symbol}: {e}")
        return None


def fetch_funding_rate_history(symbol: str, limit: int = 1000) -> Optional[pd.DataFrame]:
    """Fetch funding rate history as additional basis proxy."""
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    params = {"symbol": symbol, "limit": limit}
    try:
        resp = httpx.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)
        df["fundingRate"] = df["fundingRate"].astype(float)
        df["date"] = pd.to_datetime(df["fundingTime"], unit="ms")
        # Aggregate to daily
        df["date"] = df["date"].dt.date
        daily = df.groupby("date")["fundingRate"].sum().reset_index()
        daily.columns = ["date", "daily_funding_rate"]
        daily["date"] = pd.to_datetime(daily["date"])
        return daily
    except Exception as e:
        print(f"  [WARN] Funding rate API failed for {symbol}: {e}")
        return None


# ── Synthetic data generation ────────────────────────────────────────────────

def generate_synthetic_data(asset: str, days: int = 730) -> pd.DataFrame:
    """Generate realistic synthetic basis data using documented historical ranges."""
    np.random.seed(42 if asset == "BTC" else 99)

    # Market cycle parameters
    cycles = {"bear": 120, "neutral": 90, "bull": 60}
    basis_ranges = {
        "BTC": {"bear": (2, 8), "neutral": (8, 18), "bull": (18, 55)},
        "ETH": {"bear": (3, 10), "neutral": (10, 22), "bull": (22, 65)},
    }[asset]

    # Generate spot price path (geometric brownian motion)
    start_price = {"BTC": 30000, "ETH": 1800}[asset]
    daily_vol = 0.025  # ~2.5% daily vol
    drift = 0.0002
    prices = [start_price]
    for _ in range(days - 1):
        ret = drift + daily_vol * np.random.randn()
        prices.append(prices[-1] * (1 + ret))

    # Generate basis following market cycles
    basis_aprs = []
    day = 0
    cycle_order = ["bear", "neutral", "bull"]
    cycle_idx = 0
    while day < days:
        regime = cycle_order[cycle_idx % 3]
        cycle_len = cycles[regime] + np.random.randint(-15, 15)
        low, high = basis_ranges[regime]
        mean_basis = (low + high) / 2
        noise_scale = mean_basis * 0.15  # 15% of mean

        for _ in range(int(cycle_len)):
            if day >= days:
                break
            basis = mean_basis + noise_scale * np.random.randn()
            basis = max(basis, low * 0.5)  # allow some dips below range
            basis_aprs.append(basis)
            day += 1
        cycle_idx += 1

    dates = pd.date_range(end=datetime.now(timezone.utc).date(), periods=days, freq="D")
    spot_prices = np.array(prices[:days])
    basis_aprs = np.array(basis_aprs[:days])

    # Derive futures prices from basis
    futures_prices = spot_prices * (1 + basis_aprs / 100 * 90 / 365)

    return pd.DataFrame({
        "date": dates,
        "spot_close": spot_prices,
        "futures_close": futures_prices,
        "basis_apr": basis_aprs,
    })


# ── Data assembly ────────────────────────────────────────────────────────────

def build_dataset(asset: str) -> Tuple[pd.DataFrame, str]:
    """Try Binance APIs, fall back to synthetic."""
    symbol = f"{asset}USDT"
    print(f"\n  Fetching {asset} data from Binance...")

    spot_df = fetch_spot_klines(symbol)
    futures_df = fetch_binance_klines(symbol)

    if spot_df is not None and futures_df is not None and len(spot_df) > 100 and len(futures_df) > 100:
        # Merge on date
        futures_df = futures_df.rename(columns={"close": "futures_close"})
        merged = pd.merge(
            spot_df, futures_df[["date", "futures_close"]],
            on="date", how="inner"
        )
        if len(merged) > 100:
            # Compute basis APR (90-day quarterly proxy)
            merged["basis_apr"] = (
                (merged["futures_close"] - merged["spot_close"]) / merged["spot_close"]
            ) * (365 / 90) * 100
            print(f"  ✓ Got {len(merged)} days of {asset} data from Binance API")
            return merged[["date", "spot_close", "futures_close", "basis_apr"]].copy(), "binance_api"

    # Try using funding rate as basis proxy
    funding_df = fetch_funding_rate_history(symbol)
    if spot_df is not None and funding_df is not None:
        spot_df["date_key"] = spot_df["date"].dt.date
        funding_df["date_key"] = funding_df["date"].dt.date
        merged = pd.merge(spot_df, funding_df, on="date_key", how="inner", suffixes=("", "_fr"))
        if len(merged) > 100:
            # Convert daily funding to annualized basis
            merged["basis_apr"] = merged["daily_funding_rate"] * 365 * 100
            merged["futures_close"] = merged["spot_close"] * (1 + merged["daily_funding_rate"] * 90)
            merged = merged.rename(columns={"date": "date_final"})
            merged["date"] = pd.to_datetime(merged["date_key"])
            print(f"  ✓ Got {len(merged)} days of {asset} funding rate data from Binance")
            return merged[["date", "spot_close", "futures_close", "basis_apr"]].copy(), "binance_api"

    # Fallback: synthetic
    print(f"  ⚠ Using synthetic data for {asset}")
    return generate_synthetic_data(asset), "synthetic"


# ── Analysis ─────────────────────────────────────────────────────────────────

def classify_regime(basis_series: pd.Series) -> pd.Series:
    """Classify each day: bear (bottom 25%), neutral (middle 50%), bull (top 25%)."""
    q25 = basis_series.quantile(0.25)
    q75 = basis_series.quantile(0.75)
    conditions = [
        basis_series <= q25,
        basis_series >= q75,
    ]
    choices = ["bear", "neutral"]  # default neutral
    return pd.Series(
        np.select(conditions, ["bear", "bull"], default="neutral"),
        index=basis_series.index
    )


def simulate_carry_trade(df: pd.DataFrame, leverage: float = 1.0,
                         entry_threshold: float = 8.0,
                         fee_pct: float = 0.4) -> dict:
    """Simulate carry trade with given parameters."""
    basis = df["basis_apr"].values
    spot = df["spot_close"].values
    n = len(df)

    trades = []
    margin_call_days = 0
    total_trade_days = 0

    i = 0
    while i < n - 90:
        if basis[i] >= entry_threshold:
            # Enter trade: long spot, short futures
            entry_spot = spot[i]
            entry_basis = basis[i]
            hold_days = min(90, n - i - 1)

            # Track margin health during hold
            trade_margin_calls = 0
            max_drawdown_spot = 0
            for j in range(1, hold_days + 1):
                price_drop = (entry_spot - spot[i + j]) / entry_spot
                max_drawdown_spot = max(max_drawdown_spot, price_drop)

                if leverage > 1:
                    margin_threshold = 0.5 / leverage
                    if price_drop >= margin_threshold:
                        trade_margin_calls += 1

            total_trade_days += hold_days

            # P&L: basis captured over hold period
            exit_basis = basis[min(i + hold_days, n - 1)]
            # Carry P&L = entry_basis annualized, prorated to hold period
            carry_return_pct = entry_basis * hold_days / 365
            fee_cost_pct = fee_pct
            net_return_pct = (carry_return_pct * leverage) - fee_cost_pct

            trades.append({
                "entry_day": i,
                "entry_basis": entry_basis,
                "exit_basis": exit_basis,
                "hold_days": hold_days,
                "net_return_pct": net_return_pct,
                "max_dd_spot": max_drawdown_spot,
                "margin_calls": trade_margin_calls,
            })
            margin_call_days += trade_margin_calls

            # Skip to end of this trade (no overlapping)
            i += hold_days
        else:
            i += 1

    if not trades:
        return {
            "trades_count": 0, "win_rate": 0, "avg_return": 0,
            "max_drawdown": 0, "sharpe": 0, "annual_return": 0,
            "margin_call_pct": 0,
        }

    returns = [t["net_return_pct"] for t in trades]
    wins = sum(1 for r in returns if r > 0)

    # Annualize: total return over period, then scale
    total_return = sum(returns)
    total_days = sum(t["hold_days"] for t in trades)
    annual_return = total_return * (365 / max(total_days, 1))

    # Sharpe: annualized
    if len(returns) > 1 and np.std(returns) > 0:
        sharpe = (np.mean(returns) / np.std(returns)) * math.sqrt(365 / 90)
    else:
        sharpe = 0

    # Max drawdown from cumulative equity curve
    cum_returns = np.cumsum(returns)
    running_max = np.maximum.accumulate(cum_returns)
    drawdowns = running_max - cum_returns
    max_dd = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0

    margin_call_pct = (margin_call_days / total_trade_days * 100) if total_trade_days > 0 else 0

    return {
        "trades_count": len(trades),
        "win_rate": wins / len(trades) * 100,
        "avg_return": float(np.mean(returns)),
        "max_drawdown": max_dd,
        "sharpe": round(sharpe, 2),
        "annual_return": round(annual_return, 2),
        "margin_call_pct": round(margin_call_pct, 2),
    }


def find_optimal_threshold(df: pd.DataFrame) -> Tuple[float, dict]:
    """Test entry thresholds from 5-25% APR, find optimal."""
    best_score = -999
    best_threshold = 8.0
    results = {}

    for thresh in range(5, 26):
        sim_1x = simulate_carry_trade(df, leverage=1.0, entry_threshold=float(thresh))
        sim_2x = simulate_carry_trade(df, leverage=2.0, entry_threshold=float(thresh))

        # Score: (trades × avg_return) - (margin_call_pct × 2)
        score = (sim_1x["trades_count"] * sim_1x["avg_return"]) - (sim_2x["margin_call_pct"] * 2)
        results[thresh] = {
            "score": round(score, 3),
            "trades_1x": sim_1x["trades_count"],
            "avg_ret_1x": round(sim_1x["avg_return"], 3),
            "margin_calls_2x": sim_2x["margin_call_pct"],
        }
        if score > best_score:
            best_score = score
            best_threshold = float(thresh)

    return best_threshold, results


def analyze_asset(asset: str) -> Tuple[dict, str]:
    """Full analysis for one asset."""
    df, source = build_dataset(asset)
    basis = df["basis_apr"]

    # Classify regimes
    df["regime"] = classify_regime(basis)

    # Basic stats
    median_basis = float(basis.median())
    bull_p75 = float(basis[df["regime"] == "bull"].quantile(0.75)) if (df["regime"] == "bull").any() else 0
    bear_p25 = float(basis[df["regime"] == "bear"].quantile(0.25)) if (df["regime"] == "bear").any() else 0
    negative_pct = float((basis < 0).sum() / len(basis) * 100)
    days_above_8 = int((basis >= 8).sum())
    days_above_15 = int((basis >= 15).sum())

    # Simulations
    sim_1x = simulate_carry_trade(df, leverage=1.0, entry_threshold=8.0)
    sim_2x = simulate_carry_trade(df, leverage=2.0, entry_threshold=8.0)

    # Optimal threshold
    optimal_thresh, _ = find_optimal_threshold(df)

    result = {
        "median_basis_apr": round(median_basis, 2),
        "bull_market_basis_apr_p75": round(bull_p75, 2),
        "bear_market_basis_apr_p25": round(bear_p25, 2),
        "negative_basis_pct": round(negative_pct, 2),
        "days_above_8pct": days_above_8,
        "days_above_15pct": days_above_15,
        "sim_1x_annual_return_pct": sim_1x["annual_return"],
        "sim_1x_sharpe": sim_1x["sharpe"],
        "sim_1x_max_drawdown_pct": round(sim_1x["max_drawdown"], 2),
        "sim_2x_annual_return_pct": sim_2x["annual_return"],
        "sim_2x_margin_call_pct": sim_2x["margin_call_pct"],
        "optimal_entry_threshold_apr": optimal_thresh,
    }

    return result, source


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  CARRY TRADE HISTORICAL BASIS ANALYZER")
    print("=" * 60)

    btc_result, btc_source = analyze_asset("BTC")
    eth_result, eth_source = analyze_asset("ETH")

    data_source = btc_source if btc_source == eth_source else "mixed"

    # Recommendations: weighted average of both assets
    avg_optimal = (btc_result["optimal_entry_threshold_apr"] + eth_result["optimal_entry_threshold_apr"]) / 2
    recommended_threshold = round(avg_optimal)

    # Max leverage: if 2x margin call rate > 5%, recommend 1x
    avg_margin_call = (btc_result["sim_2x_margin_call_pct"] + eth_result["sim_2x_margin_call_pct"]) / 2
    if avg_margin_call > 5:
        recommended_leverage = 1.0
    elif avg_margin_call > 2:
        recommended_leverage = 1.5
    else:
        recommended_leverage = 2.0

    # Margin buffer: higher if margin calls are frequent
    if avg_margin_call > 3:
        recommended_buffer = 50.0
    elif avg_margin_call > 1:
        recommended_buffer = 35.0
    else:
        recommended_buffer = 25.0

    # Summary
    avg_sharpe = (btc_result["sim_1x_sharpe"] + eth_result["sim_1x_sharpe"]) / 2
    summary = (
        f"Based on {'Binance historical' if 'binance' in data_source else 'synthetic'} data, "
        f"the carry trade shows a {avg_sharpe:.1f} Sharpe at 1x leverage with "
        f"an optimal entry threshold of {recommended_threshold}% APR. "
        f"Recommended leverage: {recommended_leverage}x with {recommended_buffer}% margin buffer. "
        f"{'Use caution: 2x leverage shows significant margin call risk.' if avg_margin_call > 3 else 'Moderate leverage is viable with proper margin management.'}"
    )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_source": data_source,
        "btc": btc_result,
        "eth": eth_result,
        "recommended_entry_threshold_apr": float(recommended_threshold),
        "recommended_max_leverage": recommended_leverage,
        "recommended_margin_buffer_pct": recommended_buffer,
        "summary": summary,
    }

    # Save report
    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n  ✓ Report saved to {REPORT_PATH}")

    # Print ASCII summary
    print("\n" + "=" * 70)
    print("  CARRY TRADE ANALYSIS RESULTS")
    print("=" * 70)
    print(f"  Data Source: {data_source}")
    print(f"  Generated:  {report['generated_at']}")
    print()

    header = f"  {'Metric':<35} {'BTC':>12} {'ETH':>12}"
    print(header)
    print("  " + "-" * 59)

    metrics = [
        ("Median Basis APR %", "median_basis_apr"),
        ("Bull Mkt Basis P75 %", "bull_market_basis_apr_p75"),
        ("Bear Mkt Basis P25 %", "bear_market_basis_apr_p25"),
        ("Negative Basis Days %", "negative_basis_pct"),
        ("Days Above 8% APR", "days_above_8pct"),
        ("Days Above 15% APR", "days_above_15pct"),
        ("1x Annual Return %", "sim_1x_annual_return_pct"),
        ("1x Sharpe Ratio", "sim_1x_sharpe"),
        ("1x Max Drawdown %", "sim_1x_max_drawdown_pct"),
        ("2x Annual Return %", "sim_2x_annual_return_pct"),
        ("2x Margin Call %", "sim_2x_margin_call_pct"),
        ("Optimal Entry Thresh %", "optimal_entry_threshold_apr"),
    ]

    for label, key in metrics:
        bv = btc_result[key]
        ev = eth_result[key]
        if isinstance(bv, int):
            print(f"  {label:<35} {bv:>12d} {ev:>12d}")
        else:
            print(f"  {label:<35} {bv:>12.2f} {ev:>12.2f}")

    print()
    print("  " + "-" * 59)
    print(f"  {'Recommended Entry Threshold':<35} {recommended_threshold:>12.0f}%")
    print(f"  {'Recommended Max Leverage':<35} {recommended_leverage:>12.1f}x")
    print(f"  {'Recommended Margin Buffer':<35} {recommended_buffer:>12.0f}%")
    print()
    print(f"  Summary: {summary}")
    print("=" * 70)


if __name__ == "__main__":
    main()
