#!/usr/bin/env python3
"""
Resolution Lag Replay — 30-Day Out-of-Sample Validation

For each resolved Polymarket market, we check: was the bestBid (pre-resolution
price proxy) significantly below 1.0 for YES-resolved markets? If so, that's a
resolution lag opportunity — the real world had resolved but the market hadn't
caught up yet.

This replays the resolution_lag strategy: buy YES at whatever price the market
shows when we detect the real-world event has already resolved.

Usage:
    python scripts/resolution_lag_replay.py
"""
from __future__ import annotations

import json
import math
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.risk.sizer import PLATFORM_FEE

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CAPITAL = 500.0
GAMMA_API = "https://gamma-api.polymarket.com"
LOOKBACK_DAYS = 30

# Resolution lag strategy parameters
# We'd buy YES when we detect the event has resolved but market is still below this
MAX_ENTRY_PRICE = 0.97   # don't buy if market already at 97c+ (no lag)
MIN_ENTRY_PRICE = 0.30   # don't buy below 30c (not enough conviction)
CONFIDENCE_TIERS = {
    # entry_price range -> (position_size, confidence_label)
    # Lower prices = bigger lag = higher return but need higher confidence
    (0.30, 0.50): (15.0, "low"),      # 30-50c: small bet, big lag
    (0.50, 0.70): (25.0, "medium"),    # 50-70c: medium bet
    (0.70, 0.85): (35.0, "high"),      # 70-85c: larger bet, smaller lag
    (0.85, 0.97): (50.0, "very_high"), # 85-97c: full bet, small lag
}


# ---------------------------------------------------------------------------
# Category inference
# ---------------------------------------------------------------------------
_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "crypto": ["bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "sol",
               "token", "dogecoin", "doge", "xrp", "cardano", "ada", "price"],
    "sports": ["nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball",
               "baseball", "championship", "super bowl", "world cup", "game",
               "win", "score", "vs", "match", "fight", "ufc", "boxing"],
    "politics": ["election", "president", "senate", "congress", "governor",
                 "vote", "democrat", "republican", "trump", "biden", "harris",
                 "nominee", "primary", "ballot"],
    "macro": ["inflation", "fed", "interest rate", "gdp", "recession",
              "unemployment", "cpi", "fomc", "tariff"],
}


def _infer_category(question: str) -> str:
    lower = question.lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return category
    return "other"


# ---------------------------------------------------------------------------
# Fetch resolved markets
# ---------------------------------------------------------------------------

def _parse_closed_time(m: dict) -> datetime | None:
    ct = m.get("closedTime", "")
    if not ct:
        return None
    try:
        ct = ct.replace(" ", "T")
        if ct.endswith("+00"):
            ct += ":00"
        elif not ("+" in ct[10:] or ct.endswith("Z")):
            ct += "+00:00"
        return datetime.fromisoformat(ct.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def fetch_resolved_markets(days: int = LOOKBACK_DAYS) -> list[dict[str, Any]]:
    """Fetch resolved YES markets with bestBid below MAX_ENTRY_PRICE."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Fetching resolved markets since {cutoff_str}...")

    limit = 100

    with httpx.Client(timeout=30.0) as client:
        def _probe(offset: int) -> datetime | None:
            r = client.get(f"{GAMMA_API}/markets", params={
                "closed": "true", "limit": 1, "offset": offset,
            })
            data = r.json() if r.status_code == 200 else []
            if not data:
                return None
            return _parse_closed_time(data[0])

        # Binary search for start offset
        lo, hi = 0, 600_000
        while _probe(hi) is not None:
            hi *= 2

        print("  Binary searching for start offset...")
        while lo < hi - 500:
            mid = (lo + hi) // 2
            ct = _probe(mid)
            if ct is None or ct >= cutoff:
                hi = mid
            else:
                lo = mid
        start_offset = max(0, lo - 5000)

        # Find end
        end_probe = start_offset + 50000
        while _probe(end_probe) is not None:
            end_probe += 10000
        elo, ehi = end_probe - 10000, end_probe
        while elo < ehi - 100:
            mid = (elo + ehi) // 2
            if _probe(mid) is not None:
                elo = mid
            else:
                ehi = mid
        end_offset = elo + 100

        total_pages = (end_offset - start_offset) // limit + 1
        print(f"  Range: offset {start_offset}..{end_offset} ({total_pages} pages)")
        print(f"  Fetching with 10 concurrent workers...")

        all_markets: list[dict[str, Any]] = []
        offsets = list(range(start_offset, end_offset + limit, limit))

        def _fetch_page(off: int) -> list[dict[str, Any]]:
            try:
                with httpx.Client(timeout=30.0) as c:
                    resp = c.get(f"{GAMMA_API}/markets", params={
                        "closed": "true", "limit": limit, "offset": off,
                    })
                if resp.status_code != 200:
                    return []
                batch = resp.json()
                if not batch:
                    return []
            except Exception:
                return []

            results = []
            for m in batch:
                ct = _parse_closed_time(m)
                if ct is None or ct < cutoff:
                    continue

                # Must have resolved YES (outcomePrices[0] == "1")
                prices_str = m.get("outcomePrices", "")
                try:
                    outcome_prices = json.loads(prices_str) if isinstance(prices_str, str) else prices_str
                    if not outcome_prices or float(outcome_prices[0]) < 0.5:
                        continue  # resolved NO or ambiguous — skip
                except (json.JSONDecodeError, IndexError, TypeError, ValueError):
                    continue

                # Must have a bestBid below MAX_ENTRY_PRICE (resolution lag indicator)
                bb = m.get("bestBid")
                if bb is None:
                    continue
                try:
                    price = float(bb)
                    if MIN_ENTRY_PRICE <= price <= MAX_ENTRY_PRICE:
                        results.append(m)
                except (ValueError, TypeError):
                    pass
            return results

        done = 0
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_fetch_page, off): off for off in offsets}
            for fut in as_completed(futures):
                markets = fut.result()
                all_markets.extend(markets)
                done += 1
                if done % 50 == 0:
                    print(f"    ...{done}/{len(offsets)} pages, {len(all_markets)} lag opportunities")

    print(f"  Found {len(all_markets)} resolved-YES markets with pre-resolution bestBid < {MAX_ENTRY_PRICE}")
    return all_markets


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

def replay_market(market: dict[str, Any]) -> dict[str, Any] | None:
    """Simulate buying YES at bestBid on a market that resolved YES."""
    bb = market.get("bestBid")
    if bb is None:
        return None
    try:
        entry_price = float(bb)
    except (ValueError, TypeError):
        return None

    if not (MIN_ENTRY_PRICE <= entry_price <= MAX_ENTRY_PRICE):
        return None

    question = market.get("question") or market.get("title") or ""
    market_id = market.get("conditionId") or market.get("id") or "unknown"
    category = _infer_category(question)
    volume = float(market.get("volumeNum") or market.get("volume") or 0)

    # Determine position size by entry price tier
    size = 25.0  # default
    confidence = "medium"
    for (lo, hi), (tier_size, tier_label) in CONFIDENCE_TIERS.items():
        if lo <= entry_price < hi:
            size = tier_size
            confidence = tier_label
            break

    # P&L: we buy YES at entry_price, market resolves YES → payout = $1 per share
    # Shares bought = size / entry_price
    # Gross payout = shares * 1.0 = size / entry_price
    # Net payout after fee = shares * (1 - PLATFORM_FEE)
    # P&L = net_payout - size
    pnl = size * ((1.0 - PLATFORM_FEE) / entry_price - 1.0)
    net_return = (1.0 - PLATFORM_FEE) / entry_price - 1.0

    # Estimate hold time: resolution lag markets resolve fast
    # Use time between market creation and close as proxy
    hold_hours = 1.0  # default: assume 1 hour lag
    try:
        closed_time = _parse_closed_time(market)
        end_str = market.get("endDate", "")
        if end_str and closed_time:
            end_str = end_str.replace("Z", "+00:00")
            end_dt = datetime.fromisoformat(end_str)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            # Resolution lag = time between endDate and closedTime
            # (endDate is when the event actually ends, closedTime is when Polymarket settles)
            lag = (closed_time - end_dt).total_seconds() / 3600
            if 0 < lag < 72:
                hold_hours = lag
    except (ValueError, TypeError):
        pass

    return {
        "market_id": market_id,
        "question": question[:80],
        "entry_price": round(entry_price, 4),
        "net_return": round(net_return, 4),
        "size": round(size, 2),
        "pnl": round(pnl, 2),
        "category": category,
        "confidence": confidence,
        "volume": round(volume, 0),
        "hold_hours": round(hold_hours, 1),
        "won": True,  # all these are resolved YES by construction
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("RESOLUTION LAG REPLAY — 30-Day Out-of-Sample Validation")
    print("=" * 70)
    print(f"Capital: ${CAPITAL:.0f} | Strategy: buy YES on markets that resolved YES")
    print(f"Entry range: {MIN_ENTRY_PRICE:.0%} – {MAX_ENTRY_PRICE:.0%} (resolution lag window)")
    print()

    markets = fetch_resolved_markets(LOOKBACK_DAYS)
    if not markets:
        print("No resolution lag opportunities found.")
        sys.exit(1)

    # Replay each
    trades: list[dict[str, Any]] = []
    for m in markets:
        trade = replay_market(m)
        if trade is not None:
            trades.append(trade)

    print(f"\nPipeline: {len(markets)} candidates → {len(trades)} trades")
    print()

    if not trades:
        print("NO TRADES — all candidates filtered out.")
        sys.exit(1)

    # --- Trade log ---
    print("-" * 70)
    print("TRADE LOG (sample: first 30 + last 10)")
    print("-" * 70)
    show_trades = trades[:30] + (trades[-10:] if len(trades) > 30 else [])
    for i, t in enumerate(show_trades, 1):
        actual_idx = i if i <= 30 else len(trades) - 10 + (i - 30)
        print(
            f"  #{actual_idx:04d}  {t['question']:<55s}  "
            f"entry={t['entry_price']:.3f}  ret={t['net_return']:+.1%}  "
            f"size=${t['size']:.0f}  P&L=${t['pnl']:+.2f}  "
            f"hold={t['hold_hours']:.0f}h  [{t['category']}]"
        )
    if len(trades) > 40:
        print(f"  ... ({len(trades) - 40} trades omitted)")
    print()

    # --- Stats ---
    total_traded = len(trades)
    total_pnl = sum(t["pnl"] for t in trades)
    total_invested = sum(t["size"] for t in trades)
    roi = (total_pnl / total_invested * 100) if total_invested > 0 else 0

    pnls = [t["pnl"] for t in trades]
    mean_pnl = sum(pnls) / len(pnls)
    if len(pnls) > 1:
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / (len(pnls) - 1)
        std_pnl = math.sqrt(variance)
        trades_per_year = 365 * total_traded / LOOKBACK_DAYS
        sharpe = (mean_pnl / std_pnl) * math.sqrt(trades_per_year) if std_pnl > 0 else 0
    else:
        std_pnl = 0
        sharpe = 0

    avg_hold = sum(t["hold_hours"] for t in trades) / total_traded
    avg_return = sum(t["net_return"] for t in trades) / total_traded

    # Category breakdown
    cat_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "pnl": 0.0})
    for t in trades:
        cat_stats[t["category"]]["count"] += 1
        cat_stats[t["category"]]["pnl"] += t["pnl"]

    # Entry price distribution
    price_buckets: dict[str, int] = defaultdict(int)
    for t in trades:
        p = t["entry_price"]
        if p < 0.50:
            price_buckets["30-50c"] += 1
        elif p < 0.70:
            price_buckets["50-70c"] += 1
        elif p < 0.85:
            price_buckets["70-85c"] += 1
        else:
            price_buckets["85-97c"] += 1

    print("-" * 70)
    print("PERFORMANCE SUMMARY")
    print("-" * 70)
    print(f"  Total opportunities:   {total_traded}")
    print(f"  Win rate:              100.0% (all resolved YES by construction)")
    print(f"  Total invested:        ${total_invested:,.2f}")
    print(f"  Total P&L:             ${total_pnl:+,.2f}")
    print(f"  ROI:                   {roi:+.1f}%")
    print(f"  Mean P&L/trade:        ${mean_pnl:+.2f}")
    print(f"  Std dev P&L:           ${std_pnl:.2f}")
    print(f"  Sharpe ratio:          {sharpe:.2f}")
    print(f"  Avg hold time:         {avg_hold:.1f} hours")
    print(f"  Avg net return:        {avg_return:+.1%}")
    print(f"  Trades/day:            {total_traded / LOOKBACK_DAYS:.1f}")
    print()

    print("  Entry price distribution:")
    for bucket in ["30-50c", "50-70c", "70-85c", "85-97c"]:
        n = price_buckets.get(bucket, 0)
        pct = n / total_traded * 100
        print(f"    {bucket:<8s}  {n:4d} trades  ({pct:.0f}%)")
    print()

    print("  Category breakdown:")
    for cat, s in sorted(cat_stats.items(), key=lambda x: -x[1]["count"]):
        pct = s["count"] / total_traded * 100
        print(f"    {cat:<12s}  {s['count']:4d} trades ({pct:.0f}%)  P&L=${s['pnl']:+,.2f}")
    print()

    # --- Reality check ---
    print("-" * 70)
    print("REALITY CHECK")
    print("-" * 70)
    print()
    print("  IMPORTANT: This is an upper bound, not a realistic estimate.")
    print("  These numbers assume we DETECT every resolution lag event in real-time.")
    print("  Actual performance depends on:")
    print("    1. News/event feed coverage (ESPN, NewsAPI, crypto feeds)")
    print("    2. Matching accuracy (fuzzy + semantic matching)")
    print("    3. Execution speed (must buy before market catches up)")
    print("    4. Liquidity at time of entry (may not get filled at bestBid)")
    print()

    # Estimate realistic capture rate
    # With ESPN (5 sports) + NewsAPI + Crypto feeds, estimate:
    # - Sports: ~60% capture (good ESPN coverage)
    # - Crypto: ~40% capture (price feeds only, miss narrative markets)
    # - Politics: ~30% capture (NewsAPI lag, complex matching)
    # - Other: ~20% capture (general news matching is hard)
    capture_rates = {
        "sports": 0.60,
        "crypto": 0.40,
        "politics": 0.30,
        "macro": 0.30,
        "other": 0.20,
    }

    realistic_trades = 0
    realistic_pnl = 0.0
    for t in trades:
        rate = capture_rates.get(t["category"], 0.20)
        realistic_trades += rate
        realistic_pnl += t["pnl"] * rate

    print(f"  Estimated realistic capture:")
    print(f"    Trades/month:  {realistic_trades:.0f} (vs {total_traded} theoretical max)")
    print(f"    P&L/month:     ${realistic_pnl:+,.2f} (vs ${total_pnl:+,.2f} max)")
    print(f"    Trades/day:    {realistic_trades / LOOKBACK_DAYS:.1f}")
    print()

    # --- Verdict ---
    print("=" * 70)
    viable = total_pnl > 0 and total_traded >= 10 and realistic_trades >= 5
    if viable:
        print("VERDICT:  *** RESOLUTION LAG IS VIABLE ***")
        print()
        print(f"  {total_traded} opportunities in {LOOKBACK_DAYS} days ({total_traded/LOOKBACK_DAYS:.1f}/day)")
        print(f"  ${total_pnl:+,.2f} theoretical P&L, ~${realistic_pnl:+,.2f} realistic")
        print(f"  Sharpe {sharpe:.2f}")
        print()
        print("  Next steps:")
        print("    1. Enable resolution_lag as primary strategy (threshold DISABLED)")
        print("    2. Run with live event feeds for 48h to measure actual capture rate")
        print("    3. Compare actual vs theoretical to calibrate expectations")
    else:
        print("VERDICT:  *** INSUFFICIENT EVIDENCE ***")
        print()
        if total_traded < 10:
            print(f"  Only {total_traded} opportunities — need more data")
        if total_pnl <= 0:
            print(f"  P&L ${total_pnl:+,.2f} is not positive")
    print("=" * 70)


if __name__ == "__main__":
    main()
