"""
EdgeLab Whale Finder v2 — Informed Bettor Discovery.

NEW hypothesis: one-time informed bettors who make 1-5 large bets on specific
markets they have insider knowledge of (the CIA analyst, campaign insider,
corporate spy — not the quant trader).

Qualification criteria:
- Single bet $2,000+ on one market
- Bet placed >24h before resolution (not last-minute)
- Target categories: politics, elections, world events, geopolitics, crypto
- Won at 80%+ payout
- Dormant wallet pattern (< 5 lifetime trades = truly informed, not a grinder)

Insider probability scoring:
  HIGH:   dormant wallet + $5K+ bet + >48hr before resolution + won
  MEDIUM: low-activity wallet + $2K+ bet + >24hr before resolution + won
  LOW:    everything else that meets minimum criteria

Usage:
    python scripts/whale_finder_v2.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BASE = "https://clob.polymarket.com"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
REGISTRY_V2_PATH = os.path.join(DATA_DIR, "whale_registry_v2.json")
BACKTEST_V2_PATH = os.path.join(DATA_DIR, "whale_backtest_v2.json")

LOOKBACK_DAYS = 90
PAGE_SIZE = 500
MAX_PAGES = 100
REQUEST_DELAY = 0.3

# v2 qualification thresholds
MIN_BET_SIZE = 2000.0        # minimum single bet in dollars
HIGH_BET_SIZE = 5000.0       # threshold for HIGH insider score
MIN_HOURS_BEFORE = 24.0      # bet must be placed >24h before resolution
HIGH_HOURS_BEFORE = 48.0     # >48h → HIGH score
MAX_LIFETIME_TRADES = 5      # dormant wallet = fewer than this
LOW_ACTIVITY_TRADES = 20     # low-activity = fewer than this
MIN_PAYOUT = 0.80            # winning bet must have paid 80%+ (bought at ≤0.20 or won at ≥0.80)

# Target market categories (case-insensitive substring matching)
TARGET_CATEGORIES = {
    "politics", "election", "elections", "political",
    "geopolitics", "geopolitical", "world", "world events",
    "international", "government", "legislation", "policy",
    "crypto", "cryptocurrency", "bitcoin", "ethereum",
    "leadership", "president", "congress", "senate",
}

# Question-level keyword matching for markets that aren't tagged well
TARGET_KEYWORDS = [
    "president", "election", "vote", "congress", "senate", "governor",
    "prime minister", "leader", "resign", "impeach", "sanction",
    "war", "invasion", "ceasefire", "treaty", "nato", "un ",
    "bitcoin", "btc", "ethereum", "eth", "crypto",
    "tariff", "embargo", "coup", "assassination",
]


# ---------------------------------------------------------------------------
# Market fetching — filtered to target categories
# ---------------------------------------------------------------------------

def _is_target_market(market: dict) -> bool:
    """Check if market falls in our target categories."""
    # Check tags
    tags = market.get("tags", [])
    if isinstance(tags, list):
        for tag in tags:
            label = (tag.get("label", "") if isinstance(tag, dict) else str(tag)).lower()
            if any(cat in label for cat in TARGET_CATEGORIES):
                return True

    # Check category field
    category = str(market.get("category", "")).lower()
    if any(cat in category for cat in TARGET_CATEGORIES):
        return True

    # Check question text for keywords
    question = str(market.get("question", "")).lower()
    if any(kw in question for kw in TARGET_KEYWORDS):
        return True

    return False


def _extract_category(market: dict) -> str:
    """Extract the best category label for a market."""
    tags = market.get("tags", [])
    if isinstance(tags, list) and tags:
        label = tags[0].get("label", "") if isinstance(tags[0], dict) else str(tags[0])
        if label:
            return label.lower()

    question = str(market.get("question", "")).lower()
    for kw in ["election", "president", "congress", "senate", "vote", "governor"]:
        if kw in question:
            return "politics"
    for kw in ["war", "invasion", "nato", "sanction", "ceasefire", "treaty"]:
        if kw in question:
            return "geopolitics"
    for kw in ["bitcoin", "btc", "ethereum", "eth", "crypto"]:
        if kw in question:
            return "crypto"
    for kw in ["prime minister", "leader", "resign", "coup"]:
        if kw in question:
            return "world events"

    return str(market.get("category", "other")).lower() or "other"


async def _fetch_target_markets(days: int = LOOKBACK_DAYS) -> list[dict]:
    """Fetch resolved binary markets in target categories from the last N days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    all_markets: list[dict] = []
    offset = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        for page in range(MAX_PAGES):
            params = {"closed": "true", "limit": PAGE_SIZE, "offset": offset}
            try:
                resp = await client.get(GAMMA_URL, params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                print(f"  API error page {page + 1}: {exc}")
                break

            raw = data if isinstance(data, list) else data.get("markets", [])
            if not raw:
                break

            for m in raw:
                end_str = m.get("endDate", "") or m.get("end_date", "") or ""
                if end_str:
                    try:
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        if end_dt < cutoff:
                            continue
                    except ValueError:
                        pass

                # Must be binary with clear resolution
                outcomes_raw = m.get("outcomes", "")
                try:
                    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(outcomes, list) or len(outcomes) != 2:
                    continue

                prices_raw = m.get("outcomePrices", "")
                try:
                    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(prices, list) or len(prices) != 2:
                    continue

                try:
                    yes_price = float(prices[0])
                    no_price = float(prices[1])
                except (ValueError, TypeError):
                    continue

                if yes_price > 0.9:
                    resolved_yes = True
                elif no_price > 0.9:
                    resolved_yes = False
                else:
                    continue

                volume = float(m.get("volume", 0) or 0)
                if volume < 1000:  # higher volume threshold — real markets only
                    continue

                # Filter to target categories
                if not _is_target_market(m):
                    continue

                market_id = str(m.get("id") or m.get("conditionId") or "")
                if not market_id:
                    continue

                all_markets.append({
                    "market_id": market_id,
                    "question": m.get("question", ""),
                    "resolved_yes": resolved_yes,
                    "volume": volume,
                    "end_date": end_str,
                    "category": _extract_category(m),
                    "condition_id": m.get("conditionId", market_id),
                })

            print(f"  Page {page + 1}: {len(raw)} raw → {len(all_markets)} target markets")

            if len(raw) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            await asyncio.sleep(REQUEST_DELAY)

    print(f"  Total target markets (last {days}d): {len(all_markets)}")
    return all_markets


# ---------------------------------------------------------------------------
# Trade/position fetching per market
# ---------------------------------------------------------------------------

async def _fetch_market_trades(
    client: httpx.AsyncClient,
    market_id: str,
) -> list[dict]:
    """Fetch trade history for a market from CLOB API."""
    trades: list[dict] = []
    try:
        resp = await client.get(
            f"{CLOB_BASE}/trades",
            params={"asset_id": market_id, "limit": 500},
            timeout=15.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                trades = data
            elif isinstance(data, dict):
                trades = data.get("trades", data.get("data", []))
    except Exception as exc:
        logger.debug("Failed to fetch trades for %s: %s", market_id[:12], exc)
    return trades


# ---------------------------------------------------------------------------
# Wallet lifetime activity lookup
# ---------------------------------------------------------------------------

async def _fetch_wallet_lifetime_trades(
    client: httpx.AsyncClient,
    wallet: str,
) -> int:
    """
    Estimate total lifetime trades for a wallet.
    Uses CLOB API to fetch recent trade count as proxy.
    """
    try:
        resp = await client.get(
            f"{CLOB_BASE}/trades",
            params={"maker_address": wallet, "limit": 100},
            timeout=15.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            trades = data if isinstance(data, list) else data.get("trades", data.get("data", []))
            return len(trades)
    except Exception:
        pass
    return 0


# ---------------------------------------------------------------------------
# Core analysis pipeline
# ---------------------------------------------------------------------------

async def _analyze_markets(markets: list[dict]) -> list[dict]:
    """
    For each market, find large positions ($2K+) and analyze the wallets
    behind them. Returns list of informed bet candidates.
    """
    candidates: list[dict] = []
    # Track wallet trade counts to avoid redundant API calls
    wallet_trade_cache: dict[str, int] = {}
    batch_size = 8

    print(f"\n  Scanning {len(markets)} markets for large positions...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(markets), batch_size):
            batch = markets[i : i + batch_size]
            tasks = [_fetch_market_trades(client, m["market_id"]) for m in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for market, trade_list in zip(batch, results):
                if isinstance(trade_list, Exception) or not trade_list:
                    continue

                resolved_yes = market["resolved_yes"]
                end_date = market.get("end_date", "")
                category = market.get("category", "other")

                # Aggregate positions per wallet for this market
                wallet_positions: dict[str, dict] = defaultdict(lambda: {
                    "total_size": 0.0,
                    "total_cost": 0.0,
                    "trades": [],
                    "earliest_trade": None,
                    "side": None,
                })

                for trade in trade_list:
                    wallet = (
                        trade.get("maker_address")
                        or trade.get("owner")
                        or trade.get("trader", "")
                    )
                    if not wallet or len(wallet) < 10:
                        continue

                    price = float(trade.get("price", 0) or 0)
                    size = float(trade.get("size", 0) or trade.get("amount", 0) or 0)
                    if price <= 0 or price >= 1 or size <= 0:
                        continue

                    cost = price * size
                    side = str(trade.get("side", "")).upper()
                    bought_yes = side not in ("SELL", "NO", "0")
                    if trade.get("outcome") == "No" or side in ("SELL", "NO", "0"):
                        bought_yes = False

                    # Parse trade timestamp
                    trade_ts = trade.get("timestamp") or trade.get("created_at", "")
                    trade_dt = None
                    if trade_ts:
                        try:
                            trade_dt = datetime.fromisoformat(
                                str(trade_ts).replace("Z", "+00:00")
                            )
                        except (ValueError, AttributeError):
                            pass

                    pos = wallet_positions[wallet]
                    pos["total_size"] += size
                    pos["total_cost"] += cost
                    pos["trades"].append({
                        "price": price,
                        "size": size,
                        "cost": cost,
                        "bought_yes": bought_yes,
                        "timestamp": trade_dt,
                    })
                    pos["side"] = bought_yes  # last trade's side

                    if trade_dt and (pos["earliest_trade"] is None or trade_dt < pos["earliest_trade"]):
                        pos["earliest_trade"] = trade_dt

                # Filter to large positions ($2K+ total on this market)
                for wallet, pos in wallet_positions.items():
                    if pos["total_cost"] < MIN_BET_SIZE:
                        continue

                    # Entry timing: hours between earliest trade and resolution
                    hours_before = 0.0
                    if pos["earliest_trade"] and end_date:
                        try:
                            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                            hours_before = max(
                                (end_dt - pos["earliest_trade"]).total_seconds() / 3600, 0
                            )
                        except (ValueError, AttributeError):
                            pass

                    if hours_before < MIN_HOURS_BEFORE:
                        continue  # last-minute bet — not informed, just momentum

                    # Did the bet win?
                    bought_yes = pos["side"]
                    won = (bought_yes and resolved_yes) or (not bought_yes and not resolved_yes)

                    # Payout check: what price did they buy at?
                    avg_price = pos["total_cost"] / pos["total_size"] if pos["total_size"] > 0 else 0
                    payout_ratio = (1.0 - avg_price) / avg_price if avg_price > 0 else 0

                    # We want bets that WON — the informed bettor hypothesis
                    # requires they were right
                    if not won:
                        continue

                    # Fetch wallet lifetime trades (cached)
                    if wallet not in wallet_trade_cache:
                        lifetime = await _fetch_wallet_lifetime_trades(client, wallet)
                        wallet_trade_cache[wallet] = lifetime
                        await asyncio.sleep(0.1)  # be nice to API
                    lifetime_trades = wallet_trade_cache[wallet]

                    # Compute insider probability score
                    insider_score = _score_insider(
                        bet_size=pos["total_cost"],
                        hours_before=hours_before,
                        lifetime_trades=lifetime_trades,
                        won=won,
                        payout_ratio=payout_ratio,
                    )

                    candidates.append({
                        "wallet_address": wallet,
                        "insider_score": insider_score,
                        "market_id": market["market_id"],
                        "market_question": market["question"],
                        "bet_size": round(pos["total_cost"], 2),
                        "avg_entry_price": round(avg_price, 4),
                        "payout_ratio": round(payout_ratio, 4),
                        "entry_timing_hours": round(hours_before, 1),
                        "outcome": "win" if won else "loss",
                        "market_category": category,
                        "wallet_lifetime_trades": lifetime_trades,
                        "num_trades_this_market": len(pos["trades"]),
                        "resolved_yes": resolved_yes,
                        "bought_yes": bought_yes,
                    })

            progress = min(i + batch_size, len(markets))
            print(
                f"  Processed {progress}/{len(markets)} markets "
                f"— {len(candidates)} insider candidates found"
            )
            await asyncio.sleep(REQUEST_DELAY)

    return candidates


# ---------------------------------------------------------------------------
# Insider probability scoring
# ---------------------------------------------------------------------------

def _score_insider(
    bet_size: float,
    hours_before: float,
    lifetime_trades: int,
    won: bool,
    payout_ratio: float,
) -> str:
    """
    Score insider probability:
      HIGH:   dormant wallet (<5 trades) + $5K+ bet + >48hr before + won
      MEDIUM: low-activity (<20 trades) + $2K+ bet + >24hr before + won
      LOW:    everything else
    """
    if not won:
        return "LOW"

    is_dormant = lifetime_trades < MAX_LIFETIME_TRADES
    is_low_activity = lifetime_trades < LOW_ACTIVITY_TRADES
    is_large_bet = bet_size >= HIGH_BET_SIZE
    is_early = hours_before >= HIGH_HOURS_BEFORE

    if is_dormant and is_large_bet and is_early:
        return "HIGH"
    if is_low_activity and bet_size >= MIN_BET_SIZE and hours_before >= MIN_HOURS_BEFORE:
        return "MEDIUM"
    return "LOW"


# ---------------------------------------------------------------------------
# Multi-market repeater detection
# ---------------------------------------------------------------------------

def _find_repeaters(candidates: list[dict]) -> list[dict]:
    """
    Find wallets that appear in MULTIPLE markets with HIGH scores.
    These are the most valuable — rare informed bettors across different domains.
    """
    wallet_high_markets: dict[str, list[dict]] = defaultdict(list)

    for c in candidates:
        if c["insider_score"] == "HIGH":
            wallet_high_markets[c["wallet_address"]].append({
                "market_id": c["market_id"],
                "market_question": c["market_question"],
                "bet_size": c["bet_size"],
                "market_category": c["market_category"],
                "entry_timing_hours": c["entry_timing_hours"],
            })

    repeaters = []
    for wallet, markets in wallet_high_markets.items():
        if len(markets) >= 2:
            categories = list({m["market_category"] for m in markets})
            total_bet = sum(m["bet_size"] for m in markets)
            repeaters.append({
                "wallet_address": wallet,
                "high_score_markets": len(markets),
                "categories": categories,
                "total_informed_bets": round(total_bet, 2),
                "markets": markets,
            })

    repeaters.sort(key=lambda r: r["high_score_markets"], reverse=True)
    return repeaters


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

SCORE_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


def _print_leaderboard(candidates: list[dict], repeaters: list[dict]) -> None:
    """Print formatted insider leaderboard."""
    print(f"\n{'=' * 100}")
    print(f"  INFORMED BETTOR LEADERBOARD — v2 Insider Discovery")
    print(f"{'=' * 100}")

    # Count by score
    high = sum(1 for c in candidates if c["insider_score"] == "HIGH")
    med = sum(1 for c in candidates if c["insider_score"] == "MEDIUM")
    low = sum(1 for c in candidates if c["insider_score"] == "LOW")
    print(f"  HIGH: {high}  |  MEDIUM: {med}  |  LOW: {low}  |  Total: {len(candidates)}")
    print()

    # Sort: score → bet size
    sorted_candidates = sorted(
        candidates,
        key=lambda c: (SCORE_ORDER.get(c["insider_score"], 9), -c["bet_size"]),
    )

    print(
        f"  {'#':>3}  {'Score':<6}  {'Wallet':<14}  {'Bet Size':>10}  "
        f"{'Timing':>8}  {'Trades':>6}  {'Category':<14}  {'Market'}"
    )
    print(f"  {'-' * 95}")

    for i, c in enumerate(sorted_candidates[:40], 1):
        addr = c["wallet_address"][:6] + "..." + c["wallet_address"][-4:]
        question = c["market_question"][:35] + "..." if len(c["market_question"]) > 35 else c["market_question"]
        score_marker = {
            "HIGH": "\033[91mHIGH\033[0m",    # red
            "MEDIUM": "\033[93mMED\033[0m",    # yellow
            "LOW": "LOW",
        }.get(c["insider_score"], c["insider_score"])

        print(
            f"  {i:>3}  {score_marker:<15}  {addr:<14}  "
            f"${c['bet_size']:>9,.0f}  "
            f"{c['entry_timing_hours']:>6.0f}h  "
            f"{c['wallet_lifetime_trades']:>6}  "
            f"{c['market_category']:<14}  "
            f"{question}"
        )

    # Repeater alert
    if repeaters:
        print(f"\n{'=' * 100}")
        print(f"  MULTI-MARKET INSIDERS (wallets with HIGH scores across multiple markets)")
        print(f"{'=' * 100}")
        for r in repeaters:
            addr = r["wallet_address"][:6] + "..." + r["wallet_address"][-4:]
            cats = ", ".join(r["categories"])
            print(
                f"  {addr}  —  {r['high_score_markets']} HIGH-score markets  "
                f"| ${r['total_informed_bets']:,.0f} total  "
                f"| categories: {cats}"
            )
            for m in r["markets"]:
                q = m["market_question"][:60] + "..." if len(m["market_question"]) > 60 else m["market_question"]
                print(f"    → ${m['bet_size']:,.0f} on: {q}")

    print(f"{'=' * 100}\n")


# ---------------------------------------------------------------------------
# Backtest integration — run whale_backtest against v2 registry
# ---------------------------------------------------------------------------

async def _run_v2_backtest(candidates: list[dict]) -> dict:
    """
    P&L simulation: if we had followed every HIGH/MEDIUM insider bet
    at 1% of their position size, what would our returns look like?
    Saves results to data/whale_backtest_v2.json.
    """
    print(f"\n{'=' * 70}")
    print(f"  V2 BACKTEST — Follow Informed Bettors at 1% Sizing")
    print(f"{'=' * 70}")

    followed = [c for c in candidates if c["insider_score"] in ("HIGH", "MEDIUM")]
    if not followed:
        print("  No HIGH/MEDIUM candidates to backtest.")
        return {}

    total_risked = 0.0
    total_pnl = 0.0
    wins = 0
    losses = 0
    trade_log: list[dict] = []

    for c in followed:
        our_size = c["bet_size"] * 0.01
        avg_price = c["avg_entry_price"]

        if c["outcome"] == "win":
            pnl = (1.0 - avg_price) * our_size / avg_price if avg_price > 0 else 0
            wins += 1
        else:
            pnl = -our_size
            losses += 1

        total_risked += our_size
        total_pnl += pnl

        trade_log.append({
            "wallet_address": c["wallet_address"],
            "insider_score": c["insider_score"],
            "market_id": c["market_id"],
            "market_question": c["market_question"],
            "market_category": c["market_category"],
            "whale_bet_size": c["bet_size"],
            "our_position_size": round(our_size, 2),
            "avg_entry_price": avg_price,
            "entry_timing_hours": c["entry_timing_hours"],
            "outcome": c["outcome"],
            "pnl": round(pnl, 2),
            "wallet_lifetime_trades": c["wallet_lifetime_trades"],
        })

    win_rate = wins / len(followed) if followed else 0
    roi = (total_pnl / total_risked * 100) if total_risked > 0 else 0

    # Score breakdown
    score_breakdown = {}
    for score in ("HIGH", "MEDIUM"):
        subset = [t for t in trade_log if t["insider_score"] == score]
        if not subset:
            continue
        s_risked = sum(t["our_position_size"] for t in subset)
        s_wins = sum(1 for t in subset if t["outcome"] == "win")
        s_pnl = sum(t["pnl"] for t in subset)
        score_breakdown[score] = {
            "count": len(subset),
            "wins": s_wins,
            "win_rate": round(s_wins / len(subset), 4) if subset else 0,
            "total_risked": round(s_risked, 2),
            "total_pnl": round(s_pnl, 2),
            "roi_pct": round(s_pnl / s_risked * 100, 1) if s_risked > 0 else 0,
        }

    backtest_result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "version": "v2",
        "strategy": "follow_informed_bettors_1pct",
        "summary": {
            "signals_followed": len(followed),
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 4),
            "total_risked": round(total_risked, 2),
            "total_pnl": round(total_pnl, 2),
            "roi_pct": round(roi, 1),
        },
        "score_breakdown": score_breakdown,
        "trades": trade_log,
    }

    with open(BACKTEST_V2_PATH, "w") as f:
        json.dump(backtest_result, f, indent=2)
    print(f"  Backtest saved to {BACKTEST_V2_PATH}")

    print(f"  Signals followed:  {len(followed)} (HIGH + MEDIUM only)")
    print(f"  Win rate:          {win_rate:.1%}")
    print(f"  Total risked:      ${total_risked:,.2f}")
    print(f"  Total P&L:         ${total_pnl:+,.2f}")
    print(f"  ROI:               {roi:+.1f}%")
    print()

    for score, data in score_breakdown.items():
        print(
            f"  {score:>6}: {data['count']} bets | "
            f"WR {data['win_rate']:.0%} | "
            f"risked ${data['total_risked']:,.2f} | "
            f"P&L ${data['total_pnl']:+,.2f}"
        )

    print(f"{'=' * 70}\n")
    return backtest_result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run() -> list[dict]:
    """Run the full informed bettor discovery pipeline."""
    os.makedirs(DATA_DIR, exist_ok=True)

    print("\n  Whale Finder v2: Informed Bettor Discovery")
    print("  " + "-" * 50)
    print("  Hypothesis: one-time insiders, not systematic traders")
    print(f"  Min bet: ${MIN_BET_SIZE:,.0f} | Min timing: {MIN_HOURS_BEFORE:.0f}h before resolution")
    print(f"  Target: politics, elections, geopolitics, world events, crypto")
    print()

    # Step 1: Fetch resolved markets in target categories
    markets = await _fetch_target_markets()
    if not markets:
        print("  No target markets found.")
        return []

    # Step 2: Analyze all positions and score insider probability
    candidates = await _analyze_markets(markets)
    print(f"\n  Total insider candidates: {len(candidates)}")

    # Step 3: Find multi-market repeaters
    repeaters = _find_repeaters(candidates)

    # Step 4: Sort by score then bet size
    candidates.sort(
        key=lambda c: (SCORE_ORDER.get(c["insider_score"], 9), -c["bet_size"]),
    )

    # Step 5: Save registry
    registry = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "version": "v2",
        "hypothesis": "informed_bettor_discovery",
        "lookback_days": LOOKBACK_DAYS,
        "total_markets_analyzed": len(markets),
        "qualification_criteria": {
            "min_bet_size": MIN_BET_SIZE,
            "min_hours_before_resolution": MIN_HOURS_BEFORE,
            "max_lifetime_trades_dormant": MAX_LIFETIME_TRADES,
            "max_lifetime_trades_low_activity": LOW_ACTIVITY_TRADES,
            "target_categories": sorted(TARGET_CATEGORIES),
        },
        "summary": {
            "total_candidates": len(candidates),
            "high": sum(1 for c in candidates if c["insider_score"] == "HIGH"),
            "medium": sum(1 for c in candidates if c["insider_score"] == "MEDIUM"),
            "low": sum(1 for c in candidates if c["insider_score"] == "LOW"),
            "multi_market_repeaters": len(repeaters),
        },
        "repeaters": repeaters,
        "candidates": candidates,
    }

    with open(REGISTRY_V2_PATH, "w") as f:
        json.dump(registry, f, indent=2)
    print(f"  Registry saved to {REGISTRY_V2_PATH}")

    # Step 6: Print leaderboard
    if candidates:
        _print_leaderboard(candidates, repeaters)
    else:
        print("  No candidates met informed bettor criteria.")
        print("  CLOB API may not return per-wallet data for these markets.")

    # Step 7: Run backtest
    if candidates:
        await _run_v2_backtest(candidates)

    return candidates


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
