"""
EdgeLab Whale Finder — Informed Bettor Discovery.

HYPOTHESIS: one-time informed bettors who make 1-5 large bets on specific
markets they have insider knowledge of. The CIA analyst, the campaign insider,
the corporate board member — not the systematic quant trader.

This replaces the old "systematic trader" hypothesis (50+ trades, 65% WR).
The new hypothesis targets dormant wallets making unusually large, well-timed
bets on political/geopolitical events — and winning.

Qualification criteria:
- Min trades: 1 (not 50)
- Single bet $2,000+ on one market
- Bet placed >24h before resolution
- Target: politics, elections, geopolitics, world events, crypto
- Won at 80%+ payout
- Dormant wallet (< 5 lifetime trades)

Usage:
    python scripts/whale_finder.py
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
REGISTRY_PATH = os.path.join(DATA_DIR, "whale_registry.json")

LOOKBACK_DAYS = 90
PAGE_SIZE = 500
MAX_PAGES = 100
REQUEST_DELAY = 0.3

# Informed bettor thresholds
MIN_BET_SIZE = 2000.0
HIGH_BET_SIZE = 5000.0
MIN_HOURS_BEFORE = 24.0
HIGH_HOURS_BEFORE = 48.0
MAX_LIFETIME_TRADES = 5      # dormant wallet
LOW_ACTIVITY_TRADES = 20     # low-activity wallet
MIN_PAYOUT = 0.80

# Target categories
TARGET_CATEGORIES = {
    "politics", "election", "elections", "political",
    "geopolitics", "geopolitical", "world", "world events",
    "international", "government", "legislation", "policy",
    "crypto", "cryptocurrency", "bitcoin", "ethereum",
    "leadership", "president", "congress", "senate",
}

TARGET_KEYWORDS = [
    "president", "election", "vote", "congress", "senate", "governor",
    "prime minister", "leader", "resign", "impeach", "sanction",
    "war", "invasion", "ceasefire", "treaty", "nato", "un ",
    "bitcoin", "btc", "ethereum", "eth", "crypto",
    "tariff", "embargo", "coup", "assassination",
]


def _is_target_market(market: dict) -> bool:
    """Check if market falls in our target categories."""
    tags = market.get("tags", [])
    if isinstance(tags, list):
        for tag in tags:
            label = (tag.get("label", "") if isinstance(tag, dict) else str(tag)).lower()
            if any(cat in label for cat in TARGET_CATEGORIES):
                return True

    category = str(market.get("category", "")).lower()
    if any(cat in category for cat in TARGET_CATEGORIES):
        return True

    question = str(market.get("question", "")).lower()
    if any(kw in question for kw in TARGET_KEYWORDS):
        return True

    return False


def _extract_category(market: dict) -> str:
    """Extract best category label for a market."""
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


# ---------------------------------------------------------------------------
# Market fetching
# ---------------------------------------------------------------------------

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
                if volume < 1000:
                    continue

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
# Trade fetching
# ---------------------------------------------------------------------------

async def _fetch_market_trades(client: httpx.AsyncClient, market_id: str) -> list[dict]:
    """Fetch trade history for a market from CLOB API."""
    try:
        resp = await client.get(
            f"{CLOB_BASE}/trades",
            params={"asset_id": market_id, "limit": 500},
            timeout=15.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("trades", data.get("data", []))
    except Exception as exc:
        logger.debug("Failed to fetch trades for %s: %s", market_id[:12], exc)
    return []


async def _fetch_wallet_lifetime_trades(client: httpx.AsyncClient, wallet: str) -> int:
    """Estimate total lifetime trades for a wallet."""
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
# Insider scoring
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
# Core analysis
# ---------------------------------------------------------------------------

async def _analyze_markets(markets: list[dict]) -> list[dict]:
    """Find large positions ($2K+) and score insider probability."""
    candidates: list[dict] = []
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

                # Aggregate per-wallet positions for this market
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

                    trade_ts = trade.get("timestamp") or trade.get("created_at", "")
                    trade_dt = None
                    if trade_ts:
                        try:
                            trade_dt = datetime.fromisoformat(str(trade_ts).replace("Z", "+00:00"))
                        except (ValueError, AttributeError):
                            pass

                    pos = wallet_positions[wallet]
                    pos["total_size"] += size
                    pos["total_cost"] += cost
                    pos["trades"].append({
                        "price": price, "size": size, "cost": cost,
                        "bought_yes": bought_yes, "timestamp": trade_dt,
                    })
                    pos["side"] = bought_yes
                    if trade_dt and (pos["earliest_trade"] is None or trade_dt < pos["earliest_trade"]):
                        pos["earliest_trade"] = trade_dt

                # Filter to large positions
                for wallet, pos in wallet_positions.items():
                    if pos["total_cost"] < MIN_BET_SIZE:
                        continue

                    hours_before = 0.0
                    if pos["earliest_trade"] and end_date:
                        try:
                            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                            hours_before = max((end_dt - pos["earliest_trade"]).total_seconds() / 3600, 0)
                        except (ValueError, AttributeError):
                            pass

                    if hours_before < MIN_HOURS_BEFORE:
                        continue

                    bought_yes = pos["side"]
                    won = (bought_yes and resolved_yes) or (not bought_yes and not resolved_yes)

                    avg_price = pos["total_cost"] / pos["total_size"] if pos["total_size"] > 0 else 0
                    payout_ratio = (1.0 - avg_price) / avg_price if avg_price > 0 else 0

                    if not won:
                        continue

                    if wallet not in wallet_trade_cache:
                        lifetime = await _fetch_wallet_lifetime_trades(client, wallet)
                        wallet_trade_cache[wallet] = lifetime
                        await asyncio.sleep(0.1)
                    lifetime_trades = wallet_trade_cache[wallet]

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
            print(f"  Processed {progress}/{len(markets)} markets — {len(candidates)} insider candidates")
            await asyncio.sleep(REQUEST_DELAY)

    return candidates


# ---------------------------------------------------------------------------
# Repeater detection
# ---------------------------------------------------------------------------

def _find_repeaters(candidates: list[dict]) -> list[dict]:
    """Find wallets with HIGH scores across multiple markets — serial insiders."""
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
    print(f"  INFORMED BETTOR LEADERBOARD — Insider Discovery")
    print(f"{'=' * 100}")

    high = sum(1 for c in candidates if c["insider_score"] == "HIGH")
    med = sum(1 for c in candidates if c["insider_score"] == "MEDIUM")
    low = sum(1 for c in candidates if c["insider_score"] == "LOW")
    print(f"  HIGH: {high}  |  MEDIUM: {med}  |  LOW: {low}  |  Total: {len(candidates)}")
    print()

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
            "HIGH": "\033[91mHIGH\033[0m",
            "MEDIUM": "\033[93mMED\033[0m",
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

    if repeaters:
        print(f"\n{'=' * 100}")
        print(f"  SERIAL INSIDERS (HIGH scores across multiple markets)")
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
# Main
# ---------------------------------------------------------------------------

async def run() -> list[dict]:
    """Run the informed bettor discovery pipeline."""
    os.makedirs(DATA_DIR, exist_ok=True)

    print("\n  Whale Finder: Informed Bettor Discovery")
    print("  " + "-" * 50)
    print("  Hypothesis: one-time insiders, not systematic traders")
    print(f"  Min bet: ${MIN_BET_SIZE:,.0f} | Min timing: {MIN_HOURS_BEFORE:.0f}h before resolution")
    print(f"  Target: politics, elections, geopolitics, world events, crypto")
    print()

    markets = await _fetch_target_markets()
    if not markets:
        print("  No target markets found.")
        return []

    candidates = await _analyze_markets(markets)
    print(f"\n  Total insider candidates: {len(candidates)}")

    repeaters = _find_repeaters(candidates)

    candidates.sort(
        key=lambda c: (SCORE_ORDER.get(c["insider_score"], 9), -c["bet_size"]),
    )

    registry = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
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
        "whales": candidates,  # keep "whales" key for backward compat with whale_tracker
    }

    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2)
    print(f"  Registry saved to {REGISTRY_PATH}")

    if candidates:
        _print_leaderboard(candidates, repeaters)
    else:
        print("  No candidates met informed bettor criteria.")

    return candidates


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
