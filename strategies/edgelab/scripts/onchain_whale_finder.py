"""
EdgeLab On-Chain Whale Finder — Direct Polygon blockchain query.

Bypasses the CLOB API (which returns 401 for per-wallet data) by reading
Polymarket's CTF Exchange contract events directly from the Polygon chain.

Two parallel approaches:
  1. On-chain: PositionSplit/PositionsMerge events from CTF Exchange contract
     → filter for $2K+ USDC → cross-reference with Gamma API for market context
  2. Leaderboard: public Polymarket leaderboard API → top profit earners

Output: data/whale_registry_v3.json

Usage:
    python scripts/onchain_whale_finder.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx
from web3 import Web3
from web3.exceptions import BlockNotFound

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
REGISTRY_PATH = os.path.join(DATA_DIR, "whale_registry_v3.json")

# Polygon RPC endpoints (fallback chain)
POLYGON_RPCS = [
    "https://polygon-rpc.com",
    "https://polygon-mainnet.g.alchemy.com/v2/demo",
    "https://rpc-mainnet.matic.quiknode.pro",
]

# Polymarket CTF Exchange on Polygon
CTF_EXCHANGE = Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")

# USDC on Polygon (6 decimals)
USDC_ADDRESS = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
USDC_DECIMALS = 6

# Event signatures (keccak256 hashes)
# PositionSplit(address indexed stakeholder, bytes32 parentCollectionId,
#               bytes32 indexed conditionId, uint256[] partition, uint256 amount)
POSITION_SPLIT_TOPIC = Web3.keccak(
    text="PositionSplit(address,bytes32,bytes32,uint256[],uint256)"
).hex()

# PositionsMerge(address indexed stakeholder, bytes32 parentCollectionId,
#                bytes32 indexed conditionId, uint256[] partition, uint256 amount)
POSITIONS_MERGE_TOPIC = Web3.keccak(
    text="PositionsMerge(address,bytes32,bytes32,uint256[],uint256)"
).hex()

# Also track Transfer events on the exchange (ERC1155 token movements)
# TransferSingle(address operator, address from, address to, uint256 id, uint256 value)
TRANSFER_SINGLE_TOPIC = Web3.keccak(
    text="TransferSingle(address,address,address,uint256,uint256)"
).hex()

# Lookback
LOOKBACK_DAYS = 90
MIN_USDC_VALUE = 2_000
HIGH_USDC_VALUE = 5_000

# Block time on Polygon ~2 seconds
BLOCKS_PER_DAY = 43_200
BLOCK_RANGE_PER_QUERY = 10_000  # max range per eth_getLogs call

# Gamma API for market resolution data
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_PAGE_SIZE = 500
GAMMA_MAX_PAGES = 100

# Polymarket leaderboard
LEADERBOARD_URL = "https://polymarket.com/api/profiles/leaderboard"

# Insider scoring thresholds (same as whale_finder.py)
MAX_LIFETIME_TRADES = 5
LOW_ACTIVITY_TRADES = 20
MIN_HOURS_BEFORE = 24.0
HIGH_HOURS_BEFORE = 48.0
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

REQUEST_DELAY = 0.3


# ---------------------------------------------------------------------------
# Polygon connection
# ---------------------------------------------------------------------------

def _connect_polygon() -> Web3:
    """Try each RPC endpoint until one connects."""
    for rpc in POLYGON_RPCS:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
            if w3.is_connected():
                block = w3.eth.block_number
                print(f"  Connected to Polygon via {rpc.split('/')[2]}")
                print(f"  Current block: {block:,}")
                return w3
        except Exception as exc:
            print(f"  RPC {rpc.split('/')[2]} failed: {exc}")
            continue
    raise ConnectionError("All Polygon RPC endpoints failed")


# ---------------------------------------------------------------------------
# On-chain event fetching
# ---------------------------------------------------------------------------

def _fetch_large_events(w3: Web3) -> list[dict]:
    """
    Fetch PositionSplit and TransferSingle events from CTF Exchange.
    Filter for transactions with USDC value > MIN_USDC_VALUE.
    """
    current_block = w3.eth.block_number
    start_block = current_block - (LOOKBACK_DAYS * BLOCKS_PER_DAY)
    total_blocks = current_block - start_block

    print(f"\n  Scanning {LOOKBACK_DAYS} days of blocks: {start_block:,} → {current_block:,}")
    print(f"  Total blocks to scan: {total_blocks:,}")
    print(f"  Queries needed: ~{total_blocks // BLOCK_RANGE_PER_QUERY + 1}")

    events: list[dict] = []
    block = start_block
    query_count = 0
    error_count = 0

    while block < current_block:
        to_block = min(block + BLOCK_RANGE_PER_QUERY - 1, current_block)

        try:
            # Fetch TransferSingle events on the CTF Exchange
            # These capture position-taking (buying YES/NO tokens)
            logs = w3.eth.get_logs({
                "fromBlock": block,
                "toBlock": to_block,
                "address": CTF_EXCHANGE,
                "topics": [TRANSFER_SINGLE_TOPIC],
            })

            for log in logs:
                try:
                    tx_hash = log["transactionHash"].hex()

                    # TransferSingle: topics[1]=operator, topics[2]=from, topics[3]=to
                    # data contains: id (uint256) + value (uint256)
                    data = log["data"]
                    if isinstance(data, (bytes, bytearray)):
                        data_hex = data.hex()
                    else:
                        data_hex = data[2:] if data.startswith("0x") else data

                    if len(data_hex) < 128:
                        continue

                    token_id = int(data_hex[:64], 16)
                    value = int(data_hex[64:128], 16)

                    # Value is in conditional token units — roughly maps to USDC
                    # (Polymarket tokens are minted 1:1 with USDC collateral)
                    usdc_value = value / (10 ** USDC_DECIMALS)

                    if usdc_value < MIN_USDC_VALUE:
                        continue

                    # Extract addresses from topics
                    from_addr = "0x" + log["topics"][2].hex()[-40:]
                    to_addr = "0x" + log["topics"][3].hex()[-40:]

                    # Minting (from=0x0) = position being opened
                    # Burning (to=0x0) = position being closed
                    is_mint = from_addr == "0x" + "0" * 40
                    is_burn = to_addr == "0x" + "0" * 40

                    # We care about mints (new positions) and large transfers
                    wallet = to_addr if not is_burn else from_addr

                    # Derive condition ID from token ID
                    # In Polymarket, token_id encodes the condition + outcome
                    condition_id = hex(token_id)

                    events.append({
                        "wallet": Web3.to_checksum_address(wallet),
                        "amount_usdc": round(usdc_value, 2),
                        "token_id": token_id,
                        "condition_id": condition_id,
                        "block_number": log["blockNumber"],
                        "tx_hash": tx_hash,
                        "event_type": "mint" if is_mint else ("burn" if is_burn else "transfer"),
                        "from_addr": from_addr,
                        "to_addr": to_addr,
                    })

                except Exception as exc:
                    logger.debug("Failed to parse log: %s", exc)
                    continue

            query_count += 1
            if query_count % 20 == 0:
                progress = (block - start_block) / total_blocks * 100
                print(f"  Progress: {progress:.0f}% — {len(events)} large events found")

        except Exception as exc:
            error_count += 1
            err_msg = str(exc)
            if "429" in err_msg or "rate" in err_msg.lower():
                print(f"  Rate limited at block {block:,} — waiting 5s...")
                time.sleep(5)
                continue
            elif error_count > 20:
                print(f"  Too many errors ({error_count}), stopping scan at block {block:,}")
                break
            else:
                logger.debug("Query error at block %d: %s", block, exc)
                time.sleep(1)

        block = to_block + 1
        # Small delay to avoid rate limits
        if query_count % 5 == 0:
            time.sleep(0.5)

    print(f"  Scan complete: {len(events)} large events (> ${MIN_USDC_VALUE:,}) from {query_count} queries")
    if error_count > 0:
        print(f"  Errors encountered: {error_count}")

    return events


def _resolve_timestamps(w3: Web3, events: list[dict]) -> list[dict]:
    """Add timestamps to events by fetching block timestamps."""
    blocks_needed = list({e["block_number"] for e in events})
    block_ts: dict[int, datetime] = {}

    print(f"  Resolving timestamps for {len(blocks_needed)} unique blocks...")
    for i, block_num in enumerate(blocks_needed):
        try:
            block_data = w3.eth.get_block(block_num)
            block_ts[block_num] = datetime.fromtimestamp(block_data["timestamp"], tz=timezone.utc)
        except (BlockNotFound, Exception) as exc:
            logger.debug("Failed to get block %d: %s", block_num, exc)
        if i > 0 and i % 50 == 0:
            print(f"    {i}/{len(blocks_needed)} blocks resolved")
            time.sleep(0.5)

    for event in events:
        event["timestamp"] = block_ts.get(event["block_number"])

    resolved = sum(1 for e in events if e.get("timestamp"))
    print(f"  Timestamps resolved: {resolved}/{len(events)}")
    return events


# ---------------------------------------------------------------------------
# Market resolution data (Gamma API)
# ---------------------------------------------------------------------------

async def _fetch_resolved_markets(days: int = LOOKBACK_DAYS) -> dict[str, dict]:
    """Fetch resolved markets from Gamma API. Returns dict keyed by condition_id."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    markets: dict[str, dict] = {}
    offset = 0

    print(f"\n  Fetching resolved markets from Gamma API...")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for page in range(GAMMA_MAX_PAGES):
            try:
                resp = await client.get(
                    GAMMA_URL,
                    params={"closed": "true", "limit": GAMMA_PAGE_SIZE, "offset": offset},
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                print(f"  Gamma API error page {page + 1}: {exc}")
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

                # Determine resolution
                prices_raw = m.get("outcomePrices", "")
                try:
                    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(prices, list) or len(prices) < 2:
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

                condition_id = str(m.get("conditionId", ""))
                market_id = str(m.get("id") or condition_id)
                question = m.get("question", "")
                category = _extract_category(m)
                volume = float(m.get("volume", 0) or 0)

                # Store by both condition_id and market_id for lookup
                entry = {
                    "market_id": market_id,
                    "condition_id": condition_id,
                    "question": question,
                    "resolved_yes": resolved_yes,
                    "end_date": end_str,
                    "category": category,
                    "volume": volume,
                    "is_target": _is_target_market(m),
                }
                if condition_id:
                    markets[condition_id] = entry
                markets[market_id] = entry

            if page % 10 == 0 and page > 0:
                print(f"    Page {page + 1}: {len(markets)} resolved markets loaded")

            if len(raw) < GAMMA_PAGE_SIZE:
                break
            offset += GAMMA_PAGE_SIZE
            await asyncio.sleep(REQUEST_DELAY)

    print(f"  Total resolved markets loaded: {len(markets)}")
    return markets


def _is_target_market(market: dict) -> bool:
    """Check if market falls in target categories."""
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
    """Extract best category label."""
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
# Leaderboard approach
# ---------------------------------------------------------------------------

async def _fetch_leaderboard() -> list[dict]:
    """Fetch top profit earners from public Polymarket leaderboard."""
    print(f"\n  Fetching Polymarket leaderboard...")
    entries: list[dict] = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(LEADERBOARD_URL)
            resp.raise_for_status()
            data = resp.json()

            # Handle various response formats
            profiles = data if isinstance(data, list) else data.get("profiles", data.get("data", []))

            for p in profiles[:100]:
                addr = p.get("address", p.get("proxyWallet", p.get("wallet", "")))
                name = p.get("name", p.get("username", ""))
                profit = float(p.get("profit", p.get("totalProfit", 0)) or 0)
                volume = float(p.get("volume", p.get("totalVolume", 0)) or 0)
                markets_traded = int(p.get("marketsTraded", p.get("markets_traded", 0)) or 0)
                win_rate = float(p.get("winRate", p.get("win_rate", 0)) or 0)
                rank = int(p.get("rank", p.get("position", 0)) or 0)

                entries.append({
                    "wallet_address": addr,
                    "display_name": name,
                    "total_profit": round(profit, 2),
                    "total_volume": round(volume, 2),
                    "markets_traded": markets_traded,
                    "win_rate": round(win_rate, 4),
                    "rank": rank,
                    "source": "leaderboard",
                })

            print(f"  Leaderboard: {len(entries)} profiles loaded")

        except httpx.HTTPStatusError as exc:
            print(f"  Leaderboard API returned {exc.response.status_code} — trying alternative")
            # Try alternative endpoint format
            try:
                resp = await client.get(
                    "https://gamma-api.polymarket.com/leaderboard",
                    params={"limit": 100, "window": "all"},
                )
                resp.raise_for_status()
                data = resp.json()
                profiles = data if isinstance(data, list) else data.get("data", [])
                for p in profiles[:100]:
                    entries.append({
                        "wallet_address": p.get("address", p.get("userAddress", "")),
                        "display_name": p.get("name", ""),
                        "total_profit": round(float(p.get("pnl", 0) or 0), 2),
                        "total_volume": round(float(p.get("volume", 0) or 0), 2),
                        "markets_traded": int(p.get("numMarkets", 0) or 0),
                        "win_rate": round(float(p.get("winRate", 0) or 0), 4),
                        "rank": int(p.get("rank", 0) or 0),
                        "source": "gamma_leaderboard",
                    })
                print(f"  Gamma leaderboard: {len(entries)} profiles loaded")
            except Exception as exc2:
                print(f"  Alternative leaderboard also failed: {exc2}")

        except Exception as exc:
            print(f"  Leaderboard fetch failed: {exc}")

    return entries


# ---------------------------------------------------------------------------
# Cross-reference & scoring
# ---------------------------------------------------------------------------

def _build_whale_profiles(
    events: list[dict],
    markets: dict[str, dict],
) -> list[dict]:
    """
    Aggregate on-chain events per wallet and score for insider behavior.
    """
    print(f"\n  Building whale profiles from {len(events)} on-chain events...")

    # Group events by wallet
    wallet_events: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        wallet_events[e["wallet"]].append(e)

    print(f"  Unique wallets with large transactions: {len(wallet_events)}")

    candidates: list[dict] = []

    for wallet, w_events in wallet_events.items():
        # Aggregate per-condition (market)
        condition_bets: dict[str, dict] = defaultdict(lambda: {
            "total_usdc": 0.0,
            "event_count": 0,
            "earliest_ts": None,
            "latest_ts": None,
            "token_ids": set(),
            "event_types": [],
        })

        for e in w_events:
            cid = e["condition_id"]
            cb = condition_bets[cid]
            cb["total_usdc"] += e["amount_usdc"]
            cb["event_count"] += 1
            cb["token_ids"].add(e["token_id"])
            cb["event_types"].append(e["event_type"])

            ts = e.get("timestamp")
            if ts:
                if cb["earliest_ts"] is None or ts < cb["earliest_ts"]:
                    cb["earliest_ts"] = ts
                if cb["latest_ts"] is None or ts > cb["latest_ts"]:
                    cb["latest_ts"] = ts

        lifetime_trades = len(w_events)

        for cid, bet in condition_bets.items():
            if bet["total_usdc"] < MIN_USDC_VALUE:
                continue

            # Try to match with Gamma market data
            market = markets.get(cid)

            hours_before = 0.0
            question = f"Unknown market (token {cid[:18]}...)"
            resolved_yes = None
            category = "unknown"
            is_target = True  # assume target if we can't verify
            won = None

            if market:
                question = market["question"]
                resolved_yes = market["resolved_yes"]
                category = market["category"]
                is_target = market["is_target"]

                end_str = market.get("end_date", "")
                if end_str and bet["earliest_ts"]:
                    try:
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        hours_before = max(
                            (end_dt - bet["earliest_ts"]).total_seconds() / 3600, 0
                        )
                    except (ValueError, AttributeError):
                        pass

            # Score
            is_dormant = lifetime_trades < MAX_LIFETIME_TRADES
            is_low_activity = lifetime_trades < LOW_ACTIVITY_TRADES
            is_large = bet["total_usdc"] >= HIGH_USDC_VALUE
            is_early = hours_before >= HIGH_HOURS_BEFORE

            # Without knowing outcome token mapping, we flag based on timing + size
            if is_dormant and is_large and is_early:
                insider_score = "HIGH"
            elif is_low_activity and bet["total_usdc"] >= MIN_USDC_VALUE and hours_before >= MIN_HOURS_BEFORE:
                insider_score = "MEDIUM"
            else:
                insider_score = "LOW"

            candidates.append({
                "wallet_address": wallet,
                "insider_score": insider_score,
                "condition_id": cid,
                "market_question": question,
                "bet_size": round(bet["total_usdc"], 2),
                "entry_timing_hours": round(hours_before, 1),
                "market_category": category,
                "wallet_lifetime_events": lifetime_trades,
                "event_count_this_market": bet["event_count"],
                "event_types": list(set(bet["event_types"])),
                "is_target_market": is_target,
                "resolved_yes": resolved_yes,
                "earliest_entry": bet["earliest_ts"].isoformat() if bet["earliest_ts"] else None,
                "source": "onchain",
            })

    # Sort by score then bet size
    score_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    candidates.sort(key=lambda c: (score_order.get(c["insider_score"], 9), -c["bet_size"]))

    high = sum(1 for c in candidates if c["insider_score"] == "HIGH")
    med = sum(1 for c in candidates if c["insider_score"] == "MEDIUM")
    print(f"  Whale profiles: {len(candidates)} total — {high} HIGH, {med} MEDIUM")

    return candidates


def _find_repeaters(candidates: list[dict]) -> list[dict]:
    """Find wallets with HIGH scores across multiple markets."""
    wallet_high: dict[str, list[dict]] = defaultdict(list)
    for c in candidates:
        if c["insider_score"] == "HIGH":
            wallet_high[c["wallet_address"]].append({
                "condition_id": c["condition_id"],
                "market_question": c["market_question"],
                "bet_size": c["bet_size"],
                "market_category": c["market_category"],
                "entry_timing_hours": c["entry_timing_hours"],
            })

    repeaters = []
    for wallet, mkts in wallet_high.items():
        if len(mkts) >= 2:
            categories = list({m["market_category"] for m in mkts})
            total_bet = sum(m["bet_size"] for m in mkts)
            repeaters.append({
                "wallet_address": wallet,
                "high_score_markets": len(mkts),
                "categories": categories,
                "total_informed_bets": round(total_bet, 2),
                "markets": mkts,
            })
    repeaters.sort(key=lambda r: r["high_score_markets"], reverse=True)
    return repeaters


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _print_leaderboard(
    candidates: list[dict],
    repeaters: list[dict],
    leaderboard: list[dict],
) -> None:
    """Print formatted results."""
    print(f"\n{'=' * 110}")
    print(f"  ON-CHAIN WHALE FINDER — Direct Polygon Blockchain Query")
    print(f"{'=' * 110}")

    high = sum(1 for c in candidates if c["insider_score"] == "HIGH")
    med = sum(1 for c in candidates if c["insider_score"] == "MEDIUM")
    low = sum(1 for c in candidates if c["insider_score"] == "LOW")
    print(f"  HIGH: {high}  |  MEDIUM: {med}  |  LOW: {low}  |  Total: {len(candidates)}")
    print()

    # Top candidates by insider score
    print(
        f"  {'#':>3}  {'Score':<6}  {'Wallet':<14}  {'Bet Size':>10}  "
        f"{'Timing':>8}  {'Events':>6}  {'Category':<14}  {'Market'}"
    )
    print(f"  {'-' * 105}")

    for i, c in enumerate(candidates[:40], 1):
        addr = c["wallet_address"][:6] + "..." + c["wallet_address"][-4:]
        q = c["market_question"][:35] + "..." if len(c["market_question"]) > 35 else c["market_question"]
        score_marker = {
            "HIGH": "\033[91mHIGH\033[0m",
            "MEDIUM": "\033[93mMED\033[0m",
            "LOW": "LOW",
        }.get(c["insider_score"], c["insider_score"])
        print(
            f"  {i:>3}  {score_marker:<15}  {addr:<14}  "
            f"${c['bet_size']:>9,.0f}  "
            f"{c['entry_timing_hours']:>6.0f}h  "
            f"{c['wallet_lifetime_events']:>6}  "
            f"{c['market_category']:<14}  {q}"
        )

    if repeaters:
        print(f"\n{'=' * 110}")
        print(f"  SERIAL INSIDERS (HIGH scores across multiple markets)")
        print(f"{'=' * 110}")
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

    if leaderboard:
        print(f"\n{'=' * 110}")
        print(f"  POLYMARKET LEADERBOARD — Top Profit Earners")
        print(f"{'=' * 110}")
        print(
            f"  {'#':>3}  {'Wallet':<14}  {'Name':<20}  {'Profit':>12}  "
            f"{'Volume':>14}  {'Markets':>8}  {'Win Rate':>8}"
        )
        print(f"  {'-' * 90}")
        for i, p in enumerate(leaderboard[:25], 1):
            addr = p["wallet_address"]
            if len(addr) > 10:
                addr = addr[:6] + "..." + addr[-4:]
            name = p["display_name"][:18] + ".." if len(p["display_name"]) > 20 else p["display_name"]
            wr = f"{p['win_rate']:.0%}" if p["win_rate"] > 0 else "—"
            print(
                f"  {i:>3}  {addr:<14}  {name:<20}  "
                f"${p['total_profit']:>11,.0f}  "
                f"${p['total_volume']:>13,.0f}  "
                f"{p['markets_traded']:>8}  {wr:>8}"
            )

    print(f"\n{'=' * 110}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run() -> dict:
    """Run on-chain whale discovery pipeline."""
    os.makedirs(DATA_DIR, exist_ok=True)

    print("\n  On-Chain Whale Finder — Direct Polygon Query")
    print("  " + "-" * 55)
    print(f"  Bypasses CLOB API — reads CTF Exchange contract directly")
    print(f"  Contract: {CTF_EXCHANGE}")
    print(f"  Lookback: {LOOKBACK_DAYS} days | Min size: ${MIN_USDC_VALUE:,}")
    print()

    # Step 1: Connect to Polygon
    w3 = _connect_polygon()

    # Step 2: Run on-chain scan and leaderboard fetch in parallel
    # (on-chain is sync/blocking, so we run leaderboard first)
    leaderboard_task = asyncio.create_task(_fetch_leaderboard())
    markets_task = asyncio.create_task(_fetch_resolved_markets())

    # On-chain scan (blocking)
    print("\n  === Phase 1: On-Chain Event Scan ===")
    events = _fetch_large_events(w3)

    if events:
        events = _resolve_timestamps(w3, events)

    # Await async tasks
    print("\n  === Phase 2: Market Resolution Data ===")
    markets = await markets_task

    print("\n  === Phase 3: Leaderboard ===")
    leaderboard = await leaderboard_task

    # Step 3: Cross-reference and score
    print("\n  === Phase 4: Cross-Reference & Scoring ===")
    candidates = _build_whale_profiles(events, markets) if events else []
    repeaters = _find_repeaters(candidates)

    # Step 4: Build combined registry
    # Merge leaderboard wallets with on-chain candidates
    onchain_wallets = {c["wallet_address"].lower() for c in candidates}
    overlap = sum(
        1 for p in leaderboard
        if p["wallet_address"].lower() in onchain_wallets
    )

    registry = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "polygon_blockchain_direct",
        "contract": CTF_EXCHANGE,
        "lookback_days": LOOKBACK_DAYS,
        "methodology": {
            "onchain": "TransferSingle events from CTF Exchange, filtered > $2K USDC",
            "leaderboard": "Public Polymarket leaderboard API",
            "scoring": "Dormant wallet + large bet + early timing = HIGH insider probability",
        },
        "summary": {
            "total_onchain_candidates": len(candidates),
            "high": sum(1 for c in candidates if c["insider_score"] == "HIGH"),
            "medium": sum(1 for c in candidates if c["insider_score"] == "MEDIUM"),
            "low": sum(1 for c in candidates if c["insider_score"] == "LOW"),
            "multi_market_repeaters": len(repeaters),
            "leaderboard_profiles": len(leaderboard),
            "leaderboard_onchain_overlap": overlap,
            "total_blocks_scanned": LOOKBACK_DAYS * BLOCKS_PER_DAY,
            "total_events_found": len(events),
        },
        "repeaters": repeaters,
        "whales": candidates,  # backward compat with whale_tracker
        "leaderboard": leaderboard,
    }

    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2, default=str)
    print(f"\n  Registry saved to {REGISTRY_PATH}")

    # Print results
    if candidates or leaderboard:
        _print_leaderboard(candidates, repeaters, leaderboard)
    else:
        print("  No candidates found from either source.")

    return registry


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
