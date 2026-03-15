"""
Whale Tracker Strategy — Follow high-performing wallets on Polymarket.

Loads whale_registry.json on startup, polls CLOB API every 30s for new
positions from whale wallets, and publishes signals to opportunities:whale.

Position sizing: 1-2% of whale's bet size, governed by confidence score
(based on whale's historical win rate and bet size relative to average).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OUTPUT_CHANNEL = "opportunities:whale"
LATEST_KEY = "latest:whale"

_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
)
# Try registries in priority order: v3 (onchain) > v2 (CLOB) > v1 (legacy)
REGISTRY_PATHS = [
    os.path.join(_DATA_DIR, "whale_registry_v3.json"),
    os.path.join(_DATA_DIR, "whale_registry_v2.json"),
    os.path.join(_DATA_DIR, "whale_registry.json"),
]
# Backwards compat aliases
REGISTRY_PATH = REGISTRY_PATHS[1]
REGISTRY_V1_PATH = REGISTRY_PATHS[2]
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com/markets"

POLL_INTERVAL = 30  # seconds
MIN_WHALE_BET = 500.0  # minimum whale bet to trigger signal
MAX_DAYS_TO_RESOLUTION = 14
MIN_LIQUIDITY = 1000.0  # market must have enough liquidity for us

# Public (unauthenticated) Polymarket data endpoint — avoids 401 on CLOB API
POLYMARKET_DATA_API = "https://data-api.polymarket.com"
# Per-wallet retry cooldown: track when we last got an error so we back off
_wallet_retry_after: dict[str, float] = {}  # wallet_addr -> monotonic timestamp
WALLET_COOLDOWN_SECS = 300  # 5-minute cooldown after a failed fetch

# Position sizing: 1-2% of whale bet
SIZING_LOW = 0.01
SIZING_HIGH = 0.02


def _load_registry(path: str | None = None) -> list[dict]:
    """Load whale registry from JSON. Tries v3 > v2 > v1, skipping empty registries."""
    paths = [path] if path else REGISTRY_PATHS

    for registry_path in paths:
        try:
            with open(registry_path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            continue
        except json.JSONDecodeError as exc:
            logger.warning("Failed to parse whale registry at %s: %s", registry_path, exc)
            continue

        # Extract whale list (v2/v3 use "candidates", v1 uses "whales")
        if isinstance(data, dict):
            whales = data.get("candidates", []) or data.get("whales", [])
        elif isinstance(data, list):
            whales = data
        else:
            whales = []

        if whales:
            logger.info("Loaded %d whales from %s", len(whales), registry_path)
            return whales

        # Registry exists but is empty — log details and try next
        summary = data.get("summary", {}) if isinstance(data, dict) else {}
        logger.warning(
            "Whale registry %s has 0 entries (markets_analyzed=%s, generated=%s) — trying next",
            os.path.basename(registry_path),
            summary.get("total_candidates", data.get("total_markets_analyzed", "?")),
            data.get("generated_at", "?") if isinstance(data, dict) else "?",
        )

    logger.warning(
        "All whale registries empty — run whale_finder_v2.py or onchain_whale_finder.py "
        "to populate. Checked: %s",
        ", ".join(os.path.basename(p) for p in paths),
    )
    return []


def _confidence_score(whale: dict, bet_size: float) -> float:
    """
    Compute confidence score (0-100) based on v2 insider scoring:
    - insider_score: HIGH=80, MEDIUM=60, LOW=30 base
    - Bet size relative to $2K threshold (bigger = more confident)
    - Dormancy bonus (fewer lifetime trades = more confident)
    Falls back to v1 win_rate logic if v2 fields missing.
    """
    insider_score = whale.get("insider_score", "")
    if insider_score:
        # v2 scoring
        base = {"HIGH": 80, "MEDIUM": 60, "LOW": 30}.get(insider_score, 40)
        # Bet size bonus: 0-15 points ($2K=0, $10K+=15)
        size_bonus = min(15, max(0, (bet_size - 2000) / 8000 * 15))
        # Dormancy bonus: 0-5 points (fewer trades = more insider-like)
        lifetime = whale.get("wallet_lifetime_trades", 50)
        dormancy_bonus = max(0, min(5, (20 - lifetime) / 4))
        return round(min(100, base + size_bonus + dormancy_bonus), 1)

    # v1 fallback
    win_rate = whale.get("win_rate", 0.5)
    avg_bet = whale.get("avg_bet_size", 1000.0)
    wr_score = max(0, min(60, (win_rate - 0.50) * 200))
    size_ratio = bet_size / avg_bet if avg_bet > 0 else 1.0
    size_score = max(0, min(40, size_ratio * 20))
    return round(wr_score + size_score, 1)


def _compute_our_size(whale_bet: float, confidence: float) -> float:
    """
    Size our position at 1-2% of whale bet, scaled by confidence.
    Low confidence (< 50) → 1%, high confidence (> 80) → 2%.
    """
    # Linear interpolation between 1% and 2% based on confidence
    t = max(0, min(1, (confidence - 40) / 60))  # 0 at conf=40, 1 at conf=100
    pct = SIZING_LOW + t * (SIZING_HIGH - SIZING_LOW)
    return round(whale_bet * pct, 2)


class WhaleTracker:
    """Real-time whale position monitor and signal generator."""

    def __init__(self):
        self.whales: list[dict] = []
        self.whale_addrs: set[str] = set()
        self.seen_trades: set[str] = set()  # dedup by trade hash
        self.redis: aioredis.Redis | None = None
        self.recent_signals: list[dict] = []

    async def run(self) -> None:
        """Main loop — load registry, poll for whale activity."""
        self.whales = _load_registry()
        if not self.whales:
            logger.warning("No whales loaded — whale tracker inactive")
            # Keep running but idle (registry may be added later)
            while True:
                await asyncio.sleep(60)
                self.whales = _load_registry()
                if self.whales:
                    break

        self.whale_addrs = {w["wallet_address"] for w in self.whales}
        self._whale_lookup = {w["wallet_address"]: w for w in self.whales}

        self.redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
        logger.info(
            "Whale tracker started — monitoring %d whale wallets, polling every %ds",
            len(self.whales), POLL_INTERVAL,
        )

        try:
            while True:
                await self._poll_cycle()
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Whale tracker shutting down")
        finally:
            if self.redis:
                await self.redis.aclose()

    async def _poll_cycle(self) -> None:
        """One poll cycle — check for new whale trades."""
        async with httpx.AsyncClient(timeout=15.0) as client:
            for whale in self.whales:
                addr = whale["wallet_address"]
                try:
                    trades = await self._fetch_wallet_trades(client, addr)
                    for trade in trades:
                        await self._process_trade(client, whale, trade)
                except Exception as exc:
                    logger.debug("Error polling whale %s: %s", addr[:10], exc)

    async def _fetch_wallet_trades(
        self, client: httpx.AsyncClient, wallet: str,
    ) -> list[dict]:
        """Fetch recent trades for a wallet using the public data API (no auth required).

        Falls back to an empty list on any error and sets a 5-minute cooldown
        for the wallet to avoid hammering a failing endpoint.
        """
        import time as _time

        # Honour per-wallet cooldown
        retry_after = _wallet_retry_after.get(wallet, 0.0)
        if _time.monotonic() < retry_after:
            return []

        # Primary: public data API (unauthenticated)
        try:
            resp = await client.get(
                f"{POLYMARKET_DATA_API}/trades",
                params={"maker_address": wallet, "limit": 20},
            )
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, list) else data.get("trades", data.get("data", []))
            if resp.status_code in (401, 403):
                logger.warning(
                    "Whale tracker: auth error %d on public endpoint for wallet %s… — cooling down %ds",
                    resp.status_code, wallet[:10], WALLET_COOLDOWN_SECS,
                )
                _wallet_retry_after[wallet] = _time.monotonic() + WALLET_COOLDOWN_SECS
                return []
            # Non-auth non-200 — fall through to CLOB fallback
        except Exception as exc:
            logger.warning("Whale tracker: public data API error for %s…: %s — cooling down", wallet[:10], exc)
            _wallet_retry_after[wallet] = _time.monotonic() + WALLET_COOLDOWN_SECS
            return []

        # Fallback: CLOB trades endpoint (may require auth on some environments)
        try:
            resp = await client.get(
                f"{CLOB_BASE}/trades",
                params={"maker_address": wallet, "limit": 20},
            )
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, list) else data.get("trades", data.get("data", []))
            logger.warning(
                "Whale tracker: CLOB fallback returned %d for wallet %s… — cooling down %ds",
                resp.status_code, wallet[:10], WALLET_COOLDOWN_SECS,
            )
            _wallet_retry_after[wallet] = _time.monotonic() + WALLET_COOLDOWN_SECS
            return []
        except Exception as exc:
            logger.warning("Whale tracker: CLOB fallback error for %s…: %s — cooling down", wallet[:10], exc)
            _wallet_retry_after[wallet] = _time.monotonic() + WALLET_COOLDOWN_SECS
            return []

    async def _process_trade(
        self,
        client: httpx.AsyncClient,
        whale: dict,
        trade: dict,
    ) -> None:
        """Process a single whale trade and potentially emit a signal."""
        # Dedup
        trade_id = trade.get("id") or trade.get("trade_id", "")
        trade_hash = f"{whale['wallet_address']}:{trade_id}"
        if trade_hash in self.seen_trades:
            return
        self.seen_trades.add(trade_hash)

        # Cap dedup set size
        if len(self.seen_trades) > 10000:
            self.seen_trades = set(list(self.seen_trades)[-5000:])

        # Check bet size
        price = float(trade.get("price", 0) or 0)
        size = float(trade.get("size", 0) or trade.get("amount", 0) or 0)
        if price <= 0 or price >= 1 or size <= 0:
            return

        bet_amount = price * size
        if bet_amount < MIN_WHALE_BET:
            return

        market_id = trade.get("asset_id") or trade.get("market") or trade.get("token_id", "")
        if not market_id:
            return

        # Fetch market details for guardrail checks
        market_info = await self._fetch_market_info(client, market_id)

        logger.info(
            "Whale trade detected: %s… bet $%.0f on %s (price=%.3f)",
            whale["wallet_address"][:10], bet_amount, market_id[:20], price,
        )

        # Guardrails
        liquidity = float(market_info.get("liquidity", 0) or 0)
        if liquidity < MIN_LIQUIDITY:
            logger.debug("Guardrail: liquidity $%.0f < $%.0f — skipping", liquidity, MIN_LIQUIDITY)
            return

        days_to_res = self._days_to_resolution(market_info)
        if days_to_res > MAX_DAYS_TO_RESOLUTION:
            logger.debug("Guardrail: %d days to resolution > %d — skipping", days_to_res, MAX_DAYS_TO_RESOLUTION)
            return

        # Compute confidence and sizing
        confidence = _confidence_score(whale, bet_amount)
        our_size = _compute_our_size(bet_amount, confidence)

        signal = {
            "strategy": "whale",
            "market_id": market_id,
            "market_question": market_info.get("question", market_id),
            "current_price": price,
            "implied_true_prob": min(price + 0.03, 0.999),  # slight upward adjust for whale edge
            "liquidity": liquidity,
            "days_to_resolution": days_to_res,
            "whale_address": whale["wallet_address"],
            "whale_win_rate": whale.get("win_rate", 0),
            "whale_bet_size": bet_amount,
            "whale_avg_bet": whale.get("avg_bet_size", 0),
            "confidence_score": confidence,
            "suggested_size": our_size,
            "platform": "kalshi",
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

        # Publish
        if self.redis:
            await self.redis.publish(OUTPUT_CHANNEL, json.dumps(signal))
            # Update latest signals
            self.recent_signals.append(signal)
            self.recent_signals = self.recent_signals[-10:]  # keep last 10
            await self.redis.set(LATEST_KEY, json.dumps(self.recent_signals))

        logger.info(
            "Whale signal published: whale=%s… market=%s bet=$%.0f conf=%.0f our_size=$%.2f",
            whale["wallet_address"][:10],
            market_id[:20],
            bet_amount,
            confidence,
            our_size,
        )

    async def _fetch_market_info(self, client: httpx.AsyncClient, market_id: str) -> dict:
        """Fetch market details from Gamma API."""
        try:
            resp = await client.get(f"{GAMMA_URL}/{market_id}")
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        # Fallback: try CLOB market endpoint
        try:
            resp = await client.get(f"{CLOB_BASE}/markets/{market_id}")
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return {"market_id": market_id}

    def _days_to_resolution(self, market_info: dict) -> float:
        """Compute days until resolution from market info."""
        end_date = market_info.get("endDate") or market_info.get("end_date", "")
        if not end_date:
            return 999.0
        try:
            end_dt = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
            return max((end_dt - datetime.now(timezone.utc)).days, 0)
        except (ValueError, AttributeError):
            return 999.0


# ---------------------------------------------------------------------------
# Module-level run function (for main.py integration)
# ---------------------------------------------------------------------------

_tracker = WhaleTracker()


async def run() -> None:
    """Entry point for main.py async task."""
    await _tracker.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
