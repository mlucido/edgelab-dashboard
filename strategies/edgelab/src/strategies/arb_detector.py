"""
Cross-Platform Arbitrage Detector

Subscribes to polymarket:prices and kalshi:prices Redis channels.
Matches markets across platforms using keyword overlap, then checks for
locked arbitrage spreads after fees.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

from src.risk.sizer import PLATFORM_FEE

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OUTPUT_CHANNEL = "opportunities:arb"
MIN_LOCKED_RETURN = 0.015  # 1.5%
KEYWORD_OVERLAP_THRESHOLD = 0.60  # 60%

# Common stopwords to exclude from keyword matching
_STOPWORDS = frozenset(
    {
        "will", "the", "a", "an", "be", "is", "are", "was", "were",
        "in", "on", "at", "to", "of", "for", "by", "with", "and",
        "or", "but", "not", "if", "it", "its", "this", "that",
        "win", "who", "what", "when", "how", "which", "their",
        "have", "has", "had", "do", "does", "did", "can", "could",
        "would", "should", "may", "might", "shall", "than", "then",
    }
)


def _extract_keywords(text: str) -> set[str]:
    """Lower-case, strip punctuation, remove stopwords. Returns set of significant words."""
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    return {t for t in tokens if t not in _STOPWORDS and len(t) > 2}


def _keyword_overlap(kw_a: set[str], kw_b: set[str]) -> float:
    """Jaccard-style overlap: |intersection| / |union|."""
    if not kw_a or not kw_b:
        return 0.0
    return len(kw_a & kw_b) / len(kw_a | kw_b)


def _compute_arb(
    poly_market: dict[str, Any],
    kalshi_market: dict[str, Any],
    fee: float = PLATFORM_FEE,
) -> dict[str, Any] | None:
    """
    Given YES prices on both platforms, compute the locked arbitrage return.

    Strategy: buy YES on platform with lower price, buy NO on the other.
    NO price = 1 - YES price.

    locked_return = (1 - total_cost) - 2*fee
    where total_cost = price_yes_low + price_no_high = price_yes_low + (1 - price_yes_high)
    """
    poly_yes = float(poly_market.get("prob_yes") or poly_market.get("price") or 0)
    kalshi_yes = float(kalshi_market.get("prob_yes") or kalshi_market.get("price") or 0)

    if poly_yes <= 0 or poly_yes >= 1 or kalshi_yes <= 0 or kalshi_yes >= 1:
        return None

    # To lock in a return we need: poly_yes + kalshi_no < 1  OR  kalshi_yes + poly_no < 1
    # i.e. the two YES prices sum to < 1 (both can't resolve YES simultaneously)
    total_cost = poly_yes + kalshi_yes
    # After buying YES on poly and YES on kalshi... wait — that's not arb.
    # Correct arb: one side resolves YES, other resolves NO.
    # Buy YES on cheaper platform + buy NO on other platform.
    # Cost = min_yes + (1 - max_yes)  => only profitable if min_yes + (1-max_yes) < 1
    # i.e. max_yes > min_yes, spread = max_yes - min_yes = gross locked return
    min_yes = min(poly_yes, kalshi_yes)
    max_yes = max(poly_yes, kalshi_yes)
    spread = max_yes - min_yes
    locked_return = spread - 2 * fee

    if locked_return <= MIN_LOCKED_RETURN:
        return None

    if poly_yes < kalshi_yes:
        buy_yes_platform = "polymarket"
        buy_yes_market = poly_market
        buy_no_platform = "kalshi"
        buy_no_market = kalshi_market
    else:
        buy_yes_platform = "kalshi"
        buy_yes_market = kalshi_market
        buy_no_platform = "polymarket"
        buy_no_market = poly_market

    return {
        "strategy": "arb",
        "spread": round(spread, 4),
        "locked_return": round(locked_return, 4),
        "total_cost": round(min_yes + (1.0 - max_yes), 4),
        "buy_yes": {
            "platform": buy_yes_platform,
            "market_id": buy_yes_market.get("market_id"),
            "question": buy_yes_market.get("question"),
            "price": float(buy_yes_market.get("prob_yes") or buy_yes_market.get("price") or 0),
            "liquidity": buy_yes_market.get("liquidity"),
        },
        "buy_no": {
            "platform": buy_no_platform,
            "market_id": buy_no_market.get("market_id"),
            "question": buy_no_market.get("question"),
            "no_price": round(1.0 - float(buy_no_market.get("prob_yes") or buy_no_market.get("price") or 0), 4),
            "liquidity": buy_no_market.get("liquidity"),
        },
        "platform": "kalshi",
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


class ArbDetector:
    def __init__(self, redis_url: str = REDIS_URL):
        self.redis_url = redis_url
        self.redis: aioredis.Redis | None = None
        # platform -> market_id -> {market_data, keywords}
        self.markets: dict[str, dict[str, dict[str, Any]]] = {
            "polymarket": {},
            "kalshi": {},
        }

    async def _connect(self) -> None:
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        logger.info("Connected to Redis at %s", self.redis_url)

    def _store_market(self, platform: str, payload: dict[str, Any]) -> None:
        market_id = payload.get("market_id")
        if not market_id:
            return
        keywords = _extract_keywords(payload.get("question", ""))
        self.markets[platform][market_id] = {**payload, "_keywords": keywords}

    def _find_arb_pairs(
        self, updated_platform: str, updated_market_id: str
    ) -> list[dict[str, Any]]:
        """Find arb opportunities involving the just-updated market."""
        other_platform = "kalshi" if updated_platform == "polymarket" else "polymarket"
        updated = self.markets[updated_platform].get(updated_market_id)
        if not updated:
            return []

        updated_kw = updated.get("_keywords", set())
        opportunities = []

        for other_id, other in self.markets[other_platform].items():
            other_kw = other.get("_keywords", set())
            overlap = _keyword_overlap(updated_kw, other_kw)
            if overlap < KEYWORD_OVERLAP_THRESHOLD:
                continue

            if updated_platform == "polymarket":
                opp = _compute_arb(updated, other)
            else:
                opp = _compute_arb(other, updated)

            if opp:
                opp["keyword_overlap"] = round(overlap, 3)
                opportunities.append(opp)

        return opportunities

    async def _handle_update(self, platform: str, data: str) -> None:
        try:
            payload = json.loads(data)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to decode %s update: %s", platform, exc)
            return

        markets = payload if isinstance(payload, list) else [payload]
        assert self.redis is not None

        batch_opportunities: list[dict[str, Any]] = []
        for market in markets:
            market_id = market.get("market_id")
            if not market_id:
                continue

            self._store_market(platform, market)
            opportunities = self._find_arb_pairs(platform, market_id)

            for opp in opportunities:
                logger.info(
                    "Arb opportunity: poly=%s kalshi=%s locked=%.2f%%",
                    opp["buy_yes"]["market_id"] if opp["buy_yes"]["platform"] == "polymarket" else opp["buy_no"]["market_id"],
                    opp["buy_yes"]["market_id"] if opp["buy_yes"]["platform"] == "kalshi" else opp["buy_no"]["market_id"],
                    opp["locked_return"] * 100,
                )
                try:
                    await self.redis.publish(OUTPUT_CHANNEL, json.dumps(opp))
                except Exception as exc:
                    logger.error("Failed to publish arb opportunity: %s", exc)
                batch_opportunities.append(opp)

        if batch_opportunities:
            # Write latest opportunities list to Redis key so dashboard can read it
            try:
                await self.redis.set("latest:arb", json.dumps(batch_opportunities))
            except Exception as exc:
                logger.error("Failed to set Redis key latest:arb: %s", exc)

    async def run(self) -> None:
        """Main entry point. Subscribes to both price channels and scans for arb."""
        await self._connect()
        assert self.redis is not None

        pubsub = self.redis.pubsub()
        await pubsub.subscribe("polymarket:prices", "kalshi:prices")
        logger.info(
            "Subscribed to polymarket:prices and kalshi:prices. Publishing to %s",
            OUTPUT_CHANNEL,
        )

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                channel = message["channel"]
                data = message["data"]
                if channel == "polymarket:prices":
                    await self._handle_update("polymarket", data)
                elif channel == "kalshi:prices":
                    await self._handle_update("kalshi", data)
        except asyncio.CancelledError:
            logger.info("ArbDetector shutting down.")
        except Exception as exc:
            logger.exception("Unexpected error in ArbDetector: %s", exc)
            raise
        finally:
            await pubsub.unsubscribe()
            await self.redis.aclose()


async def run() -> None:
    detector = ArbDetector()
    await detector.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
