"""
EdgeLab Live Arb Detector — Cross-platform arbitrage with Claude API semantic matching.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import redis.asyncio as aioredis

from src.risk.sizer import PLATFORM_FEE

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OUTPUT_CHANNEL = "opportunities:arb_live"
MIN_LOCKED_RETURN = 0.015  # 1.5%
CLAUDE_CONFIDENCE_THRESHOLD = 0.85
RAPIDFUZZ_THRESHOLD = 70
MATCH_CACHE_TTL = 600  # seconds
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TRADING_MODE = os.getenv("TRADING_MODE", "paper").lower()

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-haiku-4-5-20251001"


# ---------------------------------------------------------------------------
# Semantic matching
# ---------------------------------------------------------------------------

async def _claude_match(
    client: httpx.AsyncClient,
    poly_question: str,
    kalshi_question: str,
) -> dict[str, Any]:
    """
    Call Claude API to determine if two market questions refer to the same event.
    Returns {"match": bool, "confidence": float, "reasoning": str}.
    """
    prompt = (
        f"Given Polymarket question: '{poly_question}' and Kalshi question: "
        f"'{kalshi_question}', are these the same real-world event? "
        "Return JSON only: {\"match\": bool, \"confidence\": float, \"reasoning\": str}"
    )

    payload = {
        "model": _CLAUDE_MODEL,
        "max_tokens": 256,
        "messages": [{"role": "user", "content": prompt}],
    }

    resp = await client.post(
        _ANTHROPIC_URL,
        json=payload,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        timeout=15.0,
    )
    resp.raise_for_status()
    data = resp.json()

    content = data.get("content", [])
    text = content[0].get("text", "{}") if content else "{}"

    # Strip any markdown fencing if present
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1]) if len(lines) > 2 else text

    result = json.loads(text)
    return {
        "match": bool(result.get("match", False)),
        "confidence": float(result.get("confidence", 0.0)),
        "reasoning": str(result.get("reasoning", "")),
    }


def _rapidfuzz_match(poly_question: str, kalshi_question: str) -> dict[str, Any]:
    """Fuzzy string match fallback when ANTHROPIC_API_KEY is absent."""
    from rapidfuzz import fuzz
    score = fuzz.token_sort_ratio(poly_question.lower(), kalshi_question.lower())
    matched = score >= RAPIDFUZZ_THRESHOLD
    return {
        "match": matched,
        "confidence": round(score / 100.0, 3),
        "reasoning": f"rapidfuzz token_sort_ratio={score}",
    }


# ---------------------------------------------------------------------------
# Arb calculation
# ---------------------------------------------------------------------------

def compute_arb(
    poly_market: dict[str, Any],
    kalshi_market: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """
    Calculate locked arbitrage return between two correlated binary markets.

    Strategy: buy YES on the cheaper side, buy NO on the other.
    locked_return = spread - 2 * fee
    """
    poly_yes = float(poly_market.get("prob_yes") or poly_market.get("price") or 0)
    kalshi_yes = float(kalshi_market.get("prob_yes") or kalshi_market.get("price") or 0)

    if poly_yes <= 0 or poly_yes >= 1 or kalshi_yes <= 0 or kalshi_yes >= 1:
        return None

    spread = abs(poly_yes - kalshi_yes)
    locked_return = spread - 2 * PLATFORM_FEE

    if locked_return <= MIN_LOCKED_RETURN:
        return None

    # Determine which platform to buy YES vs NO on
    if poly_yes < kalshi_yes:
        poly_side = "yes"
        kalshi_side = "no"
    else:
        poly_side = "no"
        kalshi_side = "yes"

    # Estimate size from min liquidity across both legs
    poly_liq = float(poly_market.get("liquidity") or 0)
    kalshi_liq = float(kalshi_market.get("liquidity") or 0)
    min_liq = min(poly_liq, kalshi_liq) if poly_liq and kalshi_liq else max(poly_liq, kalshi_liq)
    # Conservative size: 1% of min liquidity, capped at $100
    size = round(min(min_liq * 0.01, 100.0), 2) if min_liq > 0 else 10.0

    return {
        "strategy": "arb_live",
        "poly_market": poly_market,
        "kalshi_market": kalshi_market,
        "poly_side": poly_side,
        "kalshi_side": kalshi_side,
        "spread": round(spread, 4),
        "locked_return": round(locked_return, 4),
        "size": size,
        "platform": "kalshi",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Live Arb Detector
# ---------------------------------------------------------------------------

class ArbDetectorLive:
    def __init__(self, redis_url: str = REDIS_URL):
        self.redis_url = redis_url
        self.redis: aioredis.Redis | None = None
        self.markets: dict[str, dict[str, dict[str, Any]]] = {
            "polymarket": {},
            "kalshi": {},
        }
        self._use_claude = bool(ANTHROPIC_API_KEY)

    async def _connect(self) -> None:
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
        logger.info(
            "ArbDetectorLive connected to Redis. Matching backend: %s",
            "claude" if self._use_claude else "rapidfuzz",
        )

    def _store_market(self, platform: str, payload: dict[str, Any]) -> None:
        market_id = payload.get("id") or payload.get("market_id")
        if not market_id:
            return
        self.markets[platform][market_id] = payload

    async def _get_match_result(
        self,
        client: httpx.AsyncClient,
        poly_id: str,
        kalshi_id: str,
        poly_question: str,
        kalshi_question: str,
    ) -> dict[str, Any]:
        """Return match result, checking Redis cache first."""
        assert self.redis is not None
        cache_key = f"arb:match:{poly_id}:{kalshi_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            try:
                return json.loads(cached)
            except json.JSONDecodeError:
                pass

        if self._use_claude:
            try:
                result = await _claude_match(client, poly_question, kalshi_question)
            except (httpx.RequestError, httpx.HTTPStatusError, json.JSONDecodeError, KeyError) as exc:
                logger.warning("Claude API error, falling back to rapidfuzz: %s", exc)
                result = _rapidfuzz_match(poly_question, kalshi_question)
        else:
            result = _rapidfuzz_match(poly_question, kalshi_question)

        # Cache result
        try:
            await self.redis.setex(cache_key, MATCH_CACHE_TTL, json.dumps(result))
        except Exception as exc:
            logger.debug("Failed to cache match result: %s", exc)

        return result

    async def _scan_for_arb(
        self,
        client: httpx.AsyncClient,
        updated_platform: str,
        updated_market_id: str,
    ) -> list[dict[str, Any]]:
        """Scan for arb opportunities involving the just-updated market."""
        other_platform = "kalshi" if updated_platform == "polymarket" else "polymarket"
        updated = self.markets[updated_platform].get(updated_market_id)
        if not updated:
            return []

        updated_question = updated.get("question", "")
        opportunities = []

        for other_id, other in self.markets[other_platform].items():
            other_question = other.get("question", "")

            if updated_platform == "polymarket":
                poly_id, kalshi_id = updated_market_id, other_id
                poly_q, kalshi_q = updated_question, other_question
                poly_market, kalshi_market = updated, other
            else:
                poly_id, kalshi_id = other_id, updated_market_id
                poly_q, kalshi_q = other_question, updated_question
                poly_market, kalshi_market = other, updated

            match_result = await self._get_match_result(
                client, poly_id, kalshi_id, poly_q, kalshi_q
            )

            if not match_result.get("match"):
                continue
            if float(match_result.get("confidence", 0)) < CLAUDE_CONFIDENCE_THRESHOLD:
                continue

            opp = compute_arb(poly_market, kalshi_market)
            if opp:
                opp["match_confidence"] = match_result.get("confidence")
                opp["match_reasoning"] = match_result.get("reasoning", "")
                opportunities.append(opp)

        return opportunities

    async def _handle_update(
        self,
        client: httpx.AsyncClient,
        platform: str,
        data: str,
    ) -> None:
        try:
            payload = json.loads(data)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to decode %s update: %s", platform, exc)
            return

        markets = payload if isinstance(payload, list) else [payload]
        assert self.redis is not None

        for market in markets:
            market_id = market.get("id") or market.get("market_id")
            if not market_id:
                continue

            self._store_market(platform, market)

            opportunities = await self._scan_for_arb(client, platform, market_id)
            for opp in opportunities:
                locked = opp["locked_return"]
                logger.info(
                    "Arb opportunity [live]: poly=%s kalshi=%s locked=%.2f%% mode=%s",
                    opp["poly_market"].get("id") or opp["poly_market"].get("market_id"),
                    opp["kalshi_market"].get("id") or opp["kalshi_market"].get("market_id"),
                    locked * 100,
                    TRADING_MODE,
                )

                if TRADING_MODE == "paper":
                    logger.info(
                        "[PAPER] Arb legs: buy %s on polymarket, buy %s on kalshi — locked=%.2f%%",
                        opp["poly_side"].upper(),
                        opp["kalshi_side"].upper(),
                        locked * 100,
                    )

                try:
                    await self.redis.publish(OUTPUT_CHANNEL, json.dumps(opp))
                except Exception as exc:
                    logger.error("Failed to publish arb_live opportunity: %s", exc)

    async def run(self) -> None:
        """Main entry point. Subscribes to both price channels and scans for arb."""
        await self._connect()
        assert self.redis is not None

        pubsub = self.redis.pubsub()
        await pubsub.subscribe("polymarket:prices", "kalshi:prices")
        logger.info(
            "ArbDetectorLive subscribed to polymarket:prices and kalshi:prices. "
            "Publishing to %s",
            OUTPUT_CHANNEL,
        )

        try:
            async with httpx.AsyncClient() as client:
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    channel = message["channel"]
                    data = message["data"]
                    if channel == "polymarket:prices":
                        await self._handle_update(client, "polymarket", data)
                    elif channel == "kalshi:prices":
                        await self._handle_update(client, "kalshi", data)
        except asyncio.CancelledError:
            logger.info("ArbDetectorLive shutting down.")
        except Exception as exc:
            logger.exception("Unexpected error in ArbDetectorLive: %s", exc)
            raise
        finally:
            await pubsub.unsubscribe()
            await self.redis.aclose()


async def run() -> None:
    detector = ArbDetectorLive()
    await detector.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
