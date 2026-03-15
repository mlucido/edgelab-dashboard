"""
Resolution Lag Arbitrage Detector

Subscribes to polymarket:prices and realworld:events Redis channels.
Fuzzy-matches real-world events against open markets and fires opportunities
when a real-world event suggests resolution but the market price hasn't caught up.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis
from rapidfuzz import fuzz

from src.calibration.loader import get_true_prob, load_calibration

from src.risk.sizer import PLATFORM_FEE

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OUTPUT_CHANNEL = "opportunities:resolution_lag"

FUZZY_MATCH_THRESHOLD = 50  # lowered — rapidfuzz is now a pre-filter only
SEMANTIC_CONFIDENCE_THRESHOLD = 0.80
SEMANTIC_CACHE_TTL = 300  # 5 minutes
NEAR_RESOLVED_PRICE_THRESHOLD = 0.97



def _hours_since(iso_timestamp: str) -> float:
    """Return hours elapsed since an ISO 8601 timestamp."""
    try:
        event_time = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - event_time
        return max(delta.total_seconds() / 3600, 0.01)  # avoid division by zero
    except (ValueError, AttributeError):
        return 1.0


def _rank_score(net_return: float, confidence: float, hours_since_event: float) -> float:
    """Higher = better opportunity. Decays as event ages."""
    return net_return * confidence * (1.0 / hours_since_event)


def _build_opportunity(
    market: dict[str, Any],
    event: dict[str, Any],
    confidence: float,
    calibration: dict[int, float] | None = None,
) -> dict[str, Any] | None:
    """
    Construct an opportunity payload if conditions are met.

    Conditions:
    - semantic confidence > SEMANTIC_CONFIDENCE_THRESHOLD (or fuzzy score if fallback)
    - event suggests YES resolution
    - market price still below NEAR_RESOLVED_PRICE_THRESHOLD
    """
    market_prob = float(market.get("prob_yes") or market.get("price") or 0)
    if market_prob >= NEAR_RESOLVED_PRICE_THRESHOLD:
        return None

    resolution_signal = event.get("resolution_signal", "").lower()
    if resolution_signal not in ("yes", "true", "resolved_yes", "1"):
        return None

    implied_true_prob = get_true_prob(market_prob, calibration)
    net_return = implied_true_prob * (1.0 - PLATFORM_FEE) / market_prob - 1.0
    if net_return <= 0:
        return None

    event_timestamp = event.get("timestamp", datetime.now(timezone.utc).isoformat())
    hours_since = _hours_since(event_timestamp)
    time_sensitivity_score = 1.0 / hours_since
    # confidence is already 0-1 for semantic matches; convert if it's a raw rapidfuzz score
    conf_normalized = confidence if confidence <= 1.0 else confidence / 100.0
    rank = _rank_score(net_return, conf_normalized, hours_since)

    return {
        "strategy": "resolution_lag",
        "market_id": market.get("market_id"),
        "market_question": market.get("question"),
        "current_price": market_prob,
        "implied_true_prob": round(implied_true_prob, 4),
        "net_return": round(net_return, 4),
        "confidence": round(confidence * 100.0 if confidence <= 1.0 else confidence, 2),
        "time_sensitivity_score": round(time_sensitivity_score, 4),
        "rank_score": round(rank, 6),
        "hours_since_real_event": round(hours_since, 2),
        "event_id": event.get("event_id"),
        "event_headline": event.get("headline"),
        "event_source": event.get("source"),
        "event_timestamp": event_timestamp,
        "platform": "kalshi",
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


class ResolutionLagDetector:
    def __init__(self, redis_url: str = REDIS_URL):
        self.redis_url = redis_url
        self.redis: aioredis.Redis | None = None
        # market_id -> market dict (live, updated from polymarket:prices)
        self.markets: dict[str, dict[str, Any]] = {}
        self.calibration = load_calibration()

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if api_key:
            try:
                import anthropic
                self._anthropic = anthropic.AsyncAnthropic(api_key=api_key)
                logger.info("Claude semantic matching enabled (model: claude-haiku-4-5-20251001)")
            except ImportError:
                logger.warning("anthropic SDK not installed — falling back to rapidfuzz-only matching")
                self._anthropic = None
        else:
            logger.warning("ANTHROPIC_API_KEY not set — falling back to rapidfuzz-only matching")
            self._anthropic = None

    async def _connect(self) -> None:
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        logger.info("Connected to Redis at %s", self.redis_url)

    def _update_market(self, payload: dict[str, Any]) -> None:
        market_id = payload.get("market_id")
        if market_id:
            self.markets[market_id] = payload

    async def _semantic_match(
        self, headline: str, question: str
    ) -> tuple[float, str, str]:
        """
        Call Claude API to assess how likely a news headline resolves a Polymarket question.

        Returns (confidence: float 0-1, resolved_direction: str, reasoning: str).
        Falls back to rapidfuzz score on error.
        """
        assert self.redis is not None

        cache_key = "cache:semantic:" + hashlib.md5((headline + question).encode()).hexdigest()
        cached = await self.redis.get(cache_key)
        if cached:
            try:
                data = json.loads(cached)
                return float(data["confidence"]), data["resolved_direction"], data["reasoning"]
            except (json.JSONDecodeError, KeyError):
                pass

        assert self._anthropic is not None
        prompt = (
            f"Given this news headline: '{headline}' and this Polymarket question: '{question}', "
            "on a scale of 0-1 how likely has this question already been resolved by real-world events? "
            'Return ONLY valid JSON: {"confidence": float, "resolved_direction": "YES"|"NO"|"UNKNOWN", "reasoning": string}'
        )

        try:
            message = await self._anthropic.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = message.content[0].text.strip()
            data = json.loads(raw)
            confidence = float(data["confidence"])
            resolved_direction = str(data.get("resolved_direction", "UNKNOWN"))
            reasoning = str(data.get("reasoning", ""))

            await self.redis.set(cache_key, json.dumps({
                "confidence": confidence,
                "resolved_direction": resolved_direction,
                "reasoning": reasoning,
            }), ex=SEMANTIC_CACHE_TTL)

            return confidence, resolved_direction, reasoning

        except Exception as exc:
            logger.warning("Claude API error during semantic match: %s — using rapidfuzz fallback", exc)
            fuzzy_score = fuzz.token_sort_ratio(headline, question) / 100.0
            return fuzzy_score, "UNKNOWN", f"API error fallback: {exc}"

    async def _find_matches(
        self, event: dict[str, Any]
    ) -> list[tuple[dict[str, Any], float]]:
        """
        Return (market, confidence) pairs that pass threshold.

        If Claude API is available:
          1. rapidfuzz pre-filter at threshold 50
          2. semantic scoring via Claude; keep only confidence > 0.80

        Falls back to rapidfuzz-only (threshold 50 -> 75 equivalent) when API unavailable.
        """
        headline = event.get("headline", "")
        if not headline:
            return []

        matches = []
        for market in self.markets.values():
            question = market.get("question", "")
            if not question:
                continue
            fuzzy_score = fuzz.token_sort_ratio(headline, question)
            if fuzzy_score <= FUZZY_MATCH_THRESHOLD:
                continue

            if self._anthropic is not None:
                confidence, resolved_direction, reasoning = await self._semantic_match(headline, question)
                logger.debug(
                    "Semantic match: score=%.2f direction=%s headline=%r question=%r reasoning=%s",
                    confidence, resolved_direction, headline[:60], question[:60], reasoning[:80],
                )
                if confidence > SEMANTIC_CONFIDENCE_THRESHOLD:
                    # Apply resolved_direction to event if not already set
                    if not event.get("resolution_signal") and resolved_direction != "UNKNOWN":
                        event["resolution_signal"] = resolved_direction
                    matches.append((market, confidence))
            else:
                # Rapidfuzz-only fallback: use original threshold of 75
                if fuzzy_score > 75:
                    matches.append((market, float(fuzzy_score)))

        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    async def _handle_price_update(self, data: str) -> None:
        try:
            payload = json.loads(data)
            if isinstance(payload, list):
                for item in payload:
                    self._update_market(item)
            else:
                self._update_market(payload)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to decode price update: %s", exc)

    async def _handle_real_world_event(self, data: str) -> None:
        try:
            event = json.loads(data)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to decode real-world event: %s", exc)
            return

        matches = await self._find_matches(event)
        if not matches:
            return

        opportunities = []
        for market, confidence in matches:
            opp = _build_opportunity(market, event, confidence, self.calibration)
            if opp:
                opportunities.append(opp)
                logger.info(
                    "Resolution lag opportunity: market=%s confidence=%.1f net_return=%.2f%%",
                    opp["market_id"],
                    confidence,
                    opp["net_return"] * 100,
                )

        if not opportunities:
            return

        # Sort by rank score descending
        opportunities.sort(key=lambda o: o["rank_score"], reverse=True)

        assert self.redis is not None
        for opp in opportunities:
            try:
                await self.redis.publish(OUTPUT_CHANNEL, json.dumps(opp))
            except Exception as exc:
                logger.error("Failed to publish opportunity: %s", exc)

        # Write latest opportunities list to Redis key so dashboard can read it
        try:
            await self.redis.set("latest:resolution_lag", json.dumps(opportunities))
        except Exception as exc:
            logger.error("Failed to set Redis key latest:resolution_lag: %s", exc)

    async def run(self) -> None:
        """Main entry point. Subscribes to channels and processes events indefinitely."""
        await self._connect()
        assert self.redis is not None

        pubsub = self.redis.pubsub()
        await pubsub.subscribe("polymarket:prices", "realworld:events")
        logger.info(
            "Subscribed to polymarket:prices and realworld:events. Publishing to %s",
            OUTPUT_CHANNEL,
        )

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                channel = message["channel"]
                data = message["data"]
                if channel == "polymarket:prices":
                    await self._handle_price_update(data)
                elif channel == "realworld:events":
                    await self._handle_real_world_event(data)
        except asyncio.CancelledError:
            logger.info("ResolutionLagDetector shutting down.")
        except Exception as exc:
            logger.exception("Unexpected error in ResolutionLagDetector: %s", exc)
            raise
        finally:
            await pubsub.unsubscribe()
            await self.redis.aclose()


async def run() -> None:
    detector = ResolutionLagDetector()
    await detector.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
