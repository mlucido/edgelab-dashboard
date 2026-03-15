"""
Near-Certainty Threshold Scanner

Subscribes to polymarket:prices and fires opportunities for markets
that cross a configurable near-certainty threshold, using a calibration
table to estimate true probability and compute EV / Kelly / annualized return.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

from src.calibration.loader import get_true_prob, load_calibration
from src.risk.sizer import _kelly_fraction, annualized_return as _annualized_return, PLATFORM_FEE

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OUTPUT_CHANNEL = "opportunities:threshold"

# Default threshold percentage — lowered from 90 to 5 for live unresolved markets.
# Live Polymarket data: 83% of markets are priced below 0.10 (unlikely events).
# We rely on EV and liquidity filters to select, not a price floor.
_raw_threshold = os.getenv("THRESHOLD_PCT", "5")
try:
    THRESHOLD_PCT = float(_raw_threshold)
except ValueError:
    THRESHOLD_PCT = 5.0

THRESHOLD = THRESHOLD_PCT / 100.0



def _build_opportunity(
    market: dict[str, Any],
    calibration: dict[int, float],
) -> dict[str, Any] | None:
    """
    Evaluate a single market update and return an opportunity dict if criteria are met.

    Criteria:
    - market price >= THRESHOLD
    - liquidity > $500
    - days_to_resolution < 30 if annualized_return > 100%
    """
    market_prob = float(market.get("prob_yes") or market.get("price") or 0)
    if market_prob < THRESHOLD:
        return None

    # Hard floor: never consider markets priced below 88 cents
    # Raised from 0.84 — borderline 84-88c markets have catastrophic loss asymmetry
    MIN_MARKET_PRICE = 0.88
    if market_prob < MIN_MARKET_PRICE:
        return None

    _liq_raw = market.get("liquidity")
    liquidity = float(_liq_raw) if _liq_raw is not None else 0.0
    if liquidity <= 50:
        return None

    _dtr_raw = market.get("days_to_resolution")
    days_to_resolution = float(_dtr_raw) if _dtr_raw is not None else 7.0  # default 7 days if unknown
    if days_to_resolution > 30:
        return None

    true_prob = get_true_prob(market_prob, calibration)

    # Hard minimum: never trade below 88% implied true probability
    # Raised from 0.84 — low true_prob trades have unacceptable loss/win asymmetry
    MIN_ENTRY_PROB = 0.88
    if true_prob < MIN_ENTRY_PROB:
        return None

    ev = true_prob * (1.0 - PLATFORM_FEE) / market_prob - 1.0

    # Require minimum 0.1% EV (lowered from 0.5% for live mid-range markets)
    if ev < 0.001:
        return None

    kelly = _kelly_fraction(market_prob, true_prob)
    kelly_quarter = 0.25 * max(kelly, 0.0)

    ann_return = _annualized_return(ev, days_to_resolution) if days_to_resolution > 0 else 0.0

    return {
        "strategy": "threshold",
        "market_id": market.get("market_id"),
        "market_question": market.get("question"),
        "current_price": market_prob,
        "threshold_pct": THRESHOLD_PCT,
        "implied_true_prob": round(true_prob, 4),
        "ev": round(ev, 4),
        "kelly_fraction": round(kelly, 4),
        "kelly_quarter": round(kelly_quarter, 4),
        "days_to_resolution": days_to_resolution,
        "annualized_return": round(ann_return, 4),
        "liquidity": liquidity,
        "platform": "kalshi",
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


class ThresholdScanner:
    def __init__(self, redis_url: str = REDIS_URL):
        self.redis_url = redis_url
        self.redis: aioredis.Redis | None = None
        self.calibration = load_calibration()

    async def _connect(self) -> None:
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        logger.info("Connected to Redis at %s", self.redis_url)

    async def _handle_price_update(self, data: str) -> None:
        try:
            payload = json.loads(data)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to decode price update: %s", exc)
            return

        markets = payload if isinstance(payload, list) else [payload]
        assert self.redis is not None

        # --- Debug logging: filter pipeline stats ---
        n_total = len(markets)
        n_below_threshold = 0
        n_low_liquidity = 0
        n_far_resolution = 0
        n_negative_ev = 0
        n_passed = 0

        batch_opportunities = []
        for market in markets:
            market_prob = float(market.get("prob_yes") or market.get("price") or 0)
            _liq_raw = market.get("liquidity")
            liquidity = float(_liq_raw) if _liq_raw is not None else 0.0
            _dtr_raw = market.get("days_to_resolution")
            days_to_res = float(_dtr_raw) if _dtr_raw is not None else None

            if market_prob < THRESHOLD:
                n_below_threshold += 1
                continue
            if liquidity is None or liquidity <= 50:
                n_low_liquidity += 1
                continue
            if days_to_res is not None and days_to_res > 30:
                n_far_resolution += 1
                continue

            opp = _build_opportunity(market, self.calibration)
            if opp is None:
                n_negative_ev += 1
                # Log the first few EV rejections for diagnostics
                true_prob = get_true_prob(market_prob, self.calibration)
                ev = true_prob * (1.0 - PLATFORM_FEE) / market_prob - 1.0 if market_prob > 0 else 0
                logger.debug(
                    "EV reject: market=%s price=%.3f true_prob=%.3f ev=%.4f",
                    market.get("market_id", "?")[:30], market_prob, true_prob, ev,
                )
                continue

            n_passed += 1
            logger.info(
                "Threshold opportunity: market=%s price=%.3f ev=%.2f%% ann=%.0f%%",
                opp["market_id"],
                opp["current_price"],
                opp["ev"] * 100,
                opp["annualized_return"] * 100,
            )
            try:
                await self.redis.publish(OUTPUT_CHANNEL, json.dumps(opp))
            except Exception as exc:
                logger.error("Failed to publish opportunity: %s", exc)
            batch_opportunities.append(opp)

        logger.info(
            "Scan cycle: %d markets | %d below_threshold | %d low_liq | "
            "%d far_resolution | %d neg_ev | %d PASSED",
            n_total, n_below_threshold, n_low_liquidity,
            n_far_resolution, n_negative_ev, n_passed,
        )

        if batch_opportunities:
            # Write latest opportunities list to Redis key so dashboard can read it
            try:
                await self.redis.set("latest:threshold", json.dumps(batch_opportunities))
            except Exception as exc:
                logger.error("Failed to set Redis key latest:threshold: %s", exc)

    async def run(self) -> None:
        """Main entry point. Subscribes to polymarket:prices and scans for threshold crossings."""
        await self._connect()
        assert self.redis is not None

        pubsub = self.redis.pubsub()
        await pubsub.subscribe("polymarket:prices")
        logger.info(
            "Subscribed to polymarket:prices (threshold=%.0f%%). Publishing to %s",
            THRESHOLD_PCT,
            OUTPUT_CHANNEL,
        )

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                await self._handle_price_update(message["data"])
        except asyncio.CancelledError:
            logger.info("ThresholdScanner shutting down.")
        except Exception as exc:
            logger.exception("Unexpected error in ThresholdScanner: %s", exc)
            raise
        finally:
            await pubsub.unsubscribe()
            await self.redis.aclose()


async def run() -> None:
    scanner = ThresholdScanner()
    await scanner.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
