"""
Edge calculator: computes trading edge from momentum signal + market odds.
"""
import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

from . import config
from .momentum_scorer import MomentumSignal

logger = logging.getLogger("sports_momentum.edge_calculator")

KALSHI_FEE = 0.02  # 2% fee subtracted from edge


@dataclass
class EdgeSignal:
    game_id: str
    sport: str
    ticker: str
    market_type: str
    side: str                  # "YES" or "NO"
    market_price: float        # current market price (0-1)
    fair_value: float          # momentum-adjusted fair value
    edge_pct: float            # net edge after fees
    position_size_usd: float
    momentum_score: float
    confidence: str
    factors: list[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def _kelly_fraction(edge: float, odds: float) -> float:
    """
    Full Kelly criterion: f = (bp - q) / b
    where b = (1/odds - 1), p = win probability, q = 1 - p
    Capped at 0.25 of capital.
    """
    if odds <= 0 or odds >= 1:
        return 0.0
    b = (1.0 / odds) - 1.0
    p = odds + edge
    q = 1.0 - p
    if b <= 0:
        return 0.0
    kelly = (b * p - q) / b
    return max(0.0, min(kelly, 0.25))


def compute_edge(momentum: MomentumSignal, market: dict) -> Optional[EdgeSignal]:
    """
    Compute trading edge from a momentum signal + market.

    Logic:
    1. Determine which side (YES/NO) the momentum favors
    2. Estimate fair value = market_price + implied_prob_shift
    3. Compute raw edge = fair_value - market_price
    4. Subtract 2% Kalshi fee
    5. Return EdgeSignal only if net edge > MIN_EDGE_PCT
    6. Position sizing: HIGH=full kelly, MEDIUM=50% kelly, LOW=skip
    """
    if momentum.confidence == "LOW":
        return None  # Skip low confidence signals

    market_type = market.get("market_type", "")
    ticker = market.get("ticker", "")
    volume = market.get("volume", 0)

    # Liquidity check
    if volume < config.MIN_MARKET_LIQUIDITY_USD:
        logger.debug(f"Market {ticker} below liquidity threshold (vol={volume})")
        return None

    # Determine which side momentum favors
    # For game_winner markets: YES = home team wins
    home_match = market.get("home_match", 0)
    away_match = market.get("away_match", 0)

    if market_type == "game_winner":
        if momentum.direction == "HOME":
            # Momentum favors home — buy YES
            side = "YES"
            market_price = market.get("yes_ask", 0.5)
        else:
            # Momentum favors away — buy NO (home loses)
            side = "NO"
            market_price = market.get("no_ask", 0.5)
    elif market_type == "first_half":
        if momentum.direction == "HOME":
            side = "YES"
            market_price = market.get("yes_ask", 0.5)
        else:
            side = "NO"
            market_price = market.get("no_ask", 0.5)
    else:
        # total_points and other types — skip for now
        return None

    if market_price <= 0 or market_price >= 1:
        return None

    # Fair value: market price shifted by momentum
    prob_shift = momentum.implied_prob_shift
    fair_value = market_price + prob_shift
    fair_value = max(0.01, min(0.99, fair_value))

    # Raw edge
    raw_edge = fair_value - market_price

    # Subtract fee
    net_edge = raw_edge - KALSHI_FEE

    if net_edge < config.MIN_EDGE_PCT:
        logger.debug(f"Edge {net_edge:.3f} below threshold {config.MIN_EDGE_PCT} for {ticker}")
        return None

    # Position sizing based on confidence
    if momentum.confidence == "HIGH":
        kelly = _kelly_fraction(net_edge, market_price)
        position_size = min(config.MAX_POSITION_USD * kelly * 4, config.MAX_POSITION_USD)
    elif momentum.confidence == "MEDIUM":
        kelly = _kelly_fraction(net_edge, market_price)
        position_size = min(config.MAX_POSITION_USD * kelly * 2, config.MAX_POSITION_USD * 0.5)
    else:
        # LOW confidence already filtered above, but safety net
        return None

    position_size = max(1.0, round(position_size, 2))

    return EdgeSignal(
        game_id=momentum.game_id,
        sport=momentum.sport,
        ticker=ticker,
        market_type=market_type,
        side=side,
        market_price=round(market_price, 3),
        fair_value=round(fair_value, 3),
        edge_pct=round(net_edge, 4),
        position_size_usd=position_size,
        momentum_score=momentum.score,
        confidence=momentum.confidence,
        factors=momentum.factors,
    )
