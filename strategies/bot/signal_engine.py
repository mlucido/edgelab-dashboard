"""
signal_engine.py — Momentum scoring and edge detection.

Takes a PriceState (from data_ingestor) and a list of live PolyMarkets,
returns a Signal if the conditions are met.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional

import config
from polymarket_client import PolyMarket

log = logging.getLogger(__name__)


@dataclass
class Signal:
    asset:         str
    direction:     str          # UP / DOWN
    momentum_pct:  float        # e.g. 0.55 (%)
    volume_ratio:  float        # e.g. 1.8
    signal_strength: str        # WEAK / MEDIUM / STRONG / VERY_STRONG / EXTREME
    implied_prob:  float        # our model's true probability (0–1)
    best_market:   PolyMarket   # the Polymarket contract to trade
    contract_side: str          # YES (for UP) or NO (for DOWN)
    market_price:  float        # current price of the contract we'd buy
    edge_pct:      float        # implied_prob - market_price (after fee)
    spot_price:    float        # BTC/ETH/SOL price at signal time
    window_tag:    str = "window_45s"  # which momentum window triggered: "window_45s" or "window_90s"
    signal_tier:   str = "STRONG_SIGNAL"  # "STRONG_SIGNAL" (>=0.15%) or "WEAK_SIGNAL" (0.10-0.15%)

    def __str__(self):
        return (
            f"Signal({self.asset} {self.direction} | "
            f"mom={self.momentum_pct:+.3f}% | "
            f"edge={self.edge_pct*100:.1f}% | "
            f"{self.signal_strength} | "
            f"market_price={self.market_price:.4f} | "
            f"implied={self.implied_prob:.4f} | "
            f"{self.window_tag} | {self.signal_tier})"
        )


def _classify_strength(abs_momentum: float) -> str:
    if abs_momentum < 0.60:
        return "WEAK"
    elif abs_momentum < 0.90:
        return "MEDIUM"
    elif abs_momentum < 1.30:
        return "STRONG"
    elif abs_momentum < 2.00:
        return "VERY_STRONG"
    else:
        return "EXTREME"


def _momentum_to_prob(abs_momentum: float) -> float:
    """
    Convert absolute momentum % to an implied true probability using
    the lookup table defined in config.MOMENTUM_PROB_TABLE.

    This is the RAW continuation probability — used for normal-priced contracts.
    For low-price (deep ITM) contracts, use _low_price_edge() instead.
    """
    for lo, hi, prob in config.MOMENTUM_PROB_TABLE:
        if lo <= abs_momentum < hi:
            return prob
    # Fallback: extreme move
    return config.MOMENTUM_PROB_TABLE[-1][2]


def _momentum_edge_bonus(abs_momentum: float) -> float:
    """
    For low-price contracts, return the marginal probability bonus
    that momentum adds over the market's implied probability.
    """
    for lo, hi, bonus in config.MOMENTUM_EDGE_BONUS:
        if lo <= abs_momentum < hi:
            return bonus
    if abs_momentum >= config.MOMENTUM_EDGE_BONUS[-1][0]:
        return config.MOMENTUM_EDGE_BONUS[-1][2]
    return 0.0


def _is_low_price_contract(market_price: float) -> bool:
    return market_price < config.LOW_PRICE_THRESHOLD


STRENGTH_ORDER = ["WEAK", "MEDIUM", "STRONG", "VERY_STRONG", "EXTREME"]


def _strength_meets_minimum(strength: str, minimum: str) -> bool:
    """Check if signal strength meets the minimum required level."""
    try:
        return STRENGTH_ORDER.index(strength) >= STRENGTH_ORDER.index(minimum)
    except ValueError:
        return False


def _find_matching_market(
    asset: str,
    direction: str,
    markets: List[PolyMarket]
) -> Optional[PolyMarket]:
    """
    From the list of live markets, find the best candidate:
    - Matches asset and direction
    - Is tradeable (liquidity, spread, time)
    - Prefer highest liquidity (most reliable fill)
    """
    # Normalize: config uses "BTC-USD", Kalshi uses "BTC"
    asset_base = asset.split("-")[0]
    candidates = [
        m for m in markets
        if (m.asset == asset or m.asset == asset_base)
        and m.direction == direction
        and m.is_tradeable
    ]
    if not candidates:
        return None
    # Pick the most liquid one
    return max(candidates, key=lambda m: m.liquidity)


def evaluate(
    asset: str,
    spot_price: float,
    momentum_pct: float,       # signed %, e.g. -0.55 for down
    volume_ratio: float,
    markets: List[PolyMarket],
    window_tag: str = "window_45s",
) -> Optional[Signal]:
    """
    Core signal evaluation function.

    Returns a Signal if all conditions are met, None otherwise.
    Does NOT enforce risk gates — that's the RiskManager's job.
    """
    abs_mom = abs(momentum_pct)

    # ── Condition 1: Minimum momentum (with weak signal support) ──────────
    if abs_mom < config.WEAK_SIGNAL_THRESHOLD:
        log.debug("[%s] Momentum %.4f%% below weak threshold %.4f%%",
                  asset, abs_mom, config.WEAK_SIGNAL_THRESHOLD)
        return None

    # Determine signal tier based on momentum strength
    signal_tier = "STRONG_SIGNAL" if abs_mom >= config.MOMENTUM_MIN_PCT else "WEAK_SIGNAL"

    # ── Condition 2: Volume confirmation (REMOVED) ──────────────────────
    # Volume gate removed: rolling_volume_avg() is stubbed and always
    # returns volume_1m, so volume_ratio is always 1.0. The check added
    # false confidence without filtering anything.

    direction = "UP" if momentum_pct > 0 else "DOWN"

    # ── Condition 3: Find a matching market ───────────────────────────────
    # For Kalshi one-sided books: search for the OPPOSITE direction market
    # where the cheap NO side aligns with our momentum bet.
    # UP momentum → find DOWN ("below X") markets → buy cheap NO ("won't drop")
    # DOWN momentum → find UP ("above X") markets → buy cheap NO ("won't rise")
    search_dir = "DOWN" if direction == "UP" else "UP"
    market = _find_matching_market(asset, search_dir, markets)
    if market is None:
        # Fallback: try same direction (for two-sided books)
        market = _find_matching_market(asset, direction, markets)
    if market is None:
        log.debug("[%s] No tradeable %s market found on Polymarket", asset, direction)
        return None

    # ── Condition 4: Reject placeholder prices ($0.50 = empty book) ─────
    PLACEHOLDER_LO, PLACEHOLDER_HI = 0.499, 0.501
    if PLACEHOLDER_LO <= market.yes_price <= PLACEHOLDER_HI:
        log.warning(
            "[%s] Rejected: yes_price $%.4f is a placeholder (empty book)",
            asset, market.yes_price,
        )
        return None
    if PLACEHOLDER_LO <= market.no_price <= PLACEHOLDER_HI:
        log.warning(
            "[%s] Rejected: no_price $%.4f is a placeholder (empty book)",
            asset, market.no_price,
        )
        return None

    # ── Condition 5: Edge check ───────────────────────────────────────────
    implied_prob = _momentum_to_prob(abs_mom)
    strength     = _classify_strength(abs_mom)

    # Which contract do we buy?
    if market.no_price < market.yes_price:
        contract_side = "NO"
        market_price  = market.no_price
    else:
        contract_side = "YES"
        market_price  = market.yes_price

    # ── Low-price contract recalibration ────────────────────────────────
    # Contracts < $0.10 are deep ITM on thin books. The displayed best-ask
    # is unreliable at size. We require stronger signals and apply slippage.
    if _is_low_price_contract(market_price):
        # Gate: require minimum momentum for cheap contracts
        if abs_mom < config.LOW_PRICE_MIN_MOMENTUM:
            log.debug(
                "[%s] Low-price contract ($%.2f) rejected: momentum %.4f%% "
                "below low-price threshold %.4f%%",
                asset, market_price, abs_mom, config.LOW_PRICE_MIN_MOMENTUM,
            )
            return None

        # Gate: require minimum signal strength
        if not _strength_meets_minimum(strength, config.LOW_PRICE_MIN_STRENGTH):
            log.debug(
                "[%s] Low-price contract ($%.2f) rejected: strength %s "
                "below minimum %s",
                asset, market_price, strength, config.LOW_PRICE_MIN_STRENGTH,
            )
            return None

        # Apply slippage-adjusted price (thin books, can't fill at best ask)
        adjusted_price = market_price + config.LOW_PRICE_SLIPPAGE
        log.debug(
            "[%s] Low-price slippage: $%.2f → $%.2f (+$%.2f)",
            asset, market_price, adjusted_price, config.LOW_PRICE_SLIPPAGE,
        )

        # Use marginal edge model instead of raw momentum probability.
        # The market already prices the high base win probability into
        # the contract — momentum only adds a small incremental edge.
        market_implied_prob = 1.0 - adjusted_price  # market's NO win probability
        bonus = _momentum_edge_bonus(abs_mom)
        implied_prob = min(market_implied_prob + bonus, 0.99)
        market_price = adjusted_price

        log.debug(
            "[%s] Low-price recalibrated: market_prob=%.2f + bonus=%.2f "
            "→ implied=%.2f",
            asset, market_implied_prob, bonus, implied_prob,
        )

    # Net edge after fee
    gross_edge = implied_prob - market_price
    net_edge   = gross_edge - config.POLYMARKET_FEE

    if net_edge < config.MIN_EDGE_PCT:
        log.debug(
            "[%s] Edge %.4f%% below minimum %.4f%% (gross=%.4f, fee=%.4f)",
            asset, net_edge * 100, config.MIN_EDGE_PCT * 100,
            gross_edge * 100, config.POLYMARKET_FEE * 100
        )
        return None

    signal = Signal(
        asset          = asset,
        direction      = direction,
        momentum_pct   = momentum_pct,
        volume_ratio   = volume_ratio,
        signal_strength= strength,
        implied_prob   = implied_prob,
        best_market    = market,
        contract_side  = contract_side,
        market_price   = market_price,
        edge_pct       = net_edge,
        spot_price     = spot_price,
        window_tag     = window_tag,
        signal_tier    = signal_tier,
    )
    log.info("🔔 %s", signal)
    return signal


def compute_kelly_size(
    implied_prob: float,
    market_price: float,
    bankroll: float,
) -> float:
    """
    Full Kelly:
        K = (p * b - q) / b
    where:
        p = probability of win (implied_prob)
        q = 1 - p
        b = net odds = (1 - market_price) / market_price
              (profit per $1 risked if win)

    We use fractional Kelly (config.KELLY_FRACTION) and hard-cap at
    config.MAX_POSITION_PCT of bankroll.
    """
    p = implied_prob
    q = 1 - p
    if market_price <= 0 or market_price >= 1:
        return 0.0

    b = (1 - market_price) / market_price   # net odds

    full_kelly = (p * b - q) / b
    if full_kelly <= 0:
        return 0.0

    fractional = full_kelly * config.KELLY_FRACTION
    dollar_size = fractional * bankroll

    # Hard cap
    max_size = bankroll * config.MAX_POSITION_PCT
    size = min(dollar_size, max_size)

    # Minimum trade floor (avoid dust trades)
    if size < 1.0:
        return 0.0

    return round(size, 2)
