"""
Quote Engine — generates and manages limit orders for liquidity provision.

In PAPER mode: probabilistic fill simulation.
In LIVE mode: would submit orders via Kalshi API (not yet wired).

Fill model:
  P(fill within 5 min) = 0.3 for inside-spread orders
"""
import logging
import random
import time
from dataclasses import dataclass
from typing import Optional

from . import config
from .inventory_manager import InventoryManager

logger = logging.getLogger("liquidity_provision.quote_engine")

# Quote refresh: refresh if market moves > 2 cents or 60 seconds elapsed
QUOTE_REFRESH_SECS = 60
QUOTE_MOVE_THRESHOLD = 0.02
PAPER_FILL_PROB_5MIN = 0.30   # P(fill) for inside-spread limit orders


@dataclass
class Quote:
    market_id: str
    yes_bid: float      # our limit buy on YES
    yes_ask: float      # equivalent NO bid (sell YES at this price)
    size_usd: float
    posted_at: float    # time.time()
    spread_at_post: float


@dataclass
class PaperFill:
    market_id: str
    side: str           # 'YES' or 'NO'
    price: float
    size_usd: float
    fill_time: float
    pnl_if_pair_fills: float  # estimated spread capture if both sides fill


class QuoteEngine:
    def __init__(self, inventory_manager: InventoryManager):
        self.inventory = inventory_manager
        self.mode = config.MODE
        self._active_quotes: dict = {}   # market_id → Quote
        self._last_mid: dict = {}        # market_id → last known mid price

    def _compute_quotes(self, best_bid: float, best_ask: float) -> tuple:
        """
        Place our quotes 2 cents inside the spread.
        Returns (our_yes_bid, our_yes_ask).

        Example: spread 0.08 (bid=0.46, ask=0.54)
          our YES bid = 0.48  (2 cents better than best bid)
          our YES ask = 0.52  (2 cents better than best ask)
          If both fill: capture 0.52 - 0.48 = $0.04 spread
        """
        inside_offset = 0.02
        our_bid = round(best_bid + inside_offset, 4)
        our_ask = round(best_ask - inside_offset, 4)
        # Sanity: our bid must be < our ask
        if our_bid >= our_ask:
            mid = (best_bid + best_ask) / 2
            our_bid = round(mid - 0.01, 4)
            our_ask = round(mid + 0.01, 4)
        return our_bid, our_ask

    def _compute_size(self, price: float, headroom: float) -> float:
        """Quote size = min(MAX_POSITION_USD, inventory_headroom) / price."""
        if price <= 0:
            return 0.0
        budget = min(config.MAX_POSITION_USD, headroom)
        return round(budget / price, 4)

    def post_quotes(self, market_id: str, best_bid: float, best_ask: float) -> Optional[Quote]:
        """
        Generate a two-sided quote for a market.
        In PAPER mode: returns Quote object, does not call API.
        In LIVE mode: would submit to Kalshi (not yet implemented).
        Returns None if risk checks prevent quoting.
        """
        headroom = self.inventory.get_headroom_usd()
        if headroom <= 0:
            logger.debug(f"No inventory headroom for {market_id}")
            return None

        our_bid, our_ask = self._compute_quotes(best_bid, best_ask)
        size_usd = self._compute_size(our_bid, headroom)

        if size_usd < 0.50:   # min economic quote size
            return None

        spread_at_post = round(best_ask - best_bid, 4)
        quote = Quote(
            market_id=market_id,
            yes_bid=our_bid,
            yes_ask=our_ask,
            size_usd=size_usd,
            posted_at=time.time(),
            spread_at_post=spread_at_post,
        )
        self._active_quotes[market_id] = quote
        self._last_mid[market_id] = round((best_bid + best_ask) / 2, 4)

        if self.mode == "PAPER":
            logger.info(f"PAPER QUOTE: {market_id} YES bid={our_bid:.3f} ask={our_ask:.3f} "
                        f"size=${size_usd:.2f} spread={spread_at_post:.3f}")
        return quote

    def simulate_fills(self, market_id: str) -> list:
        """
        PAPER mode: probabilistically simulate fills for active quote on a market.
        Returns list of PaperFill (0, 1, or 2 fills).
        """
        if self.mode != "PAPER":
            return []

        quote = self._active_quotes.get(market_id)
        if not quote:
            return []

        fills = []
        age_secs = time.time() - quote.posted_at
        # Probability scales with time up to 5 minutes
        time_factor = min(age_secs / 300.0, 1.0)
        fill_prob = PAPER_FILL_PROB_5MIN * time_factor

        yes_filled = random.random() < fill_prob
        no_filled = random.random() < fill_prob

        if yes_filled:
            f = PaperFill(
                market_id=market_id,
                side="YES",
                price=quote.yes_bid,
                size_usd=quote.size_usd,
                fill_time=time.time(),
                pnl_if_pair_fills=round(quote.yes_ask - quote.yes_bid, 4),
            )
            fills.append(f)
            self.inventory.add_fill(market_id, "YES", quote.yes_bid, quote.size_usd)
            logger.info(f"PAPER FILL: {market_id} YES @ {quote.yes_bid:.3f} ${quote.size_usd:.2f}")

        if no_filled:
            no_price = round(1.0 - quote.yes_ask, 4)   # NO = 1 - YES ask
            f = PaperFill(
                market_id=market_id,
                side="NO",
                price=no_price,
                size_usd=quote.size_usd,
                fill_time=time.time(),
                pnl_if_pair_fills=round(quote.yes_ask - quote.yes_bid, 4),
            )
            fills.append(f)
            self.inventory.add_fill(market_id, "NO", no_price, quote.size_usd)
            logger.info(f"PAPER FILL: {market_id} NO @ {no_price:.3f} ${quote.size_usd:.2f}")

        return fills

    def should_refresh_quote(self, market_id: str, current_bid: float, current_ask: float) -> bool:
        """True if quote should be refreshed (stale or market moved)."""
        quote = self._active_quotes.get(market_id)
        if not quote:
            return True

        age = time.time() - quote.posted_at
        if age >= QUOTE_REFRESH_SECS:
            return True

        current_mid = (current_bid + current_ask) / 2
        prev_mid = self._last_mid.get(market_id, current_mid)
        if abs(current_mid - prev_mid) >= QUOTE_MOVE_THRESHOLD:
            logger.debug(f"Market moved {abs(current_mid - prev_mid):.3f} — refreshing {market_id}")
            return True

        return False

    def cancel_quote(self, market_id: str):
        """Remove active quote (no live cancellation needed in PAPER mode)."""
        self._active_quotes.pop(market_id, None)
        logger.debug(f"Quote cancelled: {market_id}")

    def get_active_quotes(self) -> dict:
        return dict(self._active_quotes)
