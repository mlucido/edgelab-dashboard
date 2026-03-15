"""
Funding Rate Arb Engine — main orchestrator.
Strategy 6: trades Kalshi range markets based on crypto perpetual funding rate signals.
Runs in PAPER mode by default. Set FUNDING_ARB_MODE=LIVE to go live.
"""
import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

# Add EdgeLab root to path for framework imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from . import config
from .funding_fetcher import get_all_funding_rates, get_funding_history
from .rate_analyzer import analyze_funding
from .market_matcher import match_asset_to_markets
from .edge_calculator import calculate_edge

logger = logging.getLogger("funding_arb.engine")
log_dir = Path(__file__).parent.parent.parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
_fh = logging.FileHandler(log_dir / "funding_rate_arb.log")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
logger.addHandler(_fh)
_ch = logging.StreamHandler()
_ch.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
logger.addHandler(_ch)
logger.setLevel(logging.INFO)

PAPER_TRADES_PATH = Path(__file__).parent / "sim" / "results" / "paper_trades.json"

# Approximate current prices for matching (updated each cycle from a public source)
_PRICE_FALLBACK = {"BTC": 85000.0, "ETH": 3200.0, "SOL": 175.0}


def _fetch_current_price(asset: str) -> float:
    """Fetch spot price from Coinbase public API. Falls back to constant."""
    try:
        import httpx
        symbol_map = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}
        sym = symbol_map.get(asset, f"{asset}-USD")
        with httpx.Client(timeout=8) as client:
            resp = client.get(f"https://api.coinbase.com/v2/prices/{sym}/spot")
            if resp.status_code == 200:
                return float(resp.json()["data"]["amount"])
    except Exception as e:
        logger.warning(f"Price fetch failed for {asset}: {e}")
    return _PRICE_FALLBACK.get(asset, 1000.0)


class FundingArbEngine:
    def __init__(self):
        self.mode = config.MODE
        if os.environ.get("FUNDING_ARB_PROMOTE_TO_LIVE") == "true" and self.mode == "PAPER":
            self.mode = "LIVE"
        self.paper_trades: list = []
        self.paper_capital = config.STARTING_CAPITAL_USD
        self.consecutive_losses = 0
        self.reduced_size = False
        self.paused_until = 0.0
        self.start_time = time.time()
        self._load_paper_trades()
        logger.info(f"FundingArbEngine initialized in {self.mode} mode")

    # ── Persistence ────────────────────────────────────────────────────────────

    def _load_paper_trades(self):
        if PAPER_TRADES_PATH.exists():
            try:
                self.paper_trades = json.loads(PAPER_TRADES_PATH.read_text())
                logger.info(f"Loaded {len(self.paper_trades)} existing paper trades")
            except Exception:
                self.paper_trades = []

    def _save_paper_trades(self):
        PAPER_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
        PAPER_TRADES_PATH.write_text(json.dumps(self.paper_trades, indent=2))

    # ── Trade execution ────────────────────────────────────────────────────────

    def _paper_trade(self, signal: dict, market: dict, edge_result: dict):
        """Record a virtual paper trade."""
        if time.time() < self.paused_until:
            logger.warning("Engine paused (circuit breaker active)")
            return

        position_size = config.MAX_POSITION_USD
        if self.reduced_size:
            position_size *= 0.5

        fill_price = edge_result["market_price"] + 0.01  # 1¢ slippage

        trade = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "asset": signal["asset"],
            "market_id": market["market_id"],
            "market_title": market.get("title", ""),
            "direction": signal["direction"],
            "side": edge_result["side"],
            "fill_price": round(fill_price, 3),
            "position_size_usd": position_size,
            "edge_pct": edge_result["edge_pct"],
            "implied_prob": edge_result["implied_prob"],
            "confidence": signal["confidence"],
            "funding_rate": signal["rate"],
            "rate_zscore": signal["rate_zscore"],
            "status": "OPEN",
            "pnl": 0.0,
        }
        self.paper_trades.append(trade)
        self._save_paper_trades()

        logger.info(
            f"PAPER TRADE: {signal['asset']} {signal['direction']} | "
            f"{edge_result['side']} {market['market_id']} @ {fill_price:.3f} "
            f"size=${position_size} edge={edge_result['edge_pct']:.2%} "
            f"conf={signal['confidence']}"
        )

    def _live_trade(self, signal: dict, market: dict, edge_result: dict):
        """Log a live trade to the framework trade journal."""
        try:
            from framework.trade_journal import log_trade
            log_trade(
                strategy_name="funding_rate_arb",
                market_id=market["market_id"],
                platform="kalshi",
                side=edge_result["side"],
                price=edge_result["market_price"],
                size=config.MAX_POSITION_USD,
                edge_pct=edge_result["edge_pct"],
                signal_strength=signal["confidence"],
                market_conditions={
                    "asset": signal["asset"],
                    "funding_rate": signal["rate"],
                    "rate_zscore": signal["rate_zscore"],
                    "direction": signal["direction"],
                    "magnitude": signal["magnitude"],
                },
            )
        except Exception as e:
            logger.error(f"Failed to log live trade to journal: {e}")

    # ── Risk controls ──────────────────────────────────────────────────────────

    def _check_circuit_breaker(self):
        today = datetime.now(timezone.utc).date().isoformat()
        today_pnl = sum(
            t.get("pnl", 0) for t in self.paper_trades
            if t.get("timestamp", "")[:10] == today
        )
        if today_pnl < -10.0:
            self.paused_until = time.time() + 3600
            logger.warning(f"Circuit breaker triggered: daily PnL ${today_pnl:.2f} — pausing 1hr")

        recent = [t for t in self.paper_trades if t.get("status") != "OPEN"][-5:]
        if len(recent) == 5 and all(t.get("pnl", 0) < 0 for t in recent):
            self.reduced_size = True
            logger.warning("5 consecutive losses — reducing position size 50%")

    def _check_promotion(self):
        resolved = [t for t in self.paper_trades if t.get("status") != "OPEN"]
        if len(resolved) < config.PAPER_PROMOTION_MIN_TRADES:
            return False

        hours_in_paper = (time.time() - self.start_time) / 3600
        if hours_in_paper < config.PAPER_PROMOTION_MIN_HOURS:
            return False

        wins = sum(1 for t in resolved if t.get("pnl", 0) > 0)
        win_rate = wins / len(resolved) if resolved else 0
        if win_rate >= config.PAPER_PROMOTION_WIN_RATE:
            logger.info(
                f"PROMOTION ELIGIBLE: {win_rate:.1%} WR over {len(resolved)} trades, "
                f"{hours_in_paper:.1f} hours — set FUNDING_ARB_PROMOTE_TO_LIVE=true to go live"
            )
            return True
        return False

    def _log_performance(self):
        resolved = [t for t in self.paper_trades if t.get("status") != "OPEN"]
        total_pnl = sum(t.get("pnl", 0) for t in resolved)
        wins = sum(1 for t in resolved if t.get("pnl", 0) > 0)
        n = len(resolved)
        wr = wins / n if n > 0 else 0
        logger.info(
            f"PAPER PERF: {len(self.paper_trades)} total | {n} resolved | "
            f"WR={wr:.1%} | PnL=${total_pnl:.2f}"
        )

    # ── Main loop ──────────────────────────────────────────────────────────────

    def _run_cycle(self):
        """Single scan cycle: fetch rates → analyze → match → edge → trade."""
        all_rates = get_all_funding_rates()

        for asset, rate_data in all_rates.items():
            try:
                # Fetch history for z-score computation
                history = get_funding_history(asset, limit=30)
                # Prepend current rate to history
                if rate_data and (not history or history[0].get("rate") != rate_data["rate"]):
                    history.insert(0, rate_data)

                signal = analyze_funding(asset, history)
                if signal is None:
                    continue

                # Get current spot price for market matching
                current_price = _fetch_current_price(asset)

                # Match to open Kalshi markets
                markets = match_asset_to_markets(asset, signal["direction"], current_price)
                if not markets:
                    logger.info(f"No matching Kalshi markets for {asset} {signal['direction']}")
                    continue

                # Evaluate edge for each matched market
                for market in markets:
                    edge_result = calculate_edge(signal, market)
                    if edge_result is None:
                        continue

                    if self.mode == "PAPER":
                        self._paper_trade(signal, market, edge_result)
                    elif self.mode == "LIVE":
                        self._live_trade(signal, market, edge_result)

            except Exception as e:
                logger.error(f"Cycle error for {asset}: {e}")

    def run(self):
        """Main loop. Scans every SCAN_INTERVAL_SECS."""
        logger.info(f"Starting FundingArbEngine in {self.mode} mode")
        cycle = 0
        while True:
            try:
                cycle += 1
                self._run_cycle()

                # Every ~10 cycles: housekeeping
                if cycle % 10 == 0:
                    self._check_circuit_breaker()
                    self._log_performance()
                    if self.mode == "PAPER":
                        self._check_promotion()

            except Exception as e:
                logger.error(f"Top-level engine error (cycle {cycle}): {e}")

            time.sleep(config.SCAN_INTERVAL_SECS)


def main():
    engine = FundingArbEngine()
    engine.run()


if __name__ == "__main__":
    main()
