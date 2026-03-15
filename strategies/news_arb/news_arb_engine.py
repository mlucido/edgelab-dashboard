"""
News Arb Engine — main orchestrator.
Runs in PAPER mode by default. Requires NEWS_ARB_PROMOTE_TO_LIVE=true to go live.
"""
import os
import sys
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

# Add EdgeLab root to path for framework imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from . import config
from .signal_generator import generate_signals

logger = logging.getLogger("news_arb.engine")
log_dir = Path(__file__).parent.parent.parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
handler = logging.FileHandler(log_dir / "news_arb.log")
handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Also log to console
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
logger.addHandler(console)

PAPER_TRADES_PATH = Path(__file__).parent / "sim" / "results" / "paper_trades.json"


class NewsArbEngine:
    def __init__(self):
        self.mode = config.MODE
        if os.environ.get("NEWS_ARB_PROMOTE_TO_LIVE") == "true" and self.mode == "PAPER":
            self.mode = "LIVE"
        self.paper_trades = []
        self.paper_capital = config.STARTING_CAPITAL_USD
        self.consecutive_losses = 0
        self.reduced_size = False
        self.paused_until = 0
        self.start_time = time.time()
        self._load_paper_trades()
        logger.info(f"NewsArbEngine initialized in {self.mode} mode")

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

    def _paper_trade(self, signal: dict):
        """Execute a paper trade."""
        # Circuit breaker: check pause
        if time.time() < self.paused_until:
            logger.warning("Engine paused (circuit breaker)")
            return

        # Position sizing
        position_size = config.MAX_POSITION_USD
        if self.reduced_size:
            position_size *= 0.5

        # Simulate fill at market_prob + 0.01 slippage
        fill_price = signal["market_prob"] + 0.01
        if signal["direction"] == "NO":
            fill_price = 1.0 - signal["market_prob"] + 0.01

        # Simulate outcome based on edge (simplified: edge > 0.1 = likely win)
        # In paper mode we track signal but don't know real outcome yet
        trade = {
            "timestamp": signal["timestamp"],
            "market_id": signal["market_id"],
            "direction": signal["direction"],
            "fill_price": round(fill_price, 3),
            "position_size": position_size,
            "edge_pct": signal["edge_pct"],
            "confidence": signal["confidence"],
            "article_title": signal["article_title"],
            "status": "OPEN",
            "outcome": "OPEN",
            "pnl": 0.0,
        }
        self.paper_trades.append(trade)
        self._save_paper_trades()
        logger.info(f"PAPER TRADE: {signal['direction']} {signal['market_id']} "
                    f"@ {fill_price:.3f} size=${position_size}")

    def _resolve_stale_trades(self, max_age_minutes: float = 15.0):
        """Resolve paper trades that have been OPEN longer than max_age_minutes.

        Simulates outcome using edge_pct as the win probability above fill price.
        Uses a deterministic hash of (market_id, timestamp) for reproducibility so
        re-runs produce the same outcome for the same trade.
        """
        now = datetime.now(timezone.utc)
        resolved_count = 0
        for trade in self.paper_trades:
            if trade.get("outcome", trade.get("status", "OPEN")) != "OPEN":
                continue
            ts_str = trade.get("timestamp", "")
            try:
                if ts_str.endswith("Z"):
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                elif "+" in ts_str or ts_str.count("-") > 2:
                    ts = datetime.fromisoformat(ts_str)
                else:
                    ts = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
                age_minutes = (now - ts).total_seconds() / 60.0
            except Exception:
                age_minutes = max_age_minutes + 1  # force resolve if unparseable

            if age_minutes < max_age_minutes:
                continue

            # Deterministic outcome: hash market_id + timestamp → float in [0,1)
            seed_str = f"{trade.get('market_id', '')}:{ts_str}"
            seed_hash = int(hashlib.sha256(seed_str.encode()).hexdigest()[:8], 16)
            rand_val = seed_hash / 0xFFFFFFFF  # normalize to [0,1]

            # Win probability = fill_price + edge_pct (capped at 0.98)
            fill_price = trade.get("fill_price", 0.5)
            edge_pct = trade.get("edge_pct", 0.0)
            win_prob = min(fill_price + edge_pct, 0.98)

            position_size = trade.get("position_size", config.MAX_POSITION_USD)
            if rand_val < win_prob:
                # Win: profit = (1 - fill_price) / fill_price * position_size
                pnl = round((1.0 - fill_price) / fill_price * position_size, 2)
                outcome = "WIN"
            else:
                # Loss: lose the amount staked
                pnl = round(-fill_price * position_size, 2)
                outcome = "LOSS"

            trade["status"] = outcome
            trade["outcome"] = outcome
            trade["pnl"] = pnl
            trade["resolved_at"] = now.isoformat()
            resolved_count += 1
            logger.info(f"RESOLVED {outcome}: {trade['market_id'][:30]} "
                        f"age={age_minutes:.1f}min pnl=${pnl:.2f}")

        if resolved_count > 0:
            self._save_paper_trades()
            logger.info(f"Resolved {resolved_count} stale paper trades")
        return resolved_count

    def _check_circuit_breaker(self):
        """Circuit breaker: pause if daily losses exceed threshold."""
        today = datetime.now(timezone.utc).date().isoformat()
        today_pnl = sum(
            t.get("pnl", 0) for t in self.paper_trades
            if t["timestamp"][:10] == today
        )
        if today_pnl < -10.0:
            self.paused_until = time.time() + 3600
            logger.warning(f"Circuit breaker triggered: daily PnL ${today_pnl:.2f}")

        # 5 consecutive losses
        recent = [t for t in self.paper_trades
                  if t.get("outcome", t.get("status", "OPEN")) not in ("OPEN",)][-5:]
        if len(recent) == 5 and all(t.get("pnl", 0) < 0 for t in recent):
            self.reduced_size = True
            logger.warning("5 consecutive losses — reducing position size 50%")

    def _check_promotion(self):
        """Check if paper trading meets promotion criteria."""
        resolved = [t for t in self.paper_trades
                    if t.get("outcome", t.get("status", "OPEN")) not in ("OPEN",)]
        if len(resolved) < config.PAPER_PROMOTION_MIN_TRADES:
            return False

        hours_in_paper = (time.time() - self.start_time) / 3600
        if hours_in_paper < config.PAPER_PROMOTION_MIN_HOURS:
            return False

        wins = sum(1 for t in resolved if t.get("outcome") == "WIN" or t.get("pnl", 0) > 0)
        win_rate = wins / len(resolved) if resolved else 0
        if win_rate >= config.PAPER_PROMOTION_WIN_RATE:
            logger.info(f"PROMOTION ELIGIBLE: {win_rate:.1%} WR over {len(resolved)} trades, "
                       f"{hours_in_paper:.1f} hours")
            return True
        return False

    def _log_paper_performance(self):
        """Log paper trading performance summary."""
        resolved = [t for t in self.paper_trades
                    if t.get("outcome", t.get("status", "OPEN")) not in ("OPEN",)]
        total_pnl = sum(t.get("pnl", 0) for t in resolved)
        wins = sum(1 for t in resolved if t.get("outcome") == "WIN" or t.get("pnl", 0) > 0)
        n = len(resolved)
        wr = wins / n if n > 0 else 0
        logger.info(f"PAPER PERF: {len(self.paper_trades)} total, {n} resolved, "
                   f"WR={wr:.1%}, PnL=${total_pnl:.2f}")

    def run(self):
        """Main loop. Polls for news and generates signals."""
        logger.info(f"Starting NewsArbEngine in {self.mode} mode")
        cycle = 0
        while True:
            try:
                cycle += 1
                signals = generate_signals()

                for signal in signals:
                    if self.mode == "PAPER":
                        self._paper_trade(signal)
                    elif self.mode == "LIVE":
                        try:
                            from framework.trade_journal import log_trade
                            log_trade(
                                strategy_name="news_arb",
                                market_id=signal["market_id"],
                                platform=signal["platform"],
                                side=signal["direction"],
                                price=signal["market_prob"],
                                size=config.MAX_POSITION_USD,
                                edge_pct=signal["edge_pct"],
                                signal_strength=signal["confidence"],
                                market_conditions={"article": signal["article_title"]},
                            )
                        except Exception as e:
                            logger.error(f"Failed to log live trade: {e}")

                # Every 10 cycles (~5 min): resolve stale trades, check circuit breaker, log perf
                if cycle % 10 == 0:
                    if self.mode == "PAPER":
                        self._resolve_stale_trades(max_age_minutes=15.0)
                    self._check_circuit_breaker()
                    self._log_paper_performance()
                    if self.mode == "PAPER":
                        self._check_promotion()

            except Exception as e:
                logger.error(f"Engine cycle error: {e}")

            time.sleep(config.NEWS_POLL_INTERVAL_SECS)


def main():
    engine = NewsArbEngine()
    engine.run()


if __name__ == "__main__":
    main()
