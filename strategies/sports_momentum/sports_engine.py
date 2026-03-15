"""
Sports Momentum Engine — main orchestrator.
Runs in PAPER mode by default.
"""
import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# Add EdgeLab root to path for framework imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from . import config
from .espn_feed import get_all_live_scores
from .momentum_scorer import compute_momentum
from .market_matcher import match_game_to_markets
from .edge_calculator import compute_edge

logger = logging.getLogger("sports_momentum.engine")
log_dir = Path(__file__).parent.parent.parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
_file_handler = logging.FileHandler(log_dir / "sports_momentum.log")
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
logger.addHandler(_file_handler)
logger.setLevel(logging.INFO)

_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
logger.addHandler(_console)

# Also configure sub-module loggers to write to the same file
for submod in ("sports_momentum.espn_feed", "sports_momentum.momentum_scorer",
               "sports_momentum.market_matcher", "sports_momentum.edge_calculator"):
    sub_logger = logging.getLogger(submod)
    sub_logger.addHandler(_file_handler)
    sub_logger.setLevel(logging.INFO)

PAPER_TRADES_PATH = Path(__file__).parent / "sim" / "results" / "paper_trades.json"


class SportsEngine:
    def __init__(self):
        self.mode = config.MODE
        self.paper_trades: list[dict] = []
        self.paper_capital = config.STARTING_CAPITAL_USD
        self.reduced_size = False
        self.paused_until: float = 0.0
        self.start_time = time.time()
        # game_id -> list of snapshots (for momentum history)
        self.game_history: dict[str, list[dict]] = defaultdict(list)
        self._load_paper_trades()
        logger.info(f"SportsEngine initialized in {self.mode} mode")

    def _load_paper_trades(self):
        PAPER_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
        if PAPER_TRADES_PATH.exists():
            try:
                self.paper_trades = json.loads(PAPER_TRADES_PATH.read_text())
                logger.info(f"Loaded {len(self.paper_trades)} existing paper trades")
            except Exception:
                self.paper_trades = []

    def _save_paper_trades(self):
        try:
            PAPER_TRADES_PATH.write_text(json.dumps(self.paper_trades, indent=2))
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")

    def _paper_trade(self, edge_signal) -> None:
        """Execute a paper trade and log to framework journal."""
        if time.time() < self.paused_until:
            logger.warning("Engine paused — circuit breaker active, skipping trade")
            return

        position_size = edge_signal.position_size_usd
        if self.reduced_size:
            position_size *= 0.5

        trade = {
            "timestamp": edge_signal.timestamp,
            "game_id": edge_signal.game_id,
            "sport": edge_signal.sport,
            "ticker": edge_signal.ticker,
            "market_type": edge_signal.market_type,
            "side": edge_signal.side,
            "market_price": edge_signal.market_price,
            "fair_value": edge_signal.fair_value,
            "fill_price": round(edge_signal.market_price + 0.01, 3),  # +1¢ slippage
            "position_size_usd": position_size,
            "edge_pct": edge_signal.edge_pct,
            "momentum_score": edge_signal.momentum_score,
            "confidence": edge_signal.confidence,
            "factors": edge_signal.factors,
            "status": "OPEN",
            "pnl": 0.0,
        }
        self.paper_trades.append(trade)
        self._save_paper_trades()

        logger.info(
            f"PAPER TRADE: {edge_signal.side} {edge_signal.ticker} "
            f"@ {trade['fill_price']:.3f} size=${position_size:.2f} "
            f"edge={edge_signal.edge_pct:.2%} conf={edge_signal.confidence} "
            f"factors={edge_signal.factors}"
        )

        # Log to framework trade journal
        try:
            from framework.trade_journal import log_trade
            log_trade(
                strategy_name="sports_momentum",
                market_id=edge_signal.ticker,
                platform="kalshi",
                side=edge_signal.side,
                price=edge_signal.market_price,
                size=position_size,
                edge_pct=edge_signal.edge_pct,
                signal_strength=edge_signal.confidence,
                market_conditions={
                    "sport": edge_signal.sport,
                    "game_id": edge_signal.game_id,
                    "market_type": edge_signal.market_type,
                    "momentum_score": edge_signal.momentum_score,
                    "factors": edge_signal.factors,
                },
                notes=f"momentum factors: {', '.join(edge_signal.factors)}",
            )
        except Exception as e:
            logger.error(f"Failed to log trade to framework journal: {e}")

    def _check_circuit_breaker(self):
        """Pause 1hr if daily paper losses exceed $10."""
        today = datetime.now(timezone.utc).date().isoformat()
        today_pnl = sum(
            t.get("pnl", 0) for t in self.paper_trades
            if t.get("timestamp", "")[:10] == today and t.get("status") != "OPEN"
        )
        if today_pnl < -10.0:
            self.paused_until = time.time() + 3600
            logger.warning(
                f"Circuit breaker: daily PnL ${today_pnl:.2f} < -$10. "
                f"Pausing 1hr until {datetime.fromtimestamp(self.paused_until, tz=timezone.utc).isoformat()}"
            )

        # 5 consecutive losses -> reduce size
        resolved = [t for t in self.paper_trades if t.get("status") != "OPEN"]
        recent_5 = resolved[-5:]
        if len(recent_5) == 5 and all(t.get("pnl", 0) < 0 for t in recent_5):
            if not self.reduced_size:
                self.reduced_size = True
                logger.warning("5 consecutive losses — reducing position size by 50%")

    def _check_promotion(self):
        """Check if paper trading meets live promotion criteria."""
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
                f"PROMOTION ELIGIBLE: WR={win_rate:.1%} over {len(resolved)} trades, "
                f"{hours_in_paper:.1f} hours in paper mode"
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
            f"PERF SUMMARY: {len(self.paper_trades)} total trades, {n} resolved, "
            f"WR={wr:.1%}, PnL=${total_pnl:.2f}, reduced_size={self.reduced_size}"
        )

    def _update_game_history(self, game: dict):
        """Append current snapshot to game history (capped at 20 snapshots per game)."""
        gid = game.get("game_id", "")
        if not gid:
            return
        history = self.game_history[gid]
        history.append(game)
        if len(history) > 20:
            self.game_history[gid] = history[-20:]

    def run_cycle(self) -> int:
        """
        Single pipeline cycle.
        Returns count of signals processed.
        """
        signals_processed = 0

        try:
            all_scores = get_all_live_scores()
            total_games = sum(len(v) for v in all_scores.values())
            if total_games > 0:
                logger.info(f"Live games: {total_games} across {len(all_scores)} sports")

            for sport, games in all_scores.items():
                for game in games:
                    self._update_game_history(game)
                    history = self.game_history.get(game["game_id"], [])

                    # Need at least 2 snapshots for momentum
                    if len(history) < 2:
                        continue

                    momentum = compute_momentum(game, sport, history)
                    if not momentum:
                        continue

                    logger.info(
                        f"MOMENTUM: {sport} {game.get('home_team')} vs {game.get('away_team')} "
                        f"dir={momentum.direction} score={momentum.score:.3f} "
                        f"conf={momentum.confidence} factors={momentum.factors}"
                    )

                    matched_markets = match_game_to_markets(game)
                    if not matched_markets:
                        logger.debug(f"No Kalshi markets matched for game {game['game_id']}")
                        continue

                    for market in matched_markets:
                        edge = compute_edge(momentum, market)
                        if not edge:
                            continue

                        if self.mode == "PAPER":
                            self._paper_trade(edge)
                        elif self.mode == "LIVE":
                            # Live trading: log to journal (actual order placement not implemented)
                            try:
                                from framework.trade_journal import log_trade
                                log_trade(
                                    strategy_name="sports_momentum",
                                    market_id=edge.ticker,
                                    platform="kalshi",
                                    side=edge.side,
                                    price=edge.market_price,
                                    size=edge.position_size_usd,
                                    edge_pct=edge.edge_pct,
                                    signal_strength=edge.confidence,
                                    market_conditions={
                                        "sport": edge.sport,
                                        "game_id": edge.game_id,
                                    },
                                )
                            except Exception as e:
                                logger.error(f"Live journal log error: {e}")

                        signals_processed += 1

        except Exception as e:
            logger.error(f"Cycle error: {e}")

        return signals_processed

    def run(self):
        """Main loop."""
        logger.info(f"Starting SportsEngine in {self.mode} mode, poll_interval={config.POLL_INTERVAL_SECS}s")
        cycle = 0

        while True:
            try:
                cycle += 1
                signals = self.run_cycle()

                if cycle % 10 == 0:
                    self._check_circuit_breaker()
                    self._log_performance()
                    if self.mode == "PAPER":
                        self._check_promotion()

            except Exception as e:
                logger.error(f"Main loop error: {e}")

            time.sleep(config.POLL_INTERVAL_SECS)


def main():
    engine = SportsEngine()
    engine.run()


if __name__ == "__main__":
    main()
