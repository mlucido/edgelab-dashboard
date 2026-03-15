# Sports Momentum Strategy

Detects and trades live in-game momentum shifts in NBA, NFL, MLB, and NCAAB on Kalshi prediction markets.

## How It Works

1. **ESPN Feed** — polls public ESPN scoreboard endpoints every 15 seconds for live game state
2. **Momentum Scorer** — computes sport-specific momentum scores from scoring runs, turnovers, lead changes, and foul trouble
3. **Market Matcher** — fuzzy-matches live games to open Kalshi markets (game winner, first-half, totals)
4. **Edge Calculator** — computes net edge after fees, applies Kelly position sizing
5. **Engine** — orchestrates the pipeline with circuit breakers, paper trade persistence, and promotion tracking

## Modes

- `PAPER` (default) — logs all signals and simulated trades, no real money
- `LIVE` — set `SPORTS_MOMENTUM_MODE=LIVE` after meeting promotion criteria

## Promotion Criteria

- Win rate > 55%
- Min 20 resolved trades
- Min 48 hours in paper mode

## Running

```bash
# Simulation (backtest last 14 days)
cd ~/Dropbox/EdgeLab
python3 -m strategies.sports_momentum.sim.historical_sim

# Live paper trading (background)
nohup python3 -m strategies.sports_momentum.sports_engine > /dev/null 2>&1 &

# Check logs
tail -f logs/sports_momentum.log
```

## Configuration

All settings in `config.py`. Override via environment variables:
- `SPORTS_MOMENTUM_MODE` — PAPER or LIVE
- `KALSHI_API_KEY` / `KALSHI_KEY_ID` — from .env
