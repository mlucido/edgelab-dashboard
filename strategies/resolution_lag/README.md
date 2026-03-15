# Resolution Lag Strategy

Exploits the 5–30 minute window after real-world events resolve (game ends, election called, economic data released) during which prediction market prices haven't yet adjusted to the known outcome.

**Mode:** PAPER (override via env `RESOLUTION_LAG_MODE=LIVE`)

## Detection Methods
1. Kalshi finalized markets API
2. ESPN live scoreboard final scores
3. BLS/FRED economic data releases
4. NewsAPI resolution language detection

## Thresholds
- Min certainty score: 0.85
- Min edge after fees: 8%
- Max position: $75
- Lag window: 1–30 min post-resolution

## Files
- `config.py` — all constants and API key loading
- `resolution_detector.py` — four detection methods
- `certainty_scorer.py` — multi-factor certainty scoring
- `lag_scanner.py` — finds open markets with lagging prices
- `resolution_engine.py` — main orchestrator loop
- `sim/historical_sim.py` — 90-day backtest

## Running
```bash
# Simulation
cd ~/Dropbox/EdgeLab
python3 -c "from strategies.resolution_lag.sim.historical_sim import run_simulation; run_simulation()"

# Live engine (paper mode)
nohup python3 -m strategies.resolution_lag.resolution_engine >> logs/resolution_lag.log 2>&1 &
```
