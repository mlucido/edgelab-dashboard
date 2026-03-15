# Polymarket Lag Arb Simulator
### $500 → $100,000 — Phase 0: Paper Trading

A live-data simulation of the crypto price lag arbitrage strategy
described in the Master Plan v2.0. Connects to **real** Binance price
feeds and **real** Polymarket order books — no accounts, no money.

---

## What it does

1. **Streams live BTC prices** from Binance WebSocket (public, no key needed)
2. **Scans Polymarket** for active short-duration crypto directional markets
3. **Detects momentum signals** — moves ≥ 0.40% in 45 seconds with volume confirmation
4. **Checks 6 risk gates** before every simulated trade
5. **Simulates fills and resolution** — logs every outcome to SQLite
6. **Shows a live terminal dashboard** with win rate, edge, P&L, bankroll

The sim is **ready to go live** when it hits: ✅ Win rate ≥ 68% | ✅ Avg edge ≥ 8% | ✅ 200+ closed trades

---

## Setup (5 minutes)

### Requirements
- Python 3.10+
- pip

### Install

```bash
# Clone / unzip the project
cd polymarket-sim

# Install dependencies (only 2 packages)
pip install -r requirements.txt
```

That's it. No API keys. No wallets. No accounts needed for the sim.

---

## Running

### Start the simulation

```bash
python main.py
```

The sim will:
1. Initialize the database (`sim_results.db`)
2. Connect to Binance WebSocket for live BTC prices
3. Wait ~60 seconds for price history to warm up
4. Start scanning Polymarket for tradeable markets
5. Evaluate signals every 5 seconds
6. Display a live dashboard (refreshes every 5 seconds)

**Leave it running.** Target is 200+ closed trades. At 8–15 trades/day on BTC-only
(paranoia mode), expect ~14–25 days to hit the validation threshold.

### Stop the simulation

```
Ctrl+C
```

The sim will print a final performance report before exiting.

### Check stats at any time (without stopping)

```bash
python main.py --report
```

### Reset and start fresh

```bash
python main.py --reset
python main.py
```

---

## Files

```
polymarket-sim/
├── main.py              ← Entry point. Run this.
├── config.py            ← All tunable parameters (thresholds, etc.)
├── database.py          ← SQLite schema + all read/write helpers
├── data_ingestor.py     ← Binance WebSocket price feeds
├── polymarket_client.py ← Polymarket Gamma + CLOB API (read-only)
├── signal_engine.py     ← Momentum scoring, edge calculation, Kelly sizing
├── risk_manager.py      ← 6-gate trade approval pipeline
├── sim_engine.py        ← Simulation execution loop
├── dashboard.py         ← Terminal dashboard
├── requirements.txt     ← websockets + aiohttp only
├── sim_results.db       ← Created on first run (SQLite)
└── sim.log              ← Detailed trade log
```

---

## Tuning the sim

All parameters live in `config.py`. Key ones to adjust:

| Parameter | Default | What it does |
|---|---|---|
| `MOMENTUM_MIN_PCT` | 0.40 | Min % move to generate a signal. Lower = more trades, lower win rate. |
| `MIN_EDGE_PCT` | 0.10 | Min net edge to trade. Higher = fewer but better trades. |
| `MAX_OPEN_POSITIONS` | 5 | Paranoia mode cap. Raise to 10 after $2,500 milestone. |
| `KELLY_FRACTION` | 0.25 | 25% Kelly. More aggressive = faster growth, more variance. |
| `ASSETS` | ["BTCUSDT"] | Add "ETHUSDT", "SOLUSDT" after BTC signals are validated. |

---

## Validation Targets (before going live)

Run the sim until all three pass:

- **Win rate ≥ 68%** across 200+ closed trades
- **Average edge ≥ 8%** per trade (net of simulated 2% Polymarket fee)
- **200+ closed trades** (statistical significance)

Run `python main.py --report` to check progress at any time.

---

## Going live (after sim passes)

1. Set up Polygon wallet + Polymarket account (see Master Plan v2.0, Section 9)
2. Fund wallet with $500 USDC
3. In `config.py`, the live bot (future phase) will add wallet credentials
4. Start with `MAX_POSITION_PCT = 0.01` (1% = $5/trade) for Phase 1 live calibration
5. Scale to full Kelly after 100 live trades match the sim results

---

## Disclaimer

This simulator uses real market data but places no real orders.
All P&L figures are simulated and do not represent actual trading results.
Prediction market trading involves substantial financial risk.
Never deploy capital you cannot afford to lose.
