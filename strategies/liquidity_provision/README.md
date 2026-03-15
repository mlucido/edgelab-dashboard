# Strategy 7 — Liquidity Provision

Earns spread by posting two-sided limit orders on Kalshi prediction markets.

## How It Works

1. **Spread Scanner** scans open Kalshi markets for bid-ask spreads ≥ 5 cents
2. **Risk Guard** filters out volatile, near-resolution, or cross-strategy-conflicted markets
3. **Quote Engine** posts limit orders 2 cents inside the spread on both sides
4. If both sides fill: captures ~4 cents per dollar of volume
5. **Inventory Manager** tracks open exposure; halts new quotes at $30 one-sided limit

## Files

| File | Role |
|------|------|
| `config.py` | All tunable parameters |
| `spread_scanner.py` | Kalshi API scan → SpreadOpportunity list |
| `inventory_manager.py` | Thread-safe position tracking |
| `quote_engine.py` | Quote generation + PAPER fill simulation |
| `risk_guard.py` | Pre-quote risk checks (inventory, volatility, cross-strategy) |
| `liquidity_engine.py` | Main orchestrator loop |
| `sim/historical_sim.py` | 90-day backtest on finalized markets |

## Running

```bash
# Backtest
python3 strategies/liquidity_provision/sim/historical_sim.py

# Paper trading loop
python3 strategies/liquidity_provision/liquidity_engine.py

# Promote to live (requires passing PAPER_PROMOTION criteria)
LP_PROMOTE_TO_LIVE=true python3 strategies/liquidity_provision/liquidity_engine.py
```

## Promotion Criteria

| Metric | Gate |
|--------|------|
| Spread capture rate | > 60% |
| Sharpe estimate | > 0.7 |
| Min trades | ≥ 30 |
| Min paper hours | ≥ 48 |

## Circuit Breaker

Session inventory loss > -$5 → automatic 2-hour pause.
