# Strategy 6: Funding Rate Arb

Trades Kalshi crypto range markets using perpetual futures funding rate signals.

## Thesis
When funding rates on perpetual futures spike extreme (>0.1% / 8hr), longs are paying
heavily — this predicts mean-reversion selling pressure within 4 hours. We trade NO on
Kalshi "above $X" markets near the current price to capture that drift.

## Signal Logic
| Condition | Signal |
|-----------|--------|
| Rate > 0.1% AND z-score > 2.0 | BEARISH SPIKE |
| Rate < -0.1% AND z-score < -2.0 | BULLISH SPIKE |
| Rate > 0.05% for 3+ consecutive 8hr periods | BEARISH TREND |
| Rate < -0.05% for 3+ consecutive periods | BULLISH TREND |

## Files
| File | Role |
|------|------|
| `config.py` | Thresholds, assets, sources, mode |
| `funding_fetcher.py` | Fetch rates from Binance → Bybit → OKX |
| `rate_analyzer.py` | Z-score, consecutive-period, signal generation |
| `market_matcher.py` | Match signals to open Kalshi range markets |
| `edge_calculator.py` | Compute edge: implied_prob - market_price - fees |
| `funding_engine.py` | Main orchestrator loop |
| `sim/historical_sim.py` | Backtest on Bybit history + Coinbase price data |

## Running

```bash
# Backtest
python3 strategies/funding_rate_arb/sim/historical_sim.py

# Paper trading engine
python3 strategies/funding_rate_arb/funding_engine.py

# Promote to live (after meeting promotion criteria)
FUNDING_ARB_PROMOTE_TO_LIVE=true python3 strategies/funding_rate_arb/funding_engine.py
```

## Promotion Criteria
- Win rate > 55% over 20+ resolved trades
- 48 hours minimum paper trading time
- (Sim gate): win_rate > 52%, avg_edge > 5%, sharpe > 0.8, trades >= 20

## MODE
Default: `PAPER`. Override via `FUNDING_ARB_MODE=LIVE` env var.
