# EdgeLab

Prediction market arbitrage and edge-detection trading system targeting Polymarket and Kalshi.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Streamlit Dashboard                    │
│  Live Opps │ Calibration │ Portfolio │ Settings/Backtest │
└──────────────────────┬──────────────────────────────────┘
                       │ Redis pub/sub
┌──────────────────────┴──────────────────────────────────┐
│                   Strategy Engine                        │
│  Resolution Lag │ Threshold Scanner │ Arb Detector       │
│                      │ Kelly Sizer                       │
└──────────────────────┬──────────────────────────────────┘
                       │ Redis pub/sub
┌──────────────────────┴──────────────────────────────────┐
│                    Data Feeds                            │
│  Polymarket WS+REST │ Kalshi REST │ News/ESPN Events     │
└─────────────────────────────────────────────────────────┘
                       │
              ┌────────┴────────┐
              │  Redis  │ SQLite │
              └─────────────────┘
```

## Quickstart

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start Redis (or use Docker)
docker run -d -p 6379:6379 redis:7-alpine

# 3. Run the system
python main.py &
streamlit run dashboard/app.py
```

Or with Docker Compose:

```bash
cp .env.example .env   # add your API keys
docker-compose up
```

Dashboard runs at http://localhost:8501

## Strategies

### Resolution Lag Arbitrage
Detects when a real-world event has resolved (news headline, game final score) but the prediction market hasn't settled yet. Buys near-certain outcomes before the market catches up.

- **Edge**: 3-15% depending on lag time
- **Risk**: Event misidentification, market already pricing in
- **Data**: NewsAPI headlines + ESPN scores matched against open Polymarket markets

### Near-Certainty Threshold
Scans for markets trading above a configurable threshold (default 90%) where historical calibration shows the true probability is even higher. Exploits the gap between market price and actual resolution rate.

- **Edge**: 1-5% per trade, high win rate
- **Risk**: Rare tail events (the 5% that don't resolve as expected)
- **Annualized**: 50-300%+ on short-duration markets

### Cross-Platform Arbitrage
Finds the same event priced differently on Polymarket vs Kalshi. Locks in risk-free spread by buying YES on the cheap platform and NO on the expensive one.

- **Edge**: 1.5-5% locked return after fees
- **Risk**: Execution risk, settlement differences between platforms

## API Keys

| Key | Required | Purpose |
|-----|----------|---------|
| `NEWSAPI_KEY` | Optional | Real-time news headlines for resolution lag detection |
| `KALSHI_API_KEY` | Optional | Live Kalshi market data for arb detection |

Without API keys, the system uses realistic mock data for those feeds. Polymarket and ESPN feeds work without keys.

## Configuration

Set in `.env` or dashboard Settings page:

| Setting | Default | Description |
|---------|---------|-------------|
| `THRESHOLD_PCT` | 90 | Minimum probability for threshold scanner |
| `CAPITAL` | 10000 | Paper trading capital |
| `MAX_POSITION_PCT` | 5 | Max % of capital per position |
| `MAX_CORRELATED_PCT` | 30 | Max % of capital in correlated positions |

## Testing

```bash
pytest tests/ -v
```

## Project Structure

```
edgelab/
├── main.py                  # Async orchestrator
├── dashboard/app.py         # Streamlit dashboard
├── src/
│   ├── feeds/
│   │   ├── polymarket.py    # Polymarket WS + REST feed
│   │   ├── kalshi.py        # Kalshi REST feed (mock if no key)
│   │   └── events.py        # News + ESPN event monitor
│   ├── strategies/
│   │   ├── resolution_lag.py
│   │   ├── threshold_scanner.py
│   │   └── arb_detector.py
│   ├── risk/
│   │   └── sizer.py         # Kelly criterion position sizing
│   └── calibration/
│       ├── tracker.py       # Paper/live trade tracking
│       └── backtester.py    # Historical calibration analysis
├── data/                    # SQLite DB + calibration JSON
├── logs/                    # Structured logs
├── tests/
│   └── test_strategies.py
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```
