# Price Feed Aggregator

Multi-venue price and funding rate aggregator for EdgeLab. Writes cross-venue signals to Redis for consumption by trading strategies.

## Signal Keys

| Key | Meaning | TTL | Example Value |
|-----|---------|-----|---------------|
| `edgelab:signals:divergence:{asset}` | Cross-venue price divergence score | 120s | `{"score": 0.0023, "ts": 1710500000, "prices": {"kraken": 67150.0, "exmo": 66995.0}}` |
| `edgelab:signals:eur_confirm:{asset}` | EUR/USD momentum confirmation | 120s | `{"confirmed": true, "eur_momentum": 0.0012, "usd_momentum": 0.0015, "ts": 1710500000}` |
| `edgelab:signals:funding:{asset}` | Aggregated perpetual funding rates | 600s | `{"okx": -0.0002, "bybit": -0.00015, "avg": -0.000175, "sentiment": "bullish", "next_okx": -0.0001, "ts": 1710500000}` |
| `edgelab:prices:kraken:{asset}` | Latest Kraken USD price | 60s | `"67150.50"` |
| `edgelab:prices:exmo:{asset}` | Latest EXMO USD price | 60s | `"66995.00"` |

Assets: BTC, ETH, SOL

## Quick Start

Start as a background tmux session:

```bash
tmux new-session -d -s feeds 'cd ~/Dropbox/EdgeLab && python framework/price_feeds/run.py'
```

## Verify It's Working

```bash
# Check signal keys exist
redis-cli keys "edgelab:signals:*"

# Read BTC divergence
redis-cli get "edgelab:signals:divergence:BTC"

# Read funding sentiment
redis-cli get "edgelab:signals:funding:ETH"
```

## Adding a New Feed

1. **Subclass `PriceFeed`** in a new file (e.g., `binance_feed.py`):
   - Implement `connect()`, `disconnect()`, `get_price(symbol)`, and `name` property
   - Cache prices internally and update `self._last_success` on each successful fetch

2. **Add to `self.feeds`** in `aggregator.py` `__init__`:
   ```python
   self.feeds = [KrakenFeed(), ExmoFeed(), BinanceFeed()]
   ```

3. **Done** — the aggregator picks it up automatically in `_update_loop`. A new `edgelab:prices:{venue}:{asset}` key will appear, and the venue's prices will factor into divergence scores.

## Dependencies

See `requirements.txt`. Install with:

```bash
pip install -r framework/price_feeds/requirements.txt
```
