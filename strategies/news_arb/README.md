# News Arb Strategy

Exploits the lag between when news breaks and when prediction market odds react.

## Mode
- Starts in PAPER mode by default
- Set `NEWS_ARB_PROMOTE_TO_LIVE=true` in environment to enable live trading
- Requires 20+ paper trades with >55% win rate over 48+ hours before promotion

## Running
```bash
# Run historical simulation
python3 -m strategies.news_arb.sim.historical_sim

# Start paper trading
python3 -m strategies.news_arb.news_arb_engine
```

## Architecture
- news_fetcher.py — polls NewsAPI + RSS feeds
- entity_matcher.py — matches headlines to prediction markets
- edge_calculator.py — computes sentiment and edge
- signal_generator.py — orchestrates the pipeline
- news_arb_engine.py — main engine with paper/live modes
