# EdgeLab Command Center

Read-only unified dashboard for all EdgeLab trading strategies.

## Quick Start

```bash
cd ~/Dropbox\ \(Personal\)/EdgeLab/command_center
python3 dashboard.py
```

Open http://localhost:8050

## Requirements

- Python 3.10+
- `aiohttp` (already installed via polymarket-sim/requirements.txt)
- No additional dependencies

## Data Sources

| Source | Type | Path | Status |
|--------|------|------|--------|
| Bot trades/positions | SQLite | `../polymarket-sim/sim_results.db` | Active |
| Bot log (live) | Log file | `../polymarket-sim/live.log` | Active |
| Bot log (sim) | Log file | `../polymarket-sim/sim.log` | Active |
| Bot log (paper) | Log file | `../polymarket-sim/paper_sim.log` | Active |
| EdgeLab signals | — | `../edge/edge.log` | Not yet deployed |
| EdgeLab DB | — | `../edge/edge_results.db` | Not yet deployed |

## What It Shows

- **Bot Section**: Bankroll, P&L, ROI, win rate, open positions, recent trades, recent signals
- **EdgeLab Section**: Placeholder (shows "not deployed" until EdgeLab strategy exists)
- **Shared Risk Envelope**: Total capital and deployed capital across both strategies
- **Unified Alert Feed**: Last 20 alerts from all log files, labeled [BOT] vs [EDGE]

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `DASHBOARD_PORT` | `8050` | HTTP server port |

## Architecture

- Single Python file (`dashboard.py`) using `aiohttp` to serve a self-contained HTML page
- All data reads are read-only (SQLite opened in `?mode=ro`, log files read with `open()`)
- Dashboard auto-refreshes every 30 seconds via `fetch('/api/data')`
- If any data source is unavailable, shows "No data" — never crashes

## Security

- The dashboard is **read-only** — it never writes to any database, log, or config file
- SQLite is opened in read-only mode (`?mode=ro`)
- No secrets are read or displayed — .env files are never accessed
- Listens on `0.0.0.0:8050` by default — bind to `127.0.0.1` if you want local-only access
