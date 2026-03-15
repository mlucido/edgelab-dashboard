#!/bin/bash
# EdgeLab overnight health check
# Run: while true; do bash ~/Dropbox/EdgeLab/scripts/overnight_check.sh; sleep 1800; done
cd ~/Dropbox/EdgeLab

echo "=== EdgeLab Check $(date) ==="

# Bot alive?
pgrep -f "live_main.py" > /dev/null 2>&1 && echo "Bot: ALIVE" || echo "Bot: DEAD — ALERT"

# Bot daily P&L
python3 -c "
import sqlite3
from datetime import datetime, timezone
conn = sqlite3.connect('strategies/bot/sim_results.db')
conn.row_factory = sqlite3.Row
bk = float(conn.execute('SELECT value FROM bot_state WHERE key=\"bankroll\"').fetchone()[0])
halted = conn.execute('SELECT value FROM bot_state WHERE key=\"halted\"').fetchone()[0]
today_start = datetime(*datetime.now(timezone.utc).timetuple()[:3], tzinfo=timezone.utc).timestamp()
row = conn.execute(
    'SELECT COALESCE(SUM(net_pnl),0) as pnl FROM sim_trades WHERE outcome != \"PENDING\" AND exit_ts >= ? AND bankroll_at_entry >= 500',
    (today_start,)).fetchone()
daily_pnl = float(row['pnl'])
print(f'  Bankroll: \${bk:.2f} | Today PnL: \${daily_pnl:.2f} | Halted: {halted}')
if daily_pnl < -25:
    print('  ALERT: Daily loss exceeds \$25')
conn.close()
" 2>/dev/null

# Strategy processes
for s in resolution_lag sports_momentum funding_rate_arb liquidity_provision news_arb; do
    pgrep -f "$s" > /dev/null 2>&1 && echo "$s: ALIVE" || echo "$s: DEAD"
done

# EdgeLab (edge tmux)
tmux capture-pane -t edge -p 2>/dev/null | grep -q "polymarket\|kalshi\|ESPN\|feeds" && echo "EdgeLab: ALIVE" || echo "EdgeLab: CHECK — may need restart"

echo "---"
