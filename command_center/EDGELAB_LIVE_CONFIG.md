# EdgeLab Live Config — $100 Test on Kalshi

**Prepared:** 2026-03-14
**Status:** READY — do NOT apply until manual review

## Environment Variables (strategies/edgelab/.env)

```env
TRADING_MODE=LIVE
TOTAL_CAPITAL=100
KALSHI_MAX_TRADE_CENTS=1000
KALSHI_ENABLED=true
KALSHI_API_KEY=<already set>
```

## Safety Parameters (hardcoded for test)

| Parameter | Value | File |
|-----------|-------|------|
| Daily loss limit | -$20 | src/execution/circuit_breaker.py:44 |
| Max concurrent positions | 3 | src/execution/autonomous_trader.py:82 |
| Max single trade | $100 | src/execution/circuit_breaker.py:51 |
| Max trade size (sizer) | $50 | src/execution/autonomous_trader.py:73 |
| Max deployed fraction | 20% | src/execution/autonomous_trader.py:78 |
| Max hourly trades | 10 | src/execution/circuit_breaker.py:57 |

## Commands to Start Live

```bash
# 1. Stop current EdgeLab process
kill <PID>

# 2. Update .env for live test
cd ~/Dropbox\ \(Personal\)/EdgeLab/strategies/edgelab
sed -i '' 's/TOTAL_CAPITAL=500/TOTAL_CAPITAL=100/' .env
sed -i '' 's/KALSHI_MAX_TRADE_CENTS=1000/KALSHI_MAX_TRADE_CENTS=1000/' .env

# 3. Start EdgeLab
cd ~/Dropbox\ \(Personal\)/EdgeLab/strategies/edgelab
nohup python main.py > logs/edgelab_stdout.log 2>&1 &
echo $! > /tmp/edgelab.pid
```

## Rollback Plan — Revert to Paper Mode Instantly

```bash
# 1. Kill the live process
kill $(cat /tmp/edgelab.pid 2>/dev/null) 2>/dev/null || pkill -f "python main.py"

# 2. Switch back to paper mode
cd ~/Dropbox\ \(Personal\)/EdgeLab/strategies/edgelab
sed -i '' 's/TRADING_MODE=LIVE/TRADING_MODE=paper/' .env
sed -i '' 's/TOTAL_CAPITAL=100/TOTAL_CAPITAL=500/' .env

# 3. Restart in paper mode
nohup python main.py > logs/edgelab_stdout.log 2>&1 &
```

## Post-Live Revert Checklist

- [ ] circuit_breaker.py DAILY_LOSS_LIMIT back to -200.0
- [ ] autonomous_trader.py MAX_OPEN_POSITIONS back to 20
- [ ] .env TOTAL_CAPITAL back to 500
- [ ] .env TRADING_MODE back to paper (if desired)
