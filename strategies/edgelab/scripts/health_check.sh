#!/bin/bash
# EdgeLab Health Check — Run via cron every 5 minutes
# Cron example: */5 * * * * /Users/mattlucido/Dropbox/EdgeLab/strategies/edgelab/scripts/health_check.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/../logs/health_alerts.log"

response=$(curl -sf http://localhost:8502/health)
if [ $? -ne 0 ]; then
  echo "$(date): EdgeLab DOWN — health check failed" >> "$LOG_FILE"
  # Send Telegram alert if token available
  if [ -n "$TELEGRAM_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
    curl -s "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
      -d "chat_id=${TELEGRAM_CHAT_ID}" \
      -d "text=🚨 EdgeLab DOWN — health check failed at $(date)" > /dev/null
  fi
fi
