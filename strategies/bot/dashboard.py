"""
dashboard.py — No-op stub.

The terminal dashboard has been retired in favor of the Command Center
(command_center/dashboard.py on port 8050). This stub keeps the
Dashboard class importable so main.py doesn't break.
"""

import logging

log = logging.getLogger(__name__)


class Dashboard:
    """No-op replacement — all monitoring moved to Command Center."""

    def start(self):
        log.info("Terminal dashboard disabled — use Command Center at localhost:8050")

    def stop(self):
        pass
