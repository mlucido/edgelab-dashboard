import logging
from datetime import datetime, timezone
from pathlib import Path
from .performance_analyzer import analyze_strategy, get_all_strategies

REPORTS_DIR = Path(__file__).parent / "reports"
logger = logging.getLogger("framework.config_suggester")

def generate_suggestions() -> list:
    """Weekly analysis. Outputs human-readable suggestions. NEVER auto-modifies config."""
    strategies = get_all_strategies()
    suggestions = []

    for s in strategies:
        perf = analyze_strategy(s, lookback_days=30)
        if perf["trades"] < 5:
            continue

        if perf["trades"] >= 20 and perf["win_rate"] < 0.45:
            suggestions.append(f"Consider reducing {s} position size by 25% (WR: {perf['win_rate']:.1%})")
        if perf["trades"] >= 20 and perf["win_rate"] > 0.65:
            suggestions.append(f"Consider increasing {s} position size by 10% (WR: {perf['win_rate']:.1%})")
        if perf["avg_edge"] < 0.03:
            suggestions.append(f"{s}: Edge is thin ({perf['avg_edge']:.1%}) — check if market conditions have changed")
        if perf.get("sharpe", 0) < 0.5:
            suggestions.append(f"{s}: Risk-adjusted returns below threshold (Sharpe: {perf.get('sharpe', 0):.2f}) — review signal thresholds")

    # Write report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).date().isoformat()
    report_path = REPORTS_DIR / f"weekly_{today}.txt"
    report_content = f"EdgeLab Config Suggestions — {today}\n{'='*50}\n\n"
    if suggestions:
        for i, s in enumerate(suggestions, 1):
            report_content += f"{i}. {s}\n"
    else:
        report_content += "No suggestions at this time. All strategies performing within bounds.\n"

    report_path.write_text(report_content)
    logger.info(f"[SUGGEST] Wrote {len(suggestions)} suggestions to {report_path}")
    return suggestions
