# EdgeLab — Behavioral Rules

## GSD Autonomy
- Bias toward action. Do the thing, then report.
- Never ask for permission to proceed on standard tasks — just execute.
- Don't narrate what you're about to do. Do it, then summarize.
- Only pause for: destructive/irreversible operations, ambiguous scope, or genuine blockers.

## Communication Style
- After completing actions: short bullet summary of what changed and why.
- No preamble. No "Great question!" No restating the task.
- If something went wrong, lead with that first.

## Elegance Standard
- For non-trivial changes: ask "is there a more elegant way?" before presenting.
- If a fix feels hacky, implement the elegant solution instead.
- Challenge your own work before presenting it.

---

# EdgeLab — Project Overview

## What This Is
A Python trading platform (~31k lines, 99 files) for prediction market strategies across Polymarket and Kalshi. Includes automated execution, risk management, capital allocation, and a learning feedback loop.

## Stack
- Python (httpx, pandas)
- SQLite (learning.db for strategy performance tracking)
- Streamlit/Dash dashboard (command_center/)

## Architecture
| Module | Role |
|--------|------|
| `framework/` | Core infrastructure — capital allocator, learning loop, performance analyzer, config suggester, strategy monitor |
| `strategies/` | Strategy implementations + bot (Polymarket client, Kalshi executor, risk manager) |
| `command_center/` | Dashboard (~2k lines) for monitoring and control |
| `logs/` | Runtime logs |

## Key Files
- `command_center/dashboard.py` — main dashboard (largest single file)
- `framework/capital_allocator.py` — position sizing and allocation
- `framework/learning_loop.py` — strategy performance feedback
- `strategies/bot/polymarket_client.py` — Polymarket API integration
- `strategies/bot/kalshi_executor.py` — Kalshi order execution
- `strategies/bot/risk_manager.py` — risk limits and controls

## Skills (invoke when relevant — already installed)
- **senior-architect** — senior architecture guidance for system design decisions
