"""
risk_manager.py — The 6-gate trade approval pipeline.

Every proposed trade passes through all 6 gates IN ORDER.
If any gate fails the trade is rejected with a reason string.
Gates are pure functions — no side effects.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import config
import database as db
from signal_engine import Signal

log = logging.getLogger(__name__)


@dataclass
class GateResult:
    passed:       bool
    gate:         str
    reason:       str
    adjusted_size: Optional[float] = None   # if gate reduced size rather than blocking


def check_all_gates(signal: Signal, proposed_size: float) -> GateResult:
    """
    Run all 6 gates. Return on the first failure.
    Returns GateResult(passed=True) if all pass.
    """
    bankroll = db.get_bankroll()

    # ── Gate 1: Account health ────────────────────────────────────────────
    floor = config.STARTING_BANKROLL * config.EMERGENCY_FLOOR_PCT
    if bankroll <= floor:
        return GateResult(
            passed=False, gate="Gate1_AccountHealth",
            reason=f"Bankroll ${bankroll:.2f} at/below emergency floor ${floor:.2f}"
        )

    # Also check if bot is manually halted
    if db.is_halted():
        reason = db.get_state("halt_reason") or "Bot is halted"
        return GateResult(passed=False, gate="Gate1_Halted", reason=reason)

    # ── Gate 2: Daily loss limit ──────────────────────────────────────────
    daily_loss = db.get_daily_loss()
    daily_limit = bankroll * config.DAILY_LOSS_HALT_PCT
    if daily_loss >= daily_limit:
        db.set_halt(f"Daily loss limit hit: ${daily_loss:.2f} >= ${daily_limit:.2f}")
        return GateResult(
            passed=False, gate="Gate2_DailyLoss",
            reason=f"Daily loss ${daily_loss:.4f} hit limit ${daily_limit:.4f}"
        )

    # ── Gate 3: Position size ─────────────────────────────────────────────
    max_size = bankroll * config.MAX_POSITION_PCT
    adjusted = proposed_size
    if proposed_size > max_size:
        adjusted = max_size
        log.info("Gate3: size reduced from $%.2f to $%.2f (8%% cap)", proposed_size, adjusted)

    if adjusted < 1.0:
        return GateResult(
            passed=False, gate="Gate3_MinSize",
            reason=f"Adjusted size ${adjusted:.4f} below $1.00 minimum"
        )

    # ── Gate 4: Liquidity ─────────────────────────────────────────────────
    if signal.best_market.liquidity < config.MIN_MARKET_LIQUIDITY:
        return GateResult(
            passed=False, gate="Gate4_Liquidity",
            reason=f"Market liquidity ${signal.best_market.liquidity:.0f} "
                   f"< ${config.MIN_MARKET_LIQUIDITY}"
        )

    # ── Gate 5: Edge ──────────────────────────────────────────────────────
    if signal.edge_pct < config.MIN_EDGE_PCT:
        return GateResult(
            passed=False, gate="Gate5_Edge",
            reason=f"Edge {signal.edge_pct*100:.2f}% < {config.MIN_EDGE_PCT*100:.1f}% minimum"
        )

    # ── Gate 6: Concentration ─────────────────────────────────────────────
    open_trades = db.get_open_trades()
    if len(open_trades) >= config.MAX_OPEN_POSITIONS:
        return GateResult(
            passed=False, gate="Gate6_Concentration",
            reason=f"Already {len(open_trades)} open positions "
                   f"(max {config.MAX_OPEN_POSITIONS} in paranoia mode)"
        )

    # Also check total deployed capital
    total_deployed = sum(t["position_size"] for t in open_trades)
    if (total_deployed + adjusted) > bankroll * config.MAX_DEPLOYED_PCT:
        return GateResult(
            passed=False, gate="Gate6_MaxDeployed",
            reason=f"Adding ${adjusted:.2f} would breach {config.MAX_DEPLOYED_PCT*100:.0f}% "
                   f"deployed cap (currently ${total_deployed:.2f})"
        )

    return GateResult(passed=True, gate="ALL_PASS", reason="",
                      adjusted_size=adjusted)


def check_exit_conditions(
    trade: dict,
    current_yes_price: float,
    seconds_to_resolution: float,
    spot_reversal_pct: float,   # how much spot has moved AGAINST our position since entry
) -> Optional[str]:
    """
    Evaluate whether an open position should be closed early.
    Returns an exit reason string if we should exit, None to hold.

    trade: a dict from db.get_open_trades()
    current_yes_price: latest YES price from Polymarket
    seconds_to_resolution: time until market closes
    spot_reversal_pct: positive = moving against us
    """
    entry_price   = trade["entry_price"]
    contract_side = trade["contract_side"]

    # Current price of OUR contract
    if contract_side == "YES":
        current_price = current_yes_price
    else:
        current_price = 1.0 - current_yes_price

    # Unrealized P&L as % of position
    pnl_pct = (current_price - entry_price) / entry_price

    # ── Exit 1: Stop-loss ─────────────────────────────────────────────────
    if pnl_pct <= -config.STOP_LOSS_PCT:
        return f"STOP_LOSS (pnl={pnl_pct*100:.2f}%)"

    # ── Exit 2: Spot reversal ─────────────────────────────────────────────
    if spot_reversal_pct >= config.REVERSAL_EXIT_PCT * 100:
        return f"REVERSAL (spot reversed {spot_reversal_pct:.3f}%)"

    # ── Exit 3: Lock in profit near resolution ────────────────────────────
    if (seconds_to_resolution <= config.LOCK_PROFIT_SECS
            and pnl_pct > 0):
        return f"LOCK_PROFIT ({seconds_to_resolution:.0f}s to close, pnl={pnl_pct*100:.2f}%)"

    return None   # hold
