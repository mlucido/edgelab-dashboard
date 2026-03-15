"""
Momentum scorer for Sports Momentum strategy.
Computes momentum signals from live game state and history.
"""
import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

from . import config

logger = logging.getLogger("sports_momentum.momentum_scorer")


@dataclass
class MomentumSignal:
    game_id: str
    sport: str
    favored_team: str          # team with momentum
    direction: str             # "HOME" or "AWAY"
    score: float               # 0.0 - 1.0
    confidence: str            # "HIGH", "MEDIUM", "LOW"
    factors: list[str] = field(default_factory=list)
    implied_prob_shift: float = 0.0  # estimated win prob shift
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def _parse_clock(clock_str: str) -> float:
    """Convert 'MM:SS' clock string to total seconds remaining."""
    try:
        parts = clock_str.strip().split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        return 0.0
    except Exception:
        return 0.0


def _nba_momentum(game_state: dict, history: list[dict]) -> Optional[MomentumSignal]:
    """
    NBA momentum detection:
    - Scoring runs (8+ points in 3 min window)
    - Foul trouble on key players
    - Clutch time multiplier (1.5x if last 2 min of 4th)
    - Garbage time filter (margin > 20 with < 3 min left)
    """
    thresholds = config.MOMENTUM_THRESHOLDS["nba"]
    game_id = game_state.get("game_id", "")
    period = game_state.get("period", 0)
    clock_secs = _parse_clock(game_state.get("clock", "12:00"))
    home_score = game_state.get("home_score", 0)
    away_score = game_state.get("away_score", 0)
    margin = abs(home_score - away_score)

    # Garbage time filter: blow-out in late 4th
    if period >= 4 and clock_secs < 180 and margin > 20:
        return None

    # Clutch time multiplier
    clutch_multiplier = 1.5 if (period >= 4 and clock_secs <= 120) else 1.0

    factors = []
    score = 0.0
    direction = None

    # Scoring run detection from history
    if len(history) >= 2:
        prev = history[-2]
        curr = history[-1]
        home_diff = curr.get("home_score", 0) - prev.get("home_score", 0)
        away_diff = curr.get("away_score", 0) - prev.get("away_score", 0)

        # Build run tally from recent history (up to run_window_minutes)
        run_window = thresholds["run_window_minutes"] * 60
        home_run = 0
        away_run = 0
        now_ts = datetime.now(timezone.utc).timestamp()
        for snap in history:
            snap_ts = snap.get("fetched_at", "")
            try:
                dt = datetime.fromisoformat(snap_ts.replace("Z", "+00:00"))
                age = now_ts - dt.timestamp()
                if age <= run_window:
                    home_run = curr.get("home_score", 0) - snap.get("home_score", 0)
                    away_run = curr.get("away_score", 0) - snap.get("away_score", 0)
                    break
            except Exception:
                pass

        run_threshold = thresholds["run_points"]
        if home_run >= run_threshold and home_run > away_run:
            factors.append(f"home_run_{home_run}pts")
            score += 0.4 * clutch_multiplier
            direction = "HOME"
        elif away_run >= run_threshold and away_run > home_run:
            factors.append(f"away_run_{away_run}pts")
            score += 0.4 * clutch_multiplier
            direction = "AWAY"

    # Foul trouble proxy: check last_play for foul references
    last_play = game_state.get("last_play", "").lower()
    if "foul" in last_play:
        # Check which team's player got foul
        if game_state.get("home_team", "").lower().split()[-1] in last_play:
            if direction == "AWAY" or direction is None:
                factors.append("home_foul_trouble")
                score += 0.2
                direction = "AWAY" if direction is None else direction
        else:
            if direction == "HOME" or direction is None:
                factors.append("away_foul_trouble")
                score += 0.2
                direction = "HOME" if direction is None else direction

    score = min(score, 1.0)
    if score < 0.1 or direction is None:
        return None

    confidence = "HIGH" if score > 0.5 else ("MEDIUM" if score >= 0.3 else "LOW")
    implied_shift = score * 0.12  # momentum translates to ~12% prob shift at max

    return MomentumSignal(
        game_id=game_id,
        sport="nba",
        favored_team=game_state.get(f"{direction.lower()}_team", ""),
        direction=direction,
        score=round(score, 3),
        confidence=confidence,
        factors=factors,
        implied_prob_shift=round(implied_shift, 3),
    )


def _nfl_momentum(game_state: dict, history: list[dict]) -> Optional[MomentumSignal]:
    """
    NFL momentum detection:
    - Score swing (+7 = TD, weights for turnovers/TDs/FGs/3-and-outs)
    - Turnover: +0.4 momentum shift
    - TD: +0.2
    - FG: +0.1
    - 3-and-out: +0.1
    """
    thresholds = config.MOMENTUM_THRESHOLDS["nfl"]
    game_id = game_state.get("game_id", "")
    home_score = game_state.get("home_score", 0)
    away_score = game_state.get("away_score", 0)
    last_play = game_state.get("last_play", "").lower()

    factors = []
    score = 0.0
    direction = None

    # Score swing from history
    if len(history) >= 2:
        prev = history[-2]
        home_delta = home_score - prev.get("home_score", 0)
        away_delta = away_score - prev.get("away_score", 0)

        swing = thresholds["score_swing"]
        if home_delta >= swing and home_delta > away_delta:
            factors.append(f"home_score_swing_{home_delta}pts")
            score += 0.35
            direction = "HOME"
        elif away_delta >= swing and away_delta > home_delta:
            factors.append(f"away_score_swing_{away_delta}pts")
            score += 0.35
            direction = "AWAY"

    # Play-by-play signal parsing
    play_direction = None
    play_boost = 0.0

    if "interception" in last_play or "fumble" in last_play:
        play_boost = 0.4 * thresholds["turnover_weight"]
        factors.append("turnover")
        # Turnover benefits the defense — we'd need team context; use score trend as proxy
        play_direction = "HOME" if away_score > home_score else "AWAY"  # benefiting team
    elif "touchdown" in last_play:
        play_boost = 0.2
        factors.append("touchdown")
        play_direction = "HOME" if home_score > away_score else "AWAY"
    elif "field goal" in last_play or "field_goal" in last_play:
        play_boost = 0.1
        factors.append("field_goal")
        play_direction = "HOME" if home_score > away_score else "AWAY"
    elif "punt" in last_play:
        play_boost = 0.1
        factors.append("three_and_out")
        play_direction = "HOME" if home_score > away_score else "AWAY"

    if play_boost > 0:
        score += play_boost
        if direction is None:
            direction = play_direction
        elif direction != play_direction:
            # Conflicting signals — average them
            score *= 0.7

    score = min(score, 1.0)
    if score < 0.1 or direction is None:
        return None

    confidence = "HIGH" if score > 0.5 else ("MEDIUM" if score >= 0.3 else "LOW")
    implied_shift = score * 0.10

    return MomentumSignal(
        game_id=game_id,
        sport="nfl",
        favored_team=game_state.get(f"{direction.lower()}_team", ""),
        direction=direction,
        score=round(score, 3),
        confidence=confidence,
        factors=factors,
        implied_prob_shift=round(implied_shift, 3),
    )


def _mlb_momentum(game_state: dict, history: list[dict]) -> Optional[MomentumSignal]:
    """
    MLB momentum detection:
    - Scoring innings (+0.3 per multi-run inning)
    - Pitcher trouble (+0.2)
    - Lead changes (+0.4)
    """
    thresholds = config.MOMENTUM_THRESHOLDS["mlb"]
    game_id = game_state.get("game_id", "")
    home_score = game_state.get("home_score", 0)
    away_score = game_state.get("away_score", 0)
    last_play = game_state.get("last_play", "").lower()

    factors = []
    score = 0.0
    direction = None

    # Lead change detection
    if len(history) >= 2:
        prev = history[-2]
        prev_home = prev.get("home_score", 0)
        prev_away = prev.get("away_score", 0)

        was_home_leading = prev_home > prev_away
        is_home_leading = home_score > away_score

        if was_home_leading != is_home_leading and (home_score != away_score):
            factors.append("lead_change")
            score += 0.4
            direction = "HOME" if is_home_leading else "AWAY"

        # Scoring inning detection
        home_inning_runs = home_score - prev_home
        away_inning_runs = away_score - prev_away
        run_threshold = thresholds["inning_runs"]

        if home_inning_runs >= run_threshold:
            factors.append(f"home_scoring_inning_{home_inning_runs}R")
            score += 0.3 * thresholds["bases_loaded_weight"]
            direction = "HOME" if direction is None else direction
        elif away_inning_runs >= run_threshold:
            factors.append(f"away_scoring_inning_{away_inning_runs}R")
            score += 0.3 * thresholds["bases_loaded_weight"]
            direction = "AWAY" if direction is None else direction

    # Pitcher trouble proxy from last play
    pitcher_trouble_keywords = ["walks", "wild pitch", "passed ball", "error", "home run"]
    if any(kw in last_play for kw in pitcher_trouble_keywords):
        factors.append("pitcher_trouble")
        score += 0.2 * thresholds["pitcher_era_weight"]
        # Trouble for the pitching team → benefits the batting team
        if direction is None:
            direction = "HOME" if away_score > home_score else "AWAY"

    score = min(score, 1.0)
    if score < 0.1 or direction is None:
        return None

    confidence = "HIGH" if score > 0.5 else ("MEDIUM" if score >= 0.3 else "LOW")
    implied_shift = score * 0.10

    return MomentumSignal(
        game_id=game_id,
        sport="mlb",
        favored_team=game_state.get(f"{direction.lower()}_team", ""),
        direction=direction,
        score=round(score, 3),
        confidence=confidence,
        factors=factors,
        implied_prob_shift=round(implied_shift, 3),
    )


def compute_momentum(
    game_state: dict,
    sport: str,
    history: list[dict],
) -> Optional[MomentumSignal]:
    """
    Main entry point. Routes to sport-specific scorer.
    Returns MomentumSignal or None if signal too weak.
    """
    try:
        if sport == "nba" or sport == "ncaab":
            return _nba_momentum(game_state, history)
        elif sport == "nfl":
            return _nfl_momentum(game_state, history)
        elif sport == "mlb":
            return _mlb_momentum(game_state, history)
        else:
            logger.warning(f"No momentum scorer for sport: {sport}")
            return None
    except Exception as e:
        logger.error(f"Momentum computation error for {sport} game {game_state.get('game_id')}: {e}")
        return None
