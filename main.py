from __future__ import annotations

import argparse
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from discord_api import DiscordAPIError, send_channel_message, sync_single_winner_role
from storage import load_config, load_state, save_state
from territorial import MatchRecord, fetch_and_parse
from quests import ensure_quest_state, handle_quests


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def iso_to_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def ceil_to_slot(dt: datetime, slot_minutes: int) -> datetime:
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    remainder = dt.minute % slot_minutes
    if remainder == 0:
        return dt
    add_minutes = slot_minutes - remainder
    return dt + timedelta(minutes=add_minutes)


def schedule_next_event(config: dict, state: dict, now: Optional[datetime] = None) -> datetime:
    now = now or utc_now()
    slot_minutes = int(config["slot_minutes"])
    min_gap = timedelta(hours=float(config["min_gap_hours"]))

    earliest_allowed = now + min_gap

    tomorrow_date = (now.astimezone(timezone.utc) + timedelta(days=1)).date()
    tomorrow_start = datetime.combine(tomorrow_date, datetime.min.time(), tzinfo=timezone.utc)
    tomorrow_end = tomorrow_start + timedelta(days=1)

    range_start = max(tomorrow_start, earliest_allowed)
    range_start = ceil_to_slot(range_start, slot_minutes)

    range_end = tomorrow_end - timedelta(minutes=slot_minutes)

    if range_start > range_end:
        raise RuntimeError(
            "No valid event slot exists tomorrow with the current slot_minutes/min_gap_hours settings."
        )

    total_slots = int((range_end - range_start).total_seconds() // (slot_minutes * 60))
    offset_slots = random.randint(0, total_slots)

    next_event = range_start + timedelta(minutes=offset_slots * slot_minutes)
    state["next_event_time"] = dt_to_iso(next_event)
    return next_event


def _ensure_runtime_state(state: dict) -> None:
    state.setdefault("links", {})
    state.setdefault("history", [])
    state.setdefault("last_seen_time", state.get("last_boundary_time"))
    state.setdefault("current_scores", {})
    state.setdefault("current_cycle_started_at", None)
    state.setdefault("current_cycle_match_count", 0)

    state.setdefault("last_announced_event_time", None)
    ensure_quest_state(state)



def _deserialize_scores(raw_scores: dict) -> Dict[str, dict]:
    scores: Dict[str, dict] = {}
    for username, data in raw_scores.items():
        last_score_time = iso_to_dt(data.get("last_score_time"))
        if last_score_time is None:
            continue
        scores[username] = {
            "points": float(data.get("points", 0.0)),
            "match_count": int(data.get("match_count", 0)),
            "last_score_time": last_score_time,
        }
    return scores


def _serialize_scores(scores: Dict[str, dict]) -> dict:
    out = {}
    for username, data in scores.items():
        out[username] = {
            "points": float(data["points"]),
            "match_count": int(data["match_count"]),
            "last_score_time": dt_to_iso(data["last_score_time"]),
        }
    return out


def format_state_summary(config: dict, state: dict) -> str:
    _ensure_runtime_state(state)
    return (
        f"enabled={state.get('enabled')}\n"
        f"winning_clan={config.get('winning_clan')}\n"
        f"next_event_time={state.get('next_event_time')}\n"
        f"last_event_time={state.get('last_event_time')}\n"
        f"last_seen_time={state.get('last_seen_time')}\n"
        f"current_cycle_started_at={state.get('current_cycle_started_at')}\n"
        f"current_cycle_match_count={state.get('current_cycle_match_count')}\n"
        f"current_competitors={len(state.get('current_scores', {}))}\n"
        f"links={len(state.get('links', {}))}\n"
        f"history_entries={len(state.get('history', []))}"

        f"last_announced_event_time={state.get('last_announced_event_time')}\n"
    )


def _oldest_and_latest_times(records: List[MatchRecord]) -> Tuple[datetime, datetime]:
    latest = max(r.match_time for r in records)
    oldest = min(r.match_time for r in records)
    return oldest, latest


def _filter_new_kilr_matches(
    records: List[MatchRecord],
    winning_clan: str,
    last_seen_time: Optional[datetime],
    now: datetime,
) -> List[MatchRecord]:
    out = []
    for record in records:
        if record.winning_clan != winning_clan:
            continue
        if record.match_time > now:
            continue
        if last_seen_time is not None and record.match_time <= last_seen_time:
            continue
        out.append(record)
    out.sort(key=lambda r: r.match_time)
    return out


def _accumulate_scores(scores: Dict[str, dict], records: List[MatchRecord]) -> None:
    for record in records:
        for username, points in record.payout:
            if username not in scores:
                scores[username] = {
                    "points": 0.0,
                    "match_count": 0,
                    "last_score_time": record.match_time,
                }
            scores[username]["points"] += float(points)
            scores[username]["match_count"] += 1
            if record.match_time > scores[username]["last_score_time"]:
                scores[username]["last_score_time"] = record.match_time


def _pick_winner(scores: Dict[str, dict]) -> Optional[Tuple[str, dict]]:
    if not scores:
        return None
    ranked = sorted(
        scores.items(),
        key=lambda item: (
            -item[1]["points"],
            -item[1]["match_count"],
            -item[1]["last_score_time"].timestamp(),
            item[0].lower(),
        ),
    )
    return ranked[0]


def _trim_history(state: dict, config: dict) -> None:
    limit = int(config["history_limit"])
    history = state.get("history", [])
    if len(history) > limit:
        state["history"] = history[-limit:]


def _week_key(dt: datetime) -> str:
    iso_year, iso_week, _ = dt.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


def _month_key(dt: datetime) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def _current_role_leaders(state: dict) -> Tuple[Optional[str], Optional[str]]:
    history = state.get("history", [])
    if not history:
        return None, None

    weekly_counts = defaultdict(lambda: {"wins": 0, "latest": ""})
    monthly_counts = defaultdict(lambda: {"wins": 0, "latest": ""})

    for entry in history:
        winner = entry.get("winner_username")
        event_time_s = entry.get("event_time")
        if not winner or not event_time_s:
            continue
        event_dt = iso_to_dt(event_time_s)
        if event_dt is None:
            continue

        wk = (_week_key(event_dt), winner)
        weekly_counts[wk]["wins"] += 1
        weekly_counts[wk]["latest"] = max(weekly_counts[wk]["latest"], event_time_s)

        mk = (_month_key(event_dt), winner)
        monthly_counts[mk]["wins"] += 1
        monthly_counts[mk]["latest"] = max(monthly_counts[mk]["latest"], event_time_s)

    latest_event_time = iso_to_dt(history[-1]["event_time"])
    if latest_event_time is None:
        return None, None

    current_week = _week_key(latest_event_time)
    current_month = _month_key(latest_event_time)

    week_candidates = []
    month_candidates = []

    for (bucket, winner), data in weekly_counts.items():
        if bucket == current_week:
            week_candidates.append((winner, data["wins"], data["latest"]))

    for (bucket, winner), data in monthly_counts.items():
        if bucket == current_month:
            month_candidates.append((winner, data["wins"], data["latest"]))

    weekly_winner = None
    monthly_winner = None

    if week_candidates:
        week_candidates.sort(key=lambda x: (-x[1], x[2], x[0].lower()))
        weekly_winner = week_candidates[0][0]

    if month_candidates:
        month_candidates.sort(key=lambda x: (-x[1], x[2], x[0].lower()))
        monthly_winner = month_candidates[0][0]

    return weekly_winner, monthly_winner


def _sync_roles(config: dict, state: dict) -> List[str]:
    guild_id = str(config.get("guild_id") or "").strip()
    if not guild_id:
        return []

    weekly_role_id = str(config.get("weekly_role_id") or "").strip() or None
    monthly_role_id = str(config.get("monthly_role_id") or "").strip() or None
    if not weekly_role_id and not monthly_role_id:
        return []

    links: Dict[str, str] = state.get("links", {})
    linked_user_ids = list(links.values())

    weekly_territorial_winner, monthly_territorial_winner = _current_role_leaders(state)
    weekly_discord_user = links.get(weekly_territorial_winner) if weekly_territorial_winner else None
    monthly_discord_user = links.get(monthly_territorial_winner) if monthly_territorial_winner else None

    notes = []

    if weekly_role_id:
        sync_single_winner_role(
            guild_id=guild_id,
            role_id=weekly_role_id,
            winner_discord_user_id=weekly_discord_user,
            linked_user_ids=linked_user_ids,
        )
        if weekly_territorial_winner:
            notes.append(f"weekly_role->{weekly_territorial_winner}")
        else:
            notes.append("weekly_role->none")

    if monthly_role_id:
        sync_single_winner_role(
            guild_id=guild_id,
            role_id=monthly_role_id,
            winner_discord_user_id=monthly_discord_user,
            linked_user_ids=linked_user_ids,
        )
        if monthly_territorial_winner:
            notes.append(f"monthly_role->{monthly_territorial_winner}")
        else:
            notes.append("monthly_role->none")

    return notes


def _send_announcement(config: dict, content: str) -> None:
    channel_id = str(config.get("announce_channel_id") or "").strip()
    if not channel_id:
        raise RuntimeError("config.announce_channel_id is empty")
    send_channel_message(channel_id, content)


def _initialize_tracking_state(config: dict, state: dict, now: datetime, latest_visible: datetime) -> int:
    state["last_seen_time"] = dt_to_iso(latest_visible)
    state["last_event_time"] = dt_to_iso(now)
    state["current_scores"] = {}
    state["current_cycle_started_at"] = dt_to_iso(now)
    state["current_cycle_match_count"] = 0
    schedule_next_event(config, state, now=now)
    save_state(state)
    _send_announcement(
        config,
        "✅ **KILR tracker initialized**\n"
        f"Starting fresh from latest visible log time: `{dt_to_iso(latest_visible)}`\n"
        "Next event scheduled successfully."
    )
    print("Initialized tracking state.")
    return 0


def command_enable(args: argparse.Namespace) -> int:
    config = load_config()
    state = load_state()
    _ensure_runtime_state(state)

    state["enabled"] = True

    if args.fresh_boundary:
        records = fetch_and_parse()
        if not records:
            raise RuntimeError("Territorial log returned no records, cannot set fresh start.")
        _, latest_visible = _oldest_and_latest_times(records)
        state["last_seen_time"] = dt_to_iso(latest_visible)
        state["current_scores"] = {}
        state["current_cycle_match_count"] = 0
        state["current_cycle_started_at"] = dt_to_iso(utc_now())

    next_event = schedule_next_event(config, state)
    save_state(state)
    print(f"Enabled. Next event: {dt_to_iso(next_event)}")
    return 0


def command_disable(args: argparse.Namespace) -> int:
    _ = args
    state = load_state()
    _ensure_runtime_state(state)
    state["enabled"] = False
    save_state(state)
    print("Disabled.")
    return 0


def command_status(args: argparse.Namespace) -> int:
    _ = args
    config = load_config()
    state = load_state()
    _ensure_runtime_state(state)
    print(format_state_summary(config, state))
    return 0


def command_set_boundary_now(args: argparse.Namespace) -> int:
    _ = args
    records = fetch_and_parse()
    if not records:
        raise RuntimeError("Territorial log returned no records, cannot set start point.")
    state = load_state()
    _ensure_runtime_state(state)
    _, latest_visible = _oldest_and_latest_times(records)
    state["last_seen_time"] = dt_to_iso(latest_visible)
    state["current_scores"] = {}
    state["current_cycle_match_count"] = 0
    state["current_cycle_started_at"] = dt_to_iso(utc_now())
    save_state(state)
    print(f"last_seen_time set to latest visible timestamp: {dt_to_iso(latest_visible)}")
    return 0


def command_link(args: argparse.Namespace) -> int:
    state = load_state()
    _ensure_runtime_state(state)
    links = state.setdefault("links", {})
    links[args.territorial_username] = args.discord_user_id
    save_state(state)
    print(f"Linked {args.territorial_username} -> {args.discord_user_id}")
    return 0


def command_unlink(args: argparse.Namespace) -> int:
    state = load_state()
    _ensure_runtime_state(state)
    links = state.setdefault("links", {})
    existed = links.pop(args.territorial_username, None)
    save_state(state)
    if existed:
        print(f"Unlinked {args.territorial_username}")
    else:
        print(f"No link existed for {args.territorial_username}")
    return 0


def command_run(args: argparse.Namespace) -> int:
    config = load_config()
    state = load_state()
    _ensure_runtime_state(state)

    if not state.get("enabled") and not args.force:
        print("Bot is disabled. Exiting.")
        return 0

    now = utc_now()
    next_event_time = iso_to_dt(state.get("next_event_time"))

    if next_event_time is None:
        next_event_time = schedule_next_event(config, state, now=now)
        save_state(state)
        if not args.force:
            print(f"Scheduled first event for {dt_to_iso(next_event_time)}")
            return 0

    records = fetch_and_parse()
    if not records:
        raise RuntimeError("Territorial log returned no records.")

    _, latest_visible = _oldest_and_latest_times(records)
    last_seen_time = iso_to_dt(state.get("last_seen_time"))

    if last_seen_time is None:
        return _initialize_tracking_state(config, state, now, latest_visible)

    new_kilr_records = _filter_new_kilr_matches(
        records=records,
        winning_clan=str(config["winning_clan"]),
        last_seen_time=last_seen_time,
        now=now,
    )

    handle_quests(state, config, new_kilr_records, now)

    latest_processed_time = max((r.match_time for r in new_kilr_records), default=None)

    event_due = args.force or (next_event_time is not None and now >= next_event_time)
    cutoff_time = next_event_time if next_event_time and next_event_time <= now and not args.force else now

    scheduled_event_key = dt_to_iso(next_event_time) if next_event_time else None

    if (
        not args.force
        and event_due
        and scheduled_event_key is not None
        and state.get("last_announced_event_time") == scheduled_event_key
    ):
        print("Event already announced. Skipping duplicate post.")
        return 0

    current_cycle_new: List[MatchRecord] = []
    carryover_new: List[MatchRecord] = []

    if event_due:
        for record in new_kilr_records:
            if record.match_time <= cutoff_time:
                current_cycle_new.append(record)
            else:
                carryover_new.append(record)
    else:
        current_cycle_new = new_kilr_records

    current_scores = _deserialize_scores(state.get("current_scores", {}))
    _accumulate_scores(current_scores, current_cycle_new)
    state["current_cycle_match_count"] = int(state.get("current_cycle_match_count", 0)) + len(current_cycle_new)

    if latest_processed_time is not None:
        state["last_seen_time"] = dt_to_iso(latest_processed_time)

    if state.get("current_cycle_started_at") is None:
        state["current_cycle_started_at"] = dt_to_iso(now)

    if not event_due:
        state["current_scores"] = _serialize_scores(current_scores)
        save_state(state)
        print(f"Collected {len(current_cycle_new)} new [KILR] wins. Not due yet.")
        return 0

    scores = current_scores
    winner = _pick_winner(scores)

    winner_username = None
    winner_points = None

    event_time = next_event_time if next_event_time and next_event_time <= now and not args.force else now
    event_time_str = event_time.strftime("%H:%M GMT")
    event_date = event_time.strftime("%B ") + str(event_time.day)

    if winner is not None:
        winner_username, winner_data = winner
        winner_points = round(float(winner_data["points"]), 2)

    summary_lines = []

    summary_lines.append(f"# 🪙 **KILR Amulet Coin Challenge — {event_date}**")
    summary_lines.append("")
    summary_lines.append(f"🕒 **Today’s randomly chosen time:** {event_time_str}")
    summary_lines.append("")

    if winner is None:
        summary_lines.append("No eligible **[KILR]** wins were counted today.")
    else:
        summary_lines.append(f"**👑 Winner: __{winner_username}__**")
        summary_lines.append("")
        summary_lines.append("💰 Reward: **100 Gold**")
        summary_lines.append("")

        ranked = sorted(
            scores.items(),
            key=lambda item: (
                -item[1]["points"],
                -item[1]["match_count"],
                -item[1]["last_score_time"].timestamp(),
                item[0].lower(),
            ),
        )[:10]

        rank_w = len(str(len(ranked))) if ranked else 1
        name_w = max(len("Player"), max((len(name) for name, _ in ranked), default=6))
        pts_w = max(len("Points"), max((len(f"{data['points']:.2f}") for _, data in ranked), default=6))
        wins_w = max(len("Wins"), max((len(str(data["match_count"])) for _, data in ranked), default=4))

        leaderboard_lines = []
        leaderboard_lines.append(
            f"{'#':<{rank_w}}  {'Player':<{name_w}}  {'Points':>{pts_w}}  {'Wins':>{wins_w}}"
        )
        leaderboard_lines.append(
            f"{'-'*rank_w}  {'-'*name_w}  {'-'*pts_w}  {'-'*wins_w}"
        )

        for i, (name, data) in enumerate(ranked, start=1):
            leaderboard_lines.append(
                f"{i:<{rank_w}}  {name:<{name_w}}  {data['points']:>{pts_w}.2f}  {data['match_count']:>{wins_w}}"
            )

        summary_lines.append("```")
        summary_lines.extend(leaderboard_lines)
        summary_lines.append("```")

    summary_lines.append("")
    summary_lines.append("> A random time is chosen each day. Whoever is leading at that time wins.")
    summary_lines.append("-# 👀 Keep pushing — tomorrow’s result can lock in at any moment")

    state["history"].append({
        "event_time": dt_to_iso(event_time),
        "cycle_started_at": state.get("current_cycle_started_at"),
        "winner_username": winner_username,
        "winner_points": winner_points,
        "eligible_match_count": int(state.get("current_cycle_match_count", 0)),
    })
    _trim_history(state, config)



    role_notes = []
    if winner_username is not None:
        role_notes = _sync_roles(config, state)

    state["last_event_time"] = dt_to_iso(event_time)

    schedule_next_event(config, state, now=now)

    next_cycle_scores: Dict[str, dict] = {}
    _accumulate_scores(next_cycle_scores, carryover_new)
    state["current_scores"] = _serialize_scores(next_cycle_scores)
    state["current_cycle_match_count"] = len(carryover_new)
    state["current_cycle_started_at"] = dt_to_iso(event_time)

    if role_notes:
        summary_lines.append("Role sync: " + ", ".join(role_notes))

    should_post = winner is not None or config.get("post_no_win_rounds", True)
    if should_post:
        _send_announcement(config, "\\n".join(summary_lines))

    if scheduled_event_key is not None and not args.force:
        state["last_announced_event_time"] = scheduled_event_key

    save_state(state)
    print("Run complete.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KILR Territorial scheduled Discord bot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_enable = subparsers.add_parser("enable", help="Enable the bot and schedule the next event")
    p_enable.add_argument("--fresh-boundary", action="store_true", help="Set fresh start from latest visible log")
    p_enable.set_defaults(func=command_enable)

    p_disable = subparsers.add_parser("disable", help="Disable the bot")
    p_disable.set_defaults(func=command_disable)

    p_status = subparsers.add_parser("status", help="Print current config/state summary")
    p_status.set_defaults(func=command_status)

    p_boundary = subparsers.add_parser("set-boundary-now", help="Reset start point to latest visible log timestamp now")
    p_boundary.set_defaults(func=command_set_boundary_now)

    p_link = subparsers.add_parser("link", help="Link Territorial username to Discord user ID")
    p_link.add_argument("territorial_username")
    p_link.add_argument("discord_user_id")
    p_link.set_defaults(func=command_link)

    p_unlink = subparsers.add_parser("unlink", help="Remove a Territorial username link")
    p_unlink.add_argument("territorial_username")
    p_unlink.set_defaults(func=command_unlink)

    p_run = subparsers.add_parser("run", help="Collect new wins and post result if due")
    p_run.add_argument("--force", action="store_true", help="Run result logic now even if not due yet")
    p_run.set_defaults(func=command_run)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        return args.func(args)
    except DiscordAPIError as exc:
        print(f"DISCORD ERROR: {exc}")
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
