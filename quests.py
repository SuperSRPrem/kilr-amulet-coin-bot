from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import List, Optional

from discord_api import send_channel_message


PROCEDURAL_MAPS = [
    "White Arena",
    "Black Arena",
    "Island",
    "Mountains 1",
    "Desert",
    "Swamp",
    "White Plains",
    "Cliffs",
    "Pond",
    "Halo",
    "Island Kingdom",
    "Mountains 2",
]

REALISTIC_MAIN_MAPS = [
    "Europe",
    "Africa",
    "North America",
    "South America",
    "Asia",
    "World 1",
    "World 2",
    "Caucasia",
    "Scandinavia",
    "Middle East",
]

WORLD_OR_CAUCASIA = [
    "World 1",
    "World 2",
    "Caucasia",
]

CONTINENT_MAPS = [
    "Europe",
    "Africa",
    "North America",
    "South America",
    "Asia",
    "Australia",
]

REGIONAL_MAPS = [
    "Caucasia",
    "Scandinavia",
    "Middle East",
    "British Isles",
    "Mare Nostrum",
]

EASY_REWARD = 50
MEDIUM_REWARD = 100
HARD_REWARD = 150


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def today_key(now: Optional[datetime] = None) -> str:
    now = now or utc_now()
    return now.astimezone(timezone.utc).date().isoformat()


def quest_day_title(now: Optional[datetime] = None) -> str:
    now = now or utc_now()
    return now.strftime("%B ") + str(now.day)


def ensure_quest_state(state: dict) -> None:
    state.setdefault("daily_quests", None)
    state.setdefault("last_quest_day", None)


def pick_one(options: List[str], used_maps: set[str]) -> str:
    pool = [x for x in options if x not in used_maps]
    if not pool:
        raise RuntimeError("No valid map left while generating daily quests.")
    return random.choice(pool)


def add_flags(quest: dict) -> dict:
    quest["halfway_posted"] = False
    quest["last_leader"] = None
    return quest


def make_easy_quest(used_maps: set[str]) -> dict:
    map_name = pick_one(PROCEDURAL_MAPS, used_maps)
    used_maps.add(map_name)

    return add_flags({
        "level": "easy",
        "kind": "map_wins",
        "map": map_name,
        "target": 4,
        "reward": EASY_REWARD,
        "text": f"Win 4 times on {map_name}",
        "done_by": None,
        "done_at": None,
    })


def make_medium_quest(used_maps: set[str]) -> dict:
    quest_type = random.choice(["proc", "real", "world"])

    if quest_type == "proc":
        map_name = pick_one(PROCEDURAL_MAPS, used_maps)
        used_maps.add(map_name)
        return add_flags({
            "level": "medium",
            "kind": "map_wins",
            "map": map_name,
            "target": 6,
            "reward": MEDIUM_REWARD,
            "text": f"Win 6 times on {map_name}",
            "done_by": None,
            "done_at": None,
        })

    if quest_type == "real":
        map_name = pick_one(REALISTIC_MAIN_MAPS, used_maps)
        used_maps.add(map_name)
        return add_flags({
            "level": "medium",
            "kind": "map_wins",
            "map": map_name,
            "target": 4,
            "reward": MEDIUM_REWARD,
            "text": f"Win 4 times on {map_name}",
            "done_by": None,
            "done_at": None,
        })

    map_name = pick_one(WORLD_OR_CAUCASIA, used_maps)
    used_maps.add(map_name)
    return add_flags({
        "level": "medium",
        "kind": "map_wins",
        "map": map_name,
        "target": 3,
        "reward": MEDIUM_REWARD,
        "text": f"Win 3 times on {map_name}",
        "done_by": None,
        "done_at": None,
    })


def make_hard_quest(used_maps: set[str]) -> dict:
    quest_type = random.choice(["world1", "world2", "contest", "continents", "regions"])

    if quest_type == "world1":
        used_maps.add("World 1")
        return add_flags({
            "level": "hard",
            "kind": "map_wins",
            "map": "World 1",
            "target": 6,
            "reward": HARD_REWARD,
            "text": "Win 6 times on World 1",
            "done_by": None,
            "done_at": None,
        })

    if quest_type == "world2":
        used_maps.add("World 2")
        return add_flags({
            "level": "hard",
            "kind": "map_wins",
            "map": "World 2",
            "target": 6,
            "reward": HARD_REWARD,
            "text": "Win 6 times on World 2",
            "done_by": None,
            "done_at": None,
        })

    if quest_type == "contest":
        return add_flags({
            "level": "hard",
            "kind": "contest_wins",
            "target": 2,
            "reward": HARD_REWARD,
            "text": "Win 2 Contests",
            "done_by": None,
            "done_at": None,
        })

    if quest_type == "continents":
        return add_flags({
            "level": "hard",
            "kind": "map_set",
            "maps": CONTINENT_MAPS[:],
            "target": len(CONTINENT_MAPS),
            "reward": HARD_REWARD,
            "text": "Win on all continent maps",
            "done_by": None,
            "done_at": None,
        })

    return add_flags({
        "level": "hard",
        "kind": "map_set",
        "maps": REGIONAL_MAPS[:],
        "target": len(REGIONAL_MAPS),
        "reward": HARD_REWARD,
        "text": "Win on all regional maps",
        "done_by": None,
        "done_at": None,
    })


def make_daily_quests(now: Optional[datetime] = None) -> dict:
    now = now or utc_now()
    used_maps: set[str] = set()

    hard = make_hard_quest(used_maps)
    easy = make_easy_quest(used_maps)
    medium = make_medium_quest(used_maps)

    return {
        "date": today_key(now),
        "created_at": dt_to_iso(now),
        "quests": {
            "easy": easy,
            "medium": medium,
            "hard": hard,
        },
        "data": {
            "easy": {},
            "medium": {},
            "hard": {},
        },
    }


def copy_last_quest_day(state: dict) -> None:
    old = state.get("daily_quests")
    if not old:
        return

    out = {
        "date": old.get("date"),
        "quests": {},
    }

    for name, quest in old.get("quests", {}).items():
        out["quests"][name] = {
            "text": quest.get("text"),
            "reward": quest.get("reward"),
            "done_by": quest.get("done_by"),
            "done_at": quest.get("done_at"),
        }

    state["last_quest_day"] = out


def reset_quests_if_needed(state, now=None):
    now = now or utc_now()
    ensure_quest_state(state)

    today = today_key(now)
    daily = state.get("daily_quests")

    if daily is None:
        state["daily_quests"] = make_daily_quests(now)
        return True

    if daily.get("date") != today:
        copy_last_quest_day(state)
        state["daily_quests"] = make_daily_quests(now)
        return True

    return False

def add_one_win(quest_data: dict, player: str, map_name: str) -> None:
    player_data = quest_data.setdefault(player, {})
    player_data[map_name] = player_data.get(map_name, 0) + 1


def player_progress(quest: dict, quest_data: dict, player: str) -> int:
    player_data = quest_data.get(player, {})

    if quest["kind"] == "map_wins":
        return player_data.get(quest["map"], 0)

    if quest["kind"] == "contest_wins":
        return player_data.get("Contest", 0)

    if quest["kind"] == "map_set":
        done_count = 0
        for map_name in quest["maps"]:
            if player_data.get(map_name, 0) > 0:
                done_count += 1
        return done_count

    return 0


def player_done(quest: dict, quest_data: dict, player: str) -> bool:
    return player_progress(quest, quest_data, player) >= quest["target"]


def get_leader(quest: dict, quest_data: dict):
    rows = []

    for player in quest_data:
        value = player_progress(quest, quest_data, player)
        if value > 0:
            rows.append((player, value))

    rows.sort(key=lambda x: (-x[1], x[0].lower()))

    if not rows:
        return None, 0, []

    return rows[0][0], rows[0][1], rows[:3]


def halfway_target(quest: dict) -> int:
    return max(2, quest["target"] // 2)


def make_event_line_halfway(level_name: str, leader: str) -> str:
    return f"🔥 {leader} reached halfway in {level_name.title()} Quest!"


def make_event_line_lead(level_name: str, leader: str) -> str:
    return f"⚡ {leader} has taken the lead in {level_name.title()} Quest!"


def make_event_line_done(level_name: str, winner: str) -> str:
    return f"🏆 {winner} completed the {level_name.title()} Quest!"


def check_updates(quest: dict, quest_data: dict, level_name: str) -> list[str]:
    events = []

    if quest.get("done_by"):
        return events

    leader, value, _ = get_leader(quest, quest_data)
    if leader is None:
        return events

    half = halfway_target(quest)

    if not quest.get("halfway_posted") and value >= half:
        quest["halfway_posted"] = True
        quest["last_leader"] = leader
        events.append(make_event_line_halfway(level_name, leader))
        return events

    if quest.get("halfway_posted") and quest.get("last_leader") != leader:
        quest["last_leader"] = leader
        events.append(make_event_line_lead(level_name, leader))

    return events


def update_one_quest(quest: dict, quest_data: dict, records: List[object], level_name: str, now: datetime) -> List[str]:
    events: List[str] = []

    if quest.get("done_by"):
        return events

    changed = False

    for record in records:
        winners = [username for username, _ in record.payout]

        if not winners:
            continue

        if quest["kind"] == "map_wins":
            if record.map_name != quest["map"]:
                continue

        elif quest["kind"] == "contest_wins":
            if str(record.contest).strip().lower() != "yes":
                continue

        elif quest["kind"] == "map_set":
            if record.map_name not in quest["maps"]:
                continue

        for player in winners:
            if quest["kind"] == "contest_wins":
                add_one_win(quest_data, player, "Contest")
            else:
                add_one_win(quest_data, player, str(record.map_name))

            changed = True

            if not quest.get("done_by") and player_done(quest, quest_data, player):
                quest["done_by"] = player
                quest["done_at"] = dt_to_iso(now)
                events.append(make_event_line_done(level_name, player))
                return events

    if changed:
        events.extend(check_updates(quest, quest_data, level_name))

    return events


def update_quests(state: dict, records: List[object], now: Optional[datetime] = None) -> List[str]:
    now = now or utc_now()
    reset_quests_if_needed(state, now)

    daily = state["daily_quests"]
    quests = daily["quests"]
    data = daily["data"]

    events: List[str] = []
    events += update_one_quest(quests["easy"], data["easy"], records, "easy", now)
    events += update_one_quest(quests["medium"], data["medium"], records, "medium", now)
    events += update_one_quest(quests["hard"], data["hard"], records, "hard", now)
    return events


def make_progress_lines(level_name: str, quest: dict, quest_data: dict) -> list[str]:
    lines = []
    lines.append(f"**{level_name.title()}: {quest['text']}**")

    if quest.get("done_by"):
        lines.append(f"*Completed by {quest['done_by']}*")
        return lines

    top_rows = get_leader(quest, quest_data)[2]

    if not top_rows:
        lines.append("*No progress yet*")
        return lines

    for name, value in top_rows:
        lines.append(f"* {name} : {value} / {quest['target']}")

    return lines


def make_combined_message(state: dict, events: List[str], now: datetime) -> str:
    daily = state["daily_quests"]
    quests = daily["quests"]
    data = daily["data"]

    lines = []

    for event in events:
        lines.append(event)

    if events:
        lines.append("")

    lines.append("__**Progress Update**__")
    lines.append("")
    lines.extend(make_progress_lines("easy", quests["easy"], data["easy"]))
    lines.append("")
    lines.extend(make_progress_lines("medium", quests["medium"], data["medium"]))
    lines.append("")
    lines.extend(make_progress_lines("hard", quests["hard"], data["hard"]))

    return "\n".join(lines)


def send_quest_updates(config: dict, message: str) -> None:
    channel_id = str(config.get("quest_channel_id") or "").strip()
    if not channel_id:
        return

    send_channel_message(channel_id, message)


def handle_quests(state: dict, config: dict, records: List[object], now: Optional[datetime] = None) -> List[str]:
    now = now or utc_now()
    ensure_quest_state(state)
    
    is_new_day = reset_quests_if_needed(state, now)
    events = update_quests(state, records, now)

    if events:
        message = make_combined_message(state, events, now)
        send_quest_updates(config, message)

    if is_new_day:
        msg = format_today_quests(state, now)
        send_quest_updates(config, msg)
    return events


def format_today_quests(state: dict, now: Optional[datetime] = None) -> str:
    now = now or utc_now()
    reset_quests_if_needed(state, now)

    daily = state["daily_quests"]
    quests = daily["quests"]
    last_day = state.get("last_quest_day")

    lines = []

    if last_day:
        lines.append(f"# **🏁 Yesterday's Quest Winners — {last_day.get('date', '')}**")
        lines.append("")

        for level_name in ["easy", "medium", "hard"]:
            quest = last_day["quests"].get(level_name, {})
            winner = quest.get("done_by") or "No winner"
            text = quest.get("text") or ""

            lines.append(f"**{level_name.title()}:** {text}")
            lines.append(f"Winner: {winner}")

        lines.append("")
        lines.append("*Gold rewards will be sent to the winners' accounts by clan leaders shortly.*")
        lines.append("")
        lines.append("---")
        lines.append("")

    lines.append(f"# **🧭 Daily KILR Quests — {quest_day_title(now)}**")
    lines.append("")
    lines.append(f"**Easy (50 Gold):** {quests['easy']['text']}")
    lines.append("")
    lines.append(f"**Medium (100 Gold):** {quests['medium']['text']}")
    lines.append("")
    lines.append(f"**Hard (150 Gold):** {quests['hard']['text']}")

    return "\n".join(lines)


def format_last_quest_winners(state: dict) -> str:
    last_day = state.get("last_quest_day")
    if not last_day:
        return ""

    lines = []
    lines.append(f"# 🏁 **Yesterday's Quest Winners — {last_day.get('date', '')}**")
    lines.append("")

    for level_name in ["easy", "medium", "hard"]:
        quest = last_day["quests"].get(level_name, {})
        winner = quest.get("done_by") or "No winner"
        text = quest.get("text") or ""
        lines.append(f"**{level_name.title()}:** {text}")
        lines.append(f"Winner: {winner}")
        lines.append("")

    return "".join(lines).strip()