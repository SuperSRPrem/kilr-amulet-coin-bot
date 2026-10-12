from datetime import datetime, timezone


def block_time(time):
    if isinstance(time, str):
        time = datetime.fromisoformat(time.replace("Z", "+00:00"))
    time = time.astimezone(timezone.utc)
    start = (time.hour // 3) * 3
    return f"{start:02d}:00-{(start + 3) % 24:02d}:00"


def reset_stats(state):
    # Reset totals without deleting match or claim history.
    state["tz_stats"] = {
        "since": datetime.now(timezone.utc).isoformat(),
        "entries": {}
    }
    return state


def update_stats(state):
    stats = state.setdefault("tz_stats", {"since": None, "entries": {}})
    entries = stats.setdefault("entries", {})
    since = datetime.fromisoformat(stats["since"]) if stats.get("since") else None

    for game_id, game in state.get("games", {}).items():
        if game.get("points") is None or not game.get("time"):
            continue
        time = datetime.fromisoformat(game["time"].replace("Z", "+00:00"))
        if since and time < since:
            continue
        points = int(game["points"])
        for user in game.get("claims", []):
            key = f"{game_id}:{user}"
            entries[key] = {
                "user": str(user),
                "game": str(game_id),
                "time": time.isoformat(),
                "block": block_time(time),
                "points": points
            }
    return state


def get_totals(state):
    totals = {f"{h:02d}:00-{(h + 3) % 24:02d}:00": {"total": 0, "players": {}}
              for h in range(0, 24, 3)}
    for entry in state.get("tz_stats", {}).get("entries", {}).values():
        block = totals[entry["block"]]
        points = entry["points"]
        user = entry["user"]
        block["total"] += points
        block["players"][user] = block["players"].get(user, 0) + points
    return totals


def top_players(state, block, limit=5):
    players = get_totals(state)[block]["players"]
    return sorted(players.items(), key=lambda item: (-item[1], item[0]))[:limit]
