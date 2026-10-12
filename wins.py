import re
from datetime import datetime, timedelta, timezone

from discord_api import send_channel_message
from linking import get_recent, get_since_id, msg_text, msg_time

TEAM_CHANNEL = "1523272067011711077"
WIN_CHANNEL = "1424575893178810408"
UNCLAIMED_CHANNEL = "1559221478589988964"
ADDWIN_CHANNEL = "1424577234316034139"


def read_field(text, name):
    match = re.search(rf"(?im)^\s*{re.escape(name)}:\s*(.+)$", text)
    return match.group(1).strip() if match else ""


def parse_team(msg):
    text = msg_text(msg)
    if read_field(text, "Winning Clan").upper() != "[KILR]":
        return None

    time_str = read_field(text, "Time")
    try:
        from email.utils import parsedate_to_datetime
        time = parsedate_to_datetime(time_str).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None

    payouts = read_field(text, "Payout")
    accounts = []
    for part in payouts.split(","):
        match = re.fullmatch(r"\s*(\S+)\s+-?\d+(?:\.\d+)?\s*", part)
        if match:
            accounts.append(match.group(1))

    return {
        "id": msg["id"], "time": time.isoformat(),
        "map": read_field(text, "Map"),
        "prev": read_field(text, "Prev. Points"),
        "curr": read_field(text, "Curr. Points"),
        "contest": read_field(text, "Contest").lower() in ("yes", "true", "1"),
        "accounts": accounts,
        "claims": [], "points": None,
        "posted": False
    }


def parse_claim(msg):
    text = msg_text(msg)
    first = next((line.strip() for line in text.splitlines() if line.strip()), "")
    first = first.replace("**", "")
    match = re.search(
        r"\bKILR\b\s+(.+?)\s+(?:(\d+)\s*x\s*)?(\d+)\s*\[\s*([\d.]+)\s*->\s*([\d.]+)\s*\]",
        first, re.IGNORECASE
    )
    if not match:
        return None
    users = re.findall(r"<@!?(\d+)>", text)
    return {
        "map": match.group(1).strip(),
        "points": int(match.group(2) or 1) * int(match.group(3)),
        "prev": match.group(4), "curr": match.group(5),
        "users": list(dict.fromkeys(users)),
        "time": msg_time(msg)
    }


def match_claim(claim, games):
    matches = []
    for game in games.values():
        if game["map"].casefold() != claim["map"].casefold():
            continue
        try:
            same_score = (abs(float(game["prev"]) - float(claim["prev"])) < 0.00002
                          and abs(float(game["curr"]) - float(claim["curr"])) < 0.00002)
            delay = (claim["time"] - datetime.fromisoformat(game["time"])).total_seconds()
        except (ValueError, TypeError):
            continue
        if same_score and 0 <= delay <= 3600:
            matches.append(game)
    return matches[0] if len(matches) == 1 else None


def linked_users(state, account):
    return [user for user, accounts in state.get("account_links", {}).items() if account in accounts]


def missing_accounts(game, state):
    claimed = set(game["claims"])
    return [account for account in game["accounts"]
            if linked_users(state, account)
            and not any(user in claimed for user in linked_users(state, account))]


def send_unclaimed(game, accounts, state):
    names = ", ".join(f"`{account}`" for account in accounts)
    users = sorted({user for account in accounts for user in linked_users(state, account)})
    tags = " ".join(f"<@{user}>" for user in users)
    points = f" — {game['points']} points each" if game["points"] is not None else ""
    time = datetime.fromisoformat(game["time"]).strftime("%d %b %H:%M UTC")
    send_channel_message(UNCLAIMED_CHANNEL,
                         f"Unclaimed KILR win: **{game['map']}** ({time}){points}\n"
                         f"Accounts: {names}\n{tags}")


def collect_wins(state, now=None):
    """Read and match logs without posting messages or saving state."""
    state.setdefault("games", {})
    state.setdefault("win_last_id", "0")
    now = now or datetime.now(timezone.utc)

    # On the first run, backfill 48 hours. Afterwards, use the saved ID
    # so a busy channel cannot cause older unprocessed messages to be lost.
    if state.get("team_last_id"):
        team_msgs = get_since_id(TEAM_CHANNEL, state["team_last_id"])
    else:
        team_msgs = get_recent(TEAM_CHANNEL, now - timedelta(hours=48), max_pages=200)
        if len(team_msgs) >= 20000:
            raise RuntimeError("Initial team-log scan hit the 20,000-message limit. "
                               "Backfill in smaller time windows before enabling sync.")
        team_msgs.sort(key=lambda msg: int(msg["id"]))

    for msg in team_msgs:
        if msg["id"] not in state["games"]:
            game = parse_team(msg)
            if game:
                state["games"][msg["id"]] = game
        state["team_last_id"] = msg["id"]

    if int(state["win_last_id"]):
        msgs = get_since_id(WIN_CHANNEL, state["win_last_id"])
    else:
        msgs = get_recent(WIN_CHANNEL, now - timedelta(hours=48), max_pages=200)
        if len(msgs) >= 20000:
            raise RuntimeError("Initial winlog scan hit the 20,000-message limit. "
                               "Backfill in smaller time windows before enabling sync.")
        msgs.sort(key=lambda msg: int(msg["id"]))

    pending = state.setdefault("unmatched_claims", {})
    for msg in msgs:
        claim = parse_claim(msg)
        if claim:
            pending[msg["id"]] = {"content": msg.get("content", ""),
                                  "embeds": msg.get("embeds", []),
                                  "timestamp": msg["timestamp"], "id": msg["id"]}
        state["win_last_id"] = msg["id"]

    # Keep ambiguous/unmatched claims for a later run rather than losing them.
    for claim_id, msg in list(pending.items()):
        # Older versions stored plain content strings.
        if isinstance(msg, str):
            continue
        claim = parse_claim(msg)
        game = match_claim(claim, state["games"]) if claim else None
        if game:
            game["points"] = claim["points"]
            game["claims"] = sorted(set(game["claims"]) | set(claim["users"]))
            claim_ids = game.setdefault("claim_ids", [])
            if claim_id not in claim_ids:
                claim_ids.append(claim_id)
            del pending[claim_id]
    return state


def check_wins(state):
    collect_wins(state)
    now = datetime.now(timezone.utc)
    # Only announce after claims have had time to appear.
    for game in state["games"].values():
        if game["posted"] or now - datetime.fromisoformat(game["time"]) < timedelta(minutes=30):
            continue
        accounts = missing_accounts(game, state)
        if accounts:
            send_unclaimed(game, accounts, state)
        game["posted"] = True
    return state


def send_reminders(state):
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    if (now.hour, now.minute) < (14, 30) or state.get("last_reminder") == today:
        return state

    # Reminder covers outstanding wins from the previous 24 hours.
    cutoff = now - timedelta(hours=24)
    due = {}

    for game in state.get("games", {}).values():
        if datetime.fromisoformat(game["time"]) < cutoff:
            continue
        for account in missing_accounts(game, state):
            for user in linked_users(state, account):
                due.setdefault(user, []).append((game, account))

    if due:
        lines = [
            "🏆 **KILR — Daily Unclaimed Wins Reminder**",
            "",
            "You have unclaimed wins! Use `/addwin` in this channel to claim your points.",
            "||Or amgery potato bot will beat u up and take ur pet kangaroos||"
        ]

        for user, wins in sorted(due.items()):
            total = sum(game["points"] or 0 for game, account in wins)
            unknown = any(game["points"] is None for game, account in wins)

            lines.append("")
            heading = f"**<@{user}> — {total} points unclaimed"
            if unknown:
                heading += " (plus unconfirmed points)"
            lines.append(heading + "**")

            for game, account in wins:
                points = (
                    f"{game['points']} pts"
                    if game["points"] is not None
                    else "points not yet confirmed"
                )
                lines.append(f"- {game['map']} — {points}")

        lines.extend([
            "",
            "Don't leave your hard-earned points unclaimed! ⚔️"
        ])

        send_channel_message(ADDWIN_CHANNEL, "\n".join(lines)[:1900])

    state["last_reminder"] = today
    return state
