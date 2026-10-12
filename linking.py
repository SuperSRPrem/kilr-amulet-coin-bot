import re
from datetime import datetime, timedelta, timezone

from discord_api import get_messages, send_channel_message

LINK_CHANNEL = "1559222136395141240"
GOLD_CHANNEL = "1523272072728412251"
VERIFY_ACCOUNT = "_tFM4"


def msg_text(msg):
    parts = [msg.get("content", "")]
    for embed in msg.get("embeds", []):
        parts.append(embed.get("title", ""))
        parts.append(embed.get("description", ""))
        for field in embed.get("fields", []):
            parts.append(f'{field.get("name", "")}: {field.get("value", "")}')
    return "\n".join(parts)


def msg_time(msg):
    return datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))


def get_recent(channel, since, max_pages=20):
    found = []
    before = ""
    for _ in range(max_pages):
        msgs = get_messages(channel, 100, before)
        if not msgs:
            break
        for msg in msgs:
            if msg_time(msg) >= since:
                found.append(msg)
        if msg_time(msgs[-1]) < since or len(msgs) < 100:
            break
        before = msgs[-1]["id"]
    return found


def parse_transfer(msg):
    text = msg_text(msg)
    accounts = re.findall(r"(?im)^\s*Account:\s*(\S+)", text)
    if len(accounts) < 2:
        return None
    return accounts[0], accounts[1]


def check_transfer(account, transfers):
    for msg in transfers:
        pair = parse_transfer(msg)
        if pair and pair[0] == account and pair[1] == VERIFY_ACCOUNT:
            return True
    return False


def handle_links(state):
    state.setdefault("account_links", {})
    last_id = int(state.get("link_last_id", "0"))
    now = datetime.now(timezone.utc)
    msgs = get_recent(LINK_CHANNEL, now - timedelta(days=2))
    pending = [m for m in msgs if int(m["id"]) > last_id and not m.get("author", {}).get("bot")]
    pending.sort(key=lambda m: int(m["id"]))

    if not pending:
        return state

    transfers = get_recent(GOLD_CHANNEL, now - timedelta(hours=2))
    for msg in pending:
        text = msg.get("content", "").strip()
        match = re.fullmatch(r"!link\s+(\S+)", text, re.IGNORECASE)
        if match:
            account = match.group(1)
            user = msg["author"]["id"]
            if check_transfer(account, transfers):
                accounts = state["account_links"].setdefault(user, [])
                if account not in accounts:
                    accounts.append(account)
                reply = f"<@{user}> Linked `{account}` successfully."
            else:
                reply = (f"<@{user}> No gold transfer found from `{account}` to "
                         f"`{VERIFY_ACCOUNT}` in the last 2 hours. Send any amount and try `!link {account}` again.")
            send_channel_message(LINK_CHANNEL, reply)
        state["link_last_id"] = msg["id"]
    return state
