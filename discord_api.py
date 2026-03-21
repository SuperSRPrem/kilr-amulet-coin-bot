from __future__ import annotations

import os
from typing import Iterable, Optional

import requests


DISCORD_API_BASE = "https://discord.com/api/v10"


class DiscordAPIError(RuntimeError):
    pass


def get_bot_token() -> str:
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    if not token:
        raise DiscordAPIError("Environment variable DISCORD_BOT_TOKEN is not set.")
    return token


def _headers() -> dict:
    token = get_bot_token()
    return {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }


def send_channel_message(channel_id: str, content: str) -> dict:
    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    resp = requests.post(url, headers=_headers(), json={"content": content}, timeout=30)
    if resp.status_code >= 400:
        raise DiscordAPIError(f"Discord send message failed: {resp.status_code} {resp.text}")
    return resp.json()


def add_role(guild_id: str, user_id: str, role_id: str) -> None:
    url = f"{DISCORD_API_BASE}/guilds/{guild_id}/members/{user_id}/roles/{role_id}"
    resp = requests.put(url, headers=_headers(), timeout=30)
    if resp.status_code not in (204, 201):
        raise DiscordAPIError(f"Discord add role failed: {resp.status_code} {resp.text}")


def remove_role(guild_id: str, user_id: str, role_id: str) -> None:
    url = f"{DISCORD_API_BASE}/guilds/{guild_id}/members/{user_id}/roles/{role_id}"
    resp = requests.delete(url, headers=_headers(), timeout=30)
    if resp.status_code != 204:
        if resp.status_code == 404:
            return
        raise DiscordAPIError(f"Discord remove role failed: {resp.status_code} {resp.text}")


def sync_single_winner_role(
    guild_id: str,
    role_id: Optional[str],
    winner_discord_user_id: Optional[str],
    linked_user_ids: Iterable[str],
) -> None:
    if not guild_id or not role_id:
        return

    unique_users = []
    seen = set()
    for user_id in linked_user_ids:
        if not user_id or user_id in seen:
            continue
        seen.add(user_id)
        unique_users.append(user_id)

    for user_id in unique_users:
        if winner_discord_user_id and user_id == winner_discord_user_id:
            add_role(guild_id, user_id, role_id)
        else:
            remove_role(guild_id, user_id, role_id)
