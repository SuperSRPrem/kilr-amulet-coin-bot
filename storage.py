from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
STATE_PATH = ROOT / "state.json"

DEFAULT_CONFIG = {
    "winning_clan": "[KILR]",
    "announce_channel_id": "",
    "guild_id": "",
    "weekly_role_id": None,
    "monthly_role_id": None,
    "slot_minutes": 30,
    "min_gap_hours": 12,
    "random_window_hours": 12,
    "history_limit": 300,
    "post_no_win_rounds": True,
}

DEFAULT_STATE = {
    "enabled": False,
    "next_event_time": None,
    "last_event_time": None,
    "last_boundary_time": None,
    "links": {},
    "history": [],
}


def _load_json(path: Path, default_data: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return deepcopy(default_data)

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    merged = deepcopy(default_data)
    if isinstance(data, dict):
        merged.update(data)
    return merged


def load_config() -> Dict[str, Any]:
    config = _load_json(CONFIG_PATH, DEFAULT_CONFIG)

    slot_minutes = int(config["slot_minutes"])
    if slot_minutes <= 0:
        raise ValueError("config.slot_minutes must be > 0")

    if 60 % slot_minutes != 0:
        raise ValueError("config.slot_minutes must divide 60 cleanly (e.g. 5, 10, 15, 20, 30, 60)")

    config["min_gap_hours"] = float(config["min_gap_hours"])
    config["random_window_hours"] = float(config["random_window_hours"])
    config["history_limit"] = int(config["history_limit"])
    config["post_no_win_rounds"] = bool(config["post_no_win_rounds"])

    return config


def load_state() -> Dict[str, Any]:
    state = _load_json(STATE_PATH, DEFAULT_STATE)
    if not isinstance(state.get("links"), dict):
        state["links"] = {}
    if not isinstance(state.get("history"), list):
        state["history"] = []
    return state


def save_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
