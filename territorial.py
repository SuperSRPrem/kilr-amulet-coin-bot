from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional, Tuple

import requests


CLAN_RESULTS_URL = "https://territorial.io/clan-results"


@dataclass
class MatchRecord:
    match_time: datetime
    contest: Optional[str]
    map_name: Optional[str]
    player_count: Optional[int]
    winning_clan: Optional[str]
    prev_points: Optional[float]
    gain: Optional[float]
    curr_points: Optional[float]
    payout: List[Tuple[str, float]]
    clan_winners: List[str]
    raw_lines: List[str]


def _normalize_dt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_log_time(value: str) -> datetime:
    dt = parsedate_to_datetime(value.strip())
    return _normalize_dt(dt)


def fetch_raw_results(timeout: int = 30) -> str:
    response = requests.get(CLAN_RESULTS_URL, timeout=timeout)
    response.raise_for_status()
    return response.text


def split_blocks(raw_text: str) -> List[List[str]]:
    lines = [line.rstrip() for line in raw_text.splitlines()]
    blocks: List[List[str]] = []
    current: List[str] = []

    for line in lines:
        if line.strip().startswith("Time:"):
            if current:
                blocks.append(current)
            current = [line]
        elif current:
            current.append(line)

    if current:
        blocks.append(current)

    return blocks


def _extract_value(line: str) -> str:
    _, value = line.split(":", 1)
    return value.strip()


def _maybe_float(value: Optional[str]) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _maybe_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


_PAYOUT_RE = re.compile(r"(.+?)\s+(-?\d+(?:\.\d+)?)$")


def parse_payout(value: str) -> List[Tuple[str, float]]:
    value = value.strip()
    if not value:
        return []

    results: List[Tuple[str, float]] = []
    for part in [p.strip() for p in value.split(",") if p.strip()]:
        match = _PAYOUT_RE.match(part)
        if not match:
            continue
        username = match.group(1).strip()
        points = float(match.group(2))
        results.append((username, points))
    return results


def parse_block(block_lines: List[str]) -> Optional[MatchRecord]:
    fields = {}

    for line in block_lines:
        stripped = line.strip()
        if ":" not in stripped:
            continue
        key, _ = stripped.split(":", 1)
        fields[key.strip()] = _extract_value(stripped)

    time_value = fields.get("Time")
    if not time_value:
        return None

    try:
        match_time = parse_log_time(time_value)
    except Exception:
        return None

    payout = parse_payout(fields.get("Payout", ""))
    clan_winners = [x.strip() for x in fields.get("Clan Winners", "").split(",") if x.strip()]

    return MatchRecord(
        match_time=match_time,
        contest=fields.get("Contest"),
        map_name=fields.get("Map"),
        player_count=_maybe_int(fields.get("Player Count")),
        winning_clan=fields.get("Winning Clan"),
        prev_points=_maybe_float(fields.get("Prev. Points")),
        gain=_maybe_float(fields.get("Gain")),
        curr_points=_maybe_float(fields.get("Curr. Points")),
        payout=payout,
        clan_winners=clan_winners,
        raw_lines=block_lines,
    )


def fetch_and_parse(timeout: int = 30) -> List[MatchRecord]:
    raw_text = fetch_raw_results(timeout=timeout)
    blocks = split_blocks(raw_text)

    records: List[MatchRecord] = []
    for block in blocks:
        record = parse_block(block)
        if record is not None:
            records.append(record)

    records.sort(key=lambda r: r.match_time, reverse=True)
    return records
