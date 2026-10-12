from datetime import datetime, timedelta, timezone

from linking import get_recent
from wins import TEAM_CHANNEL, parse_team

since = datetime.now(timezone.utc) - timedelta(hours=2)
messages = get_recent(TEAM_CHANNEL, since, max_pages=10)
games = [game for msg in messages if (game := parse_team(msg)) is not None]

print(f'Team messages fetched in last 2 hours: {len(messages)}')
print(f'KILR victories parsed: {len(games)}')
for game in sorted(games, key=lambda game: game['time'], reverse=True)[:10]:
    print(f"{game['time']} | {game['map']} | {game['prev']} -> {game['curr']} | {len(game['accounts'])} payout accounts")
if len(messages) >= 1000:
    print('WARNING: Reached 10-page limit; some messages may be missing.')
