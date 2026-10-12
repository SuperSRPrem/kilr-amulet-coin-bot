from datetime import datetime, timedelta, timezone
from linking import get_recent
from wins import TEAM_CHANNEL, WIN_CHANNEL, parse_team, parse_claim, match_claim

since = datetime.now(timezone.utc) - timedelta(hours=2)
team = get_recent(TEAM_CHANNEL, since, max_pages=10)
winlogs = get_recent(WIN_CHANNEL, since, max_pages=10)
games = {msg['id']: game for msg in team if (game := parse_team(msg))}
claims = [claim for msg in winlogs if (claim := parse_claim(msg))]
matched = [(claim, match_claim(claim, games)) for claim in claims]
print(f'Team messages: {len(team)}, KILR victories: {len(games)}')
print(f'Winlog messages: {len(winlogs)}, parsed claims: {len(claims)}')
print(f'Matched claims: {sum(game is not None for _, game in matched)}')
for claim, game in matched[:20]:
    print(f"{claim['map']} | {claim['prev']} -> {claim['curr']} | "
          f"{len(claim['users'])} claimants | "
          f"{'MATCH ' + game['id'] if game else 'UNMATCHED'}")
print('Read-only test: no messages sent and no state changed.')
