"""Read-only integration test for KILR win tracking and timezone totals."""
import json
from collections import Counter
from copy import deepcopy

from storage import load_state
from wins import collect_wins, missing_accounts
from timezones import update_stats, get_totals

state = deepcopy(load_state())
before_games = len(state.get('games', {}))
collect_wins(state)
update_stats(state)

games = list(state.get('games', {}).values())
claims = sum(len(game.get('claims', [])) for game in games)
zero = [game for game in games if game.get('points') is not None and not game.get('claims')]
missing = [(game, missing_accounts(game, state)) for game in games]
missing = [(game, accounts) for game, accounts in missing if accounts]
totals = get_totals(state)

print('Read-only integration test')
print('Games in state before:', before_games)
print('KILR victories after scan:', len(games))
print('Claimed player-win pairs:', claims)
print('Matched winlogs with zero claimants:', len(zero))
print('Unmatched winlogs:', len(state.get('unmatched_claims', {})))
print('Linked Discord users:', len(state.get('account_links', {})))
print('Games with linked, unclaimed accounts:', len(missing))
print('Timezone points:', {block: data['total'] for block, data in totals.items()})
for game, accounts in missing[:10]:
    print('UNCLAIMED:', game['map'], game['time'], ','.join(accounts))
for game in zero[:5]:
    print('ZERO CLAIMANTS:', game['map'], game['time'], 'points=', game['points'])
print('No Discord messages sent. No state file saved.')
