from discord_api import get_messages
from linking import msg_text, parse_transfer
from wins import parse_team, parse_claim

channels = {
    'Team logs': ('1523272067011711077', parse_team),
    'Gold logs': ('1523272072728412251', parse_transfer),
    'KILR winlogs': ('1424575893178810408', parse_claim),
    'Link channel': ('1559222136395141240', None),
    'Addwin channel': ('1424577234316034139', None),
    'Unclaimed channel': ('1559221478589988964', None),
}

for name, (channel, parser) in channels.items():
    try:
        msgs = get_messages(channel, 20)
        readable = sum(bool(msg_text(m).strip()) for m in msgs)
        parsed = sum(parser(m) is not None for m in msgs) if parser else '-'
        print(f'{name}: OK, fetched={len(msgs)}, readable={readable}, parsed={parsed}')
    except Exception as err:
        print(f'{name}: ERROR: {type(err).__name__}: {err}')
