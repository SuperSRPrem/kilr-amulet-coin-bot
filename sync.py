import argparse

from storage import load_state, save_state
from linking import handle_links
from wins import check_wins, send_reminders
from timezones import update_stats, reset_stats, get_totals


def run():
    state = load_state()

    # Save after each step so completed work isn't lost if a later step fails.
    handle_links(state)
    save_state(state)

    check_wins(state)
    save_state(state)

    update_stats(state)
    save_state(state)

    send_reminders(state)
    save_state(state)
    print('Account links, wins and timezone stats updated.')


def show_stats():
    state = load_state()
    for block, data in get_totals(state).items():
        print(f"{block} GMT: {data['total']} points")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', nargs='?', default='run', choices=['run', 'stats', 'reset-stats'])
    args = parser.parse_args()

    if args.command == 'run':
        run()
    elif args.command == 'stats':
        show_stats()
    else:
        state = load_state()
        reset_stats(state)
        save_state(state)
        print('Timezone stats reset.')


if __name__ == '__main__':
    main()
