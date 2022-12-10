import json
import random
import time
import datetime
from retry import retry
from selenium import webdriver
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN
from random import randrange

page_size = 1
browser = webdriver.Chrome()
base_url = 'https://api.tracker.gg/api/v1/warzone/standard/leaderboards?type=battle-royale&platform=psn&board=Wins&skip={}&take={}'
opening_response_tag = '<html><head><meta name="color-scheme" content="light dark"></head><body><pre style="word-wrap: break-word; white-space: pre-wrap;">'
closing_response_tag = '</pre></body></html>'


def reset_connection():
    global browser
    global request_count
    browser.delete_all_cookies()
    browser.close()
    browser.quit()
    time.sleep(0.5 + randrange(2))
    rotate_VPN()
    time.sleep(0.5 + randrange(2))
    request_count = 0
    browser = webdriver.Chrome()
    browser.delete_all_cookies()


def rate_limited():
    print('{}| RATE_LIMITED'.format(current_timestamp()))
    reset_connection()


def current_timestamp():
    return datetime.datetime.now().isoformat()


@retry(tries=5, delay=3)
def next_player(player_index,
                  known_players,
                  error_file,
                  players_raw_file,
                  players_processed_file):
    url = base_url.format(player_index, page_size)
    browser.get(url)
    response_html = browser.page_source
    response_str = response_html.replace(opening_response_tag, '')
    response_str = response_str.replace(closing_response_tag, '')

    if response_str.startswith('{') and response_str.endswith('}'):
        response = json.loads(response_str)
        if 'errors' in response:
            errors = response_str['errors']
            if any(error['code'] == 'RateLimited' for error in errors):
                rate_limited()
            elif any(error['code'] == 'LeaderboardStatus::NoData' for error in errors):
                raise Exception('The leaderboard is not currently available.')
            else:
                print('Failed loading players for player index {}. Response: {}'
                  .format(str(player_index),
                          response))
                error_file.write(json.dumps({'error': browser.page_source}))
        else:
            for player_json in response["data"]["items"]:
                player_id = player_json['owner']['id']

                if player_id not in known_players:
                    players_raw_file.write('{}\n'.format(json.dumps(player_json)))
                    players_processed_file.write('{}\n'.format(json.dumps({'id': player_id})))
                    print('Processed player {}'.format(player_id))
                else:
                    print('Skipped {}'.fomat(player_id))
    elif 'Access denied' in response_str or \
            'Checking if the site connection is secure' in response_str or \
            'review the security of your connection' in response_str:
        rate_limited()
        return next_player(player_index,
                           known_players,
                           error_file,
                           players_raw_file,
                           players_processed_file)
    else:
        print('Failed loading players for player index {}. Response: {}'
              .format(str(player_index), response_html))
        error_file.write(json.dumps({'error': browser.page_source}))


# Load a list of player ids we've already downloaded.
known_players = set()
try:
    with open('../data/sampled_players.processed.jsonl', 'r') as players_file:
        for entry in players_file.readlines():
            if entry.startswith('{'):
                parsed_entry = json.loads(entry)
                known_players.add(parsed_entry['id'].strip())
except Exception as e:
    print('Failed loading previous known players: {}'.format(e))


# Page through a leaderboard, writing to file any players
# we haven't encountered before.
def sample_next_set_of_players(n=5):
    with open('../data/sampled_players.raw.jsonl', 'a') as players_raw_file:
        with open('../data/sampled_players.processed.jsonl', 'a') as players_processed_file:
            player_count = 0
            with open('../data/sampled_players.errors.jsonl', 'a') as error_file:
                initialize_VPN(save=1, area_input=['complete rotation'])
                rotate_VPN()

                while player_count < n:
                    try:
                        player_index = random.randrange(1, 50_000)
                        next_player(player_index,
                                      known_players,
                                      error_file,
                                      players_raw_file,
                                      players_processed_file)
                        player_count += 1
                    except Exception as e:
                        print('Failed loading players for player index {} due to error {}'
                              .format(str(player_index), str(e)))
                        rotate_VPN()


sample_next_set_of_players()
