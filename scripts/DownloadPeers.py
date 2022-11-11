import json
import time
import datetime
from selenium import webdriver
from random import randrange
from urllib import parse
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN

browser = webdriver.Chrome()
base_url = 'https://api.tracker.gg/api/v2/warzone/standard/matches/atvi/{}?type=wz'
opening_response_tag = '<html><head><meta name="color-scheme" content="light dark"></head><body><pre style="word-wrap: break-word; white-space: pre-wrap;">'
closing_response_tag = '</pre></body></html>'
request_count = 0


def reset_connection():
    global browser
    browser.close()
    browser.quit()
    time.sleep(5)
    rotate_VPN()
    browser = webdriver.Chrome()
    browser.delete_all_cookies()


# Load the full list of match ids.
known_peers = set()
try:
    with open('../data/peers.processed.jsonl', 'r') as peers_file:
        for peer_str in peers_file.readlines():
            if peer_str.startswith('{'):
                peer = json.loads(peer_str)
                known_peers.add(peer['username'].strip())
except Exception as e:
    print('Failed loading existing matches: {}'.format(str(e)))


def current_timestamp():
    return datetime.datetime.now().isoformat()


# Method for loading, parsing and extracting matches for a given
# player given a URL.
def fetch_peer(username):
    errors = []
    matches = []
    page = '2020-07-01T01:00:00+00:00'

    while page:
        fetched_matches, encountered_errors, page = fetch_peer_matches(username, page)

        for match in fetched_matches:
            matches.append(match)

        for error in encountered_errors:
            errors.append(error)
    return matches, errors


#
#
def fetch_peer_matches(peer, curr_page):
    time.sleep(0 + randrange(2))
    query_url = '{}&next={}'.format(base_url.format(parse.quote(peer)), parse.quote_plus(str(curr_page)))
    print('{}| Peer query: {}'.format(current_timestamp(), query_url))

    errors = []
    matches = []

    global request_count
    if request_count >= 50:
        reset_connection()
        request_count = 0

    browser.get(query_url)
    request_count += 1

    response_html = browser.page_source
    response_str = response_html.replace(opening_response_tag, '')
    response_str = response_str.replace(closing_response_tag, '')

    if response_str.startswith('{') and response_str.endswith('}'):
        response_json = json.loads(response_str)

        if 'errors' in response_json:
            if any(error['code'] == 'RateLimited' or error['code'] == 'Warden::Challenge' for error in response_json['errors']):
                print('{}| Rate Limited! Sleeping 30 seconds...'.format(current_timestamp()))
                time.sleep(15)
                reset_connection()
                request_count = 0
                return matches, errors, curr_page
            else:
                print('{}| ERROR Skipping peer {} due to failed query. cause: {}.'
                      .format(current_timestamp(), peer, response_json))
                errors.append({ 'username': peer, 'response': response_json })
                return matches, errors, None
        else:
            data = response_json['data']
            next_page = data['metadata']['next']

            for match in data['matches']:
                match_json = {
                    'id': match['attributes']['id'],
                    'duration_ms': match['metadata'].get('duration', {}).get('value', 0),
                    'timestamp': match['metadata']['timestamp'],
                    'player_count': match['metadata']['playerCount'],
                    'team_count': match['metadata']['teamCount'],
                    'map': match['metadata']['mapName'],
                    'mode': match['metadata']['modeName']
                }

                segments = match['segments']
                segments = [x for x in segments if x['type'] == 'overview']
                overview = segments[0]
                stats = overview['stats']

                match_json['kills'] = stats.get('kills', {}).get('value', 0)
                match_json['score'] = stats.get('score', {}).get('value', 0)
                match_json['deaths'] = stats.get('deaths', {}).get('value', 0)
                match_json['kd_ratio'] = stats.get('kdRatio', {}).get('value', 0)
                match_json['longest_streak'] = stats.get('longestStreak', {}).get('value', 0)
                match_json['damage_done'] = stats.get('damageDone', {}).get('value', 0)
                match_json['damage_taken'] = stats.get('damageTaken', {}).get('value', 0)
                match_json['damage_done_per_minute'] = stats.get('damageDonePerMinute', {}).get('value', 0)
                match_json['percent_time_moving'] = stats.get('percentTimeMoving', {}).get('value', 0)
                match_json['executions'] = stats.get('executions', {}).get('value', 0)
                match_json['nearmisses'] = stats.get('nearmisses', {}).get('value', 0)
                match_json['time_played'] = stats.get('timePlayed', {}).get('value', 0)
                match_json['assists'] = stats.get('assists', {}).get('value', 0)
                match_json['spm'] = stats.get('scorePerMinute', {}).get('value', 0)
                match_json['headshots'] = stats.get('headshots', {}).get('value', 0)
                match_json['match_xp'] = stats.get('matchXp', {}).get('value', 0)
                match_json['total_xp'] = stats.get('totalXp', {}).get('value', 0)
                match_json['score_xp'] = stats.get('scoreXp', {}).get('value', 0)

                metadata = overview['metadata']
                placement = metadata['placement']
                match_json['placement'] = placement

                teammates = metadata['teammates'] or []
                match_json['teammates'] = [{'id': t['platformUserHandle'], 'stats': t['stats']} for t in teammates]

                matches.append(match_json)

            if eagerly_terminate_match_queries(matches):
                next_page = None
                print('{}| WARN Eagerly terminating peer match queries! current_match_count: {}, current_page: {}, next_page: {}'
                      .format(current_timestamp(), str(len(matches)), str(curr_page), str(next_page)))
            return matches, errors, next_page
    else:
        error_message = 'Failed downloading matches for player {}'.format(peer)
        print(error_message)
        request_count = 0
        reset_connection()
        raise Exception(error_message)


def eagerly_terminate_match_queries(batch):
    for match in batch:
        if match['timestamp'].isnumeric() and datetime.datetime.fromtimestamp(match['timestamp']).year > 2020:
            return True
        elif match['timestamp'] > '2020-07-01T01:00:00+00:00':
            return True
        elif len(batch) < 10:
            return True


def download_peers():
    with open('../data/matches.processed.jsonl', 'r') as matches_file:
        with open('../data/peers.processed.jsonl', 'a') as peers_processed_file:
            with open('../data/peers.raw.jsonl', 'a') as peers_raw_file:
                with open('../data/peer_errors.jsonl', 'a') as errors_file:
                    initialize_VPN(save=1, area_input=['complete rotation'])
                    rotate_VPN()

                    for match_str in matches_file.readlines():
                        match = json.loads(match_str)

                        for player in match['players']:
                            peer_username = player['username'].strip()

                            if peer_username in known_peers:
                                print("{}| Skipping peer {}".format(current_timestamp(), peer_username))
                            else:
                                processed_matches, errors = fetch_peer(peer_username)

                                peers_processed_file.write('{}\n'.format(json.dumps({
                                    'username': peer_username,
                                    'matches': processed_matches
                                })))

                                if len(errors) > 0:
                                    errors_file.write('{}\n'.format(json.dumps({
                                        'username': peer_username,
                                        'matches': errors
                                    })))

                                known_peers.add(peer_username)
                                print('{}| Processed {} matches for player {}. Encountered {} errors'
                                      .format(current_timestamp(),
                                              str(len(processed_matches)),
                                              peer_username,
                                              str(len(errors))))
    terminate_VPN()
    print('Done!')


download_peers()
