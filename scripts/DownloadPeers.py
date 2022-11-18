import json
import time
import datetime
from selenium import webdriver
from random import randrange
from urllib import parse
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN

browser = webdriver.Chrome()
base_url = 'https://api.tracker.gg/api/v2/warzone/standard/matches/{}/{}?type=wz'
opening_response_tag = '<html><head><meta name="color-scheme" content="light dark"></head><body><pre style="word-wrap: break-word; white-space: pre-wrap;">'
closing_response_tag = '</pre></body></html>'
request_count = 0


def reset_connection():
    global browser
    global request_count
    browser.close()
    browser.quit()
    time.sleep(randrange(2))
    rotate_VPN()
    time.sleep(randrange(2))
    request_count = 0
    browser = webdriver.Chrome()
    browser.delete_all_cookies()


def rate_limited():
    print('{}| Rate Limited...'.format(current_timestamp()))
    reset_connection()


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
def fetch_peer(username, platform_username, platform):
    errors = []
    matches = []
    page = '2020-07-01T01:00:00+00:00'

    while page:
        fetched_matches, encountered_errors, page = fetch_peer_matches(username, platform, platform_username, page)

        for match in fetched_matches:
            matches.append(match)

        for error in encountered_errors:
            errors.append(error)
    return matches, errors


#
#
def fetch_peer_matches(peer, platform, platform_username, curr_page):
    time.sleep(0 + randrange(2))
    page_arg = parse.quote_plus(str(curr_page))
    unpaged_query_url = base_url.format(platform, parse.quote(platform_username))
    query_url = '{}&next={}'.format(unpaged_query_url, page_arg)
    print('{}| Querying for peer \'{}\'. Query: {}'.format(current_timestamp(), peer, query_url))

    errors = []
    matches = []

    global request_count
    if request_count >= 50:
        reset_connection()

    browser.get(query_url)
    request_count += 1

    response_html = browser.page_source
    response_str = response_html.replace(opening_response_tag, '')
    response_str = response_str.replace(closing_response_tag, '')

    if response_str.startswith('{') and response_str.endswith('}'):
        response_json = json.loads(response_str)

        if 'errors' in response_json:
            if any(error['code'] == 'RateLimited' or error['code'] == 'Warden::Challenge' for error in
                   response_json['errors']):
                rate_limited()
                return matches, errors, curr_page
            else:
                print('{}| ERROR Skipping peer {} due to failed query. cause: {}.'
                      .format(current_timestamp(), peer, response_json))
                errors.append({'username': peer, 'response': response_json})
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
                print(
                    '{}| WARN Eagerly terminating peer match queries! current_match_count: {}, current_page: {}, next_page: {}'
                        .format(current_timestamp(), str(len(matches)), str(curr_page), str(next_page)))
            return matches, errors, next_page
    else:
        if 'Site Error - 500x' in response_str or '404 - File or directory not found' in response_str:
            print('{}| Site error! Failed downloading peer data. peer: {}'
                  .format(current_timestamp(), peer))
            return matches, errors, None
        elif 'Access denied' in response_str or 'Checking if the site connection is secure' in response_str:
            rate_limited()
            return matches, errors, curr_page
        else:
            print('{}| Failed downloading matches for player {}'
                  .format(current_timestamp(), peer))
            time.sleep(5000000)
            reset_connection()
            return matches, errors, curr_page


def eagerly_terminate_match_queries(batch):
    for match in batch:
        if match['timestamp'].isnumeric() and datetime.datetime.fromtimestamp(match['timestamp']).year > 2020:
            return True
        elif match['timestamp'] > '2020-07-01T01:00:00+00:00':
            return True
        elif len(batch) < 10:
            return True


def extract_platform_user_id(profileUrl, platform):
    userId = profileUrl.split('/warzone/profile/{}/'.format(platform))[1]
    userId = userId.split('/overview')[0]
    return userId


def serialized_player(raw_player):
    metadata = raw_player['metadata']
    attributes = raw_player['attributes']
    player = {'username': attributes['platformUserIdentifier']}

    if 'platformSlug' in attributes:
        player['platform'] = attributes['platformSlug']
        player['platform_user_id'] = extract_platform_user_id(metadata['profileUrl'], player['platform'])

    return player


def download_peers():
    with open('../data/matches.raw.jsonl', 'r') as matches_file:
        with open('../data/peers.processed.jsonl', 'a') as peers_processed_file:
            with open('../data/peer_errors.jsonl', 'a') as errors_file:
                initialize_VPN(save=1, area_input=['complete rotation'])
                rotate_VPN()

                for match_str in matches_file.readlines():
                    match = json.loads(match_str)['data']

                    for segment in match['segments']:
                        player = serialized_player(segment)
                        peer_username = player['username']

                        if 'platform' in player and player['platform'] != 'atvi':
                            platform = player['platform']
                            peer_username = peer_username.strip()
                            platform_user_id = player['platform_user_id']

                            if peer_username in known_peers:
                                print("{}| Skipping known peer {}".format(current_timestamp(), peer_username))
                            else:
                                processed_matches, errors = fetch_peer(peer_username, platform_user_id, platform)

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
                        else:
                            print("{}| Skipping over player with an unknown platform"
                                  .format(current_timestamp(), peer_username))
                            errors_file.write('{}\n'.format(json.dumps({
                                'username': peer_username,
                                'matches': {
                                    "cause": "Peer with a null id",
                                    "player": player,
                                    "match_id": match['attributes']['id']
                                }
                            })))
    terminate_VPN()
    print('Done!')


download_peers()
