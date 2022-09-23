import json
import time
from selenium import webdriver
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN

browser = webdriver.Chrome()
base_url = 'https://api.tracker.gg/api/v2/warzone/standard/matches/psn/{}?type=wz'
opening_response_tag = '<html><head><meta name="color-scheme" content="light dark"></head><body><pre style="word-wrap: break-word; white-space: pre-wrap;">'
closing_response_tag = '</pre></body></html>'


# Method for loading, parsing and extracting matches for a given
# player given a URL.
def fetch_matches(query_url):
    browser.get(query_url)

    response_html = browser.page_source
    response_str = response_html.replace(opening_response_tag, '')
    response_str = response_str.replace(closing_response_tag, '')

    raw_matches = []
    matches = []
    errors = []

    if response_str.startswith('{') and response_str.endswith('}'):
        response_json = json.loads(response_str)

        if 'errors' in response_json:
            if any(error['code'] == 'RateLimited' for error in response_json['errors']):
                print('Rate Limited! Sleeping 5 minutes...')
                time.sleep(300)
                rotate_VPN()
            else:
                print('ERROR player: {}. cause: {}'.format(peer_username, response_json))
                errors.append({
                    'username': peer_username,
                    'response': response_json
                })
        else:
            data = response_json['data']
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
                raw_matches.append(match)
    else:
        print('Failed downloading matches for player {}'.format(peer_username))
        errors.append({
            'username': peer_username,
            'response': response_html
        })
    return raw_matches, matches, errors


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


# Some matches cannot be resolved
with open('../data/player_matches.processed.jsonl', 'r') as matches_file:
    with open('../data/peers.processed.jsonl', 'a') as peers_processed_file:
        with open('../data/peers.raw.jsonl', 'a') as peers_raw_file:
            with open('../data/peer_errors.jsonl', 'a') as error_file:
                initialize_VPN(save=1, area_input=['complete rotation'])

                for match_str in matches_file.readlines():
                    if match_str.startswith('{'):
                        match = json.loads(match_str)
                        for player in match['players']:
                            peer_username = player['username'].strip()

                            if peer_username in known_peers:
                                print("Skipping peer {}".format(peer_username))
                            else:
                                query_url = base_url.format(peer_username)
                                raw_fetched_matches, fetched_matches, errors = fetch_matches(query_url)

                                match_count = 0
                                raw_matches = []
                                full_errors = []
                                processed_matches = []
                                prev_last_match_id = None

                                while len(fetched_matches) > 0:
                                    for match in fetched_matches:
                                        match_count += 1
                                        processed_matches.append(match)

                                    for match in raw_fetched_matches:
                                        raw_matches.append(match)

                                    for error in errors:
                                        full_errors.append(error)

                                    last_match = processed_matches[-1]
                                    last_match_id = last_match['id']
                                    last_timestamp = last_match['timestamp']

                                    if last_match_id == prev_last_match_id:
                                        break

                                    paged_query_url = query_url + '&next=' + last_timestamp
                                    raw_fetched_matches, fetched_matches, errors = fetch_matches(paged_query_url)
                                    prev_last_match_id = last_match_id

                                    peers_raw_file.write('{}\n'.format(json.dumps({
                                        'username': peer_username,
                                        'matches': raw_matches
                                    })))
                                    peers_processed_file.write('{}\n'.format(json.dumps({
                                        'username': peer_username,
                                        'matches': processed_matches
                                    })))
                                    error_file.write(json.dumps(full_errors))
                                    known_peers.add(peer_username)
                                    print('Processed {} matches for player {}'
                                          .format(str(match_count), peer_username))

                                else:
                                    print('Skipped player {}'.format(peer_username))


print('Done!')


