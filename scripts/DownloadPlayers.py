import json
import time
import datetime
from selenium import webdriver
from urllib import parse
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN

browser = webdriver.Chrome()
base_url = 'https://api.tracker.gg/api/v2/warzone/standard/matches/atvi/{}?type=wz'
opening_response_tag = '<html><head><meta name="color-scheme" content="light dark"></head><body><pre style="word-wrap: break-word; white-space: pre-wrap;">'
closing_response_tag = '</pre></body></html>'


# Load a list of player ids we've already downloaded matches for.
known_players = set()
try:
    with open('../data/enriched_players.processed.jsonl', 'r') as matches_file:
        for entry in matches_file.readlines():
            if entry.startswith('{'):
                parsed_entry = json.loads(entry)
                known_players.add(parsed_entry['username'].strip())
except Exception as e:
    print('Failed loading existing matches: {}'.format(str(e)))


def current_timestamp():
    return datetime.datetime.now().isoformat()


def download_players():
    # Load matches for all players we haven't done so for already.
    with open('../data/sampled_players.processed.jsonl', 'r') as sampled_players:
        with open('../data/enriched_players.processed.jsonl', 'a') as processed_file:
            with open('../data/enriched_players.raw.jsonl', 'a') as raw_file:
                with open('../data/enriched_player_errors.jsonl', 'a') as error_file:
                    initialize_VPN(save=1, area_input=['complete rotation'])

                    for player_json_str in sampled_players.readlines():
                        player_json = json.loads(player_json_str)
                        username = player_json['id']

                        if username not in known_players:
                            print('{}| Processing {}'.format(current_timestamp(), username))
                            query_url = base_url.format(parse.quote_plus(username))
                            raw_fetched_matches, fetched_matches, errors, next_page = fetch_matches(query_url, username)

                            match_count = 0
                            raw_matches = []
                            full_errors = []
                            processed_matches = []

                            while len(fetched_matches) > 0 or next_page:
                                print('{}| Player: {}, fetched_matches: {}, next_page: {}'
                                      .format(current_timestamp(),
                                              username,
                                              str(len(fetched_matches)),
                                              next_page))

                                for match in fetched_matches:
                                    match_count += 1
                                    processed_matches.append(match)

                                for match in raw_fetched_matches:
                                    raw_matches.append(match)

                                for error in errors:
                                    full_errors.append(error)

                                paged_query_url = '{}{}{}'.format(query_url, '&next=', parse.quote_plus(str(next_page)))
                                raw_fetched_matches, fetched_matches, errors, next_page = fetch_matches(paged_query_url, username)

                            raw_file.write('{}\n'.format(json.dumps({
                                'username': username,
                                'matches': raw_matches
                            })))

                            processed_file.write('{}\n'.format(json.dumps({
                                'username': username,
                                'matches': processed_matches
                            })))

                            error_file.write(json.dumps(full_errors))
                            print('Processed {} matches for player {}'
                                  .format(str(match_count), username))
                        else:
                            print('Skipped player {}'.format(username))
                    terminate_VPN()


# Method for loading, parsing and extracting matches for a given
# player given a URL.
def fetch_matches(query_url, username):
    browser.get(query_url)

    response_html = browser.page_source
    response_str = response_html.replace(opening_response_tag, '')
    response_str = response_str.replace(closing_response_tag, '')

    matches = []
    errors = []
    next_page = None
    raw_matches = []

    if response_str.startswith('{') and response_str.endswith('}'):
        response_json = json.loads(response_str)

        if 'errors' in response_json:
            if any(error['code'] == 'RateLimited' for error in response_json['errors']):
                print('Rate Limited! Sleeping 10 minutes...')
                time.sleep(300)
                rotate_VPN()
                print('Resuming...')
            else:
                print('ERROR player: {}. cause: {}'.format(username, response_json))
                errors.append({
                    'username': username,
                    'response': response_json
                })
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
                raw_matches.append(match)
    else:
        print('Failed downloading matches for player {}'.format(username))
        errors.append({
            'username': username,
            'response': response_html
        })
    return raw_matches, matches, errors, next_page


download_players()







