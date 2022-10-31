import json
import time
import datetime
from selenium import webdriver
from random import randrange
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN

browser = webdriver.Chrome()
base_url = 'https://api.tracker.gg/api/v2/warzone/standard/matches/{}'

opening_response_tag = '<html><head><meta name="color-scheme" content="light dark"></head><body><pre style="word-wrap: break-word; white-space: pre-wrap;">'
closing_response_tag = '</pre></body></html>'
matches = []
errors = []

known_match_ids = set()
try:
    with open('../data/matches.processed.jsonl', 'r') as matches_file:
        for entry in matches_file.readlines():
            if entry.startswith('{'):
                parsed_entry = json.loads(entry)
                known_match_ids.add(parsed_entry['id'].strip())
except Exception as e:
    print('Failed loading existing matches: {}'.format(str(e)))


def current_timestamp():
    return datetime.datetime.now().isoformat()


def serialized_player(segment):
    stats = segment['stats']
    metadata = segment['metadata']
    attributes = segment['attributes']
    lifetime_stats = attributes.get('lifeTimeStats', {})
    return {
        'username': attributes['platformUserIdentifier'],
        'team': attributes['team'],
        'placement': metadata.get('placement', {}).get('value'),
        'stats': {
            'kills': stats.get('kills', {}).get('value', 0),
            'medal_xp': stats.get('medalXp', {}).get('value', 0),
            'match_xp': stats.get('matchXp', {}).get('value', 0),
            'score_xp': stats.get('scoreXp', {}).get('value', 0),
            'wall_bangs': stats.get('wallBangs', {}).get('value', 0),
            'score': stats.get('score', {}).get('value', 0),
            'total_xp': stats.get('totalXp', {}).get('value', 0),
            'headshots': stats.get('headshots', {}).get('value', 0),
            'assists': stats.get('assists', {}).get('value', 0),
            'challenge_xp': stats.get('challengeXp', {}).get('value', 0),
            'spm': stats.get('scorePerMinute', {}).get('value', 0),
            'distance_traveled': stats.get('distanceTraveled', {}).get('value', 0),
            'team_survival_time': stats.get('teamSurvivalTime', {}).get('value', 0),
            'deaths': stats.get('deaths', {}).get('value', 0),
            'kd_ratio': stats.get('kdRatio', {}).get('value', 0),
            'bonus_xp': stats.get('bonusXp', {}).get('value', 0),
            'gulag_deaths': stats.get('gulagDeaths', {}).get('value', 0),
            'time_played': stats.get('timePlayed', {}).get('value', 0),
            'executions': stats.get('executions', {}).get('value', 0),
            'gulag_kills': stats.get('gulagKills', {}).get('value', 0),
            'nearmisses': stats.get('nearmisses', {}).get('value', 0),
            'percent_time_moving': stats.get('percentTimeMoving', {}).get('value', 0),
            'misc_xp': stats.get('miscXp', {}).get('value', 0),
            'longest_streak': stats.get('longestStreak', {}).get('value', 0),
            'team_placement': stats.get('teamPlacement', {}).get('value', 0),
            'damage_done': stats.get('damageDone', {}).get('value', 0),
            'damage_taken': stats.get('damageTaken', {}).get('value', 0),
            'damage_done_per_minute': stats.get('damageDonePerMinute', {}).get('value', 0),
            'damage_ratio': stats.get('damageRatio', {}).get('value', 0)
        },
        'lifetime_stats': {
            'kd_ratio': lifetime_stats.get('kdRatio', 0),
            'level': lifetime_stats.get('level', 0),
            'kills': lifetime_stats.get('kills', 0),
            'deaths': lifetime_stats.get('deaths', 0),
            'games_played': lifetime_stats.get('gamesPlayed', 0),
            'top5': lifetime_stats.get('top5', 0),
            'wins': lifetime_stats.get('wins', 0)
        }
    }


def download_matches_for_players():
    with open('../data/matches.processed.jsonl', 'a') as processed_matches_file:
        with open('../data/matches.raw.jsonl', 'a') as raw_matches_file:
            with open('../data/player_match_errors.jsonl', 'a') as match_errors_file:
                with open('../data/enriched_players.processed.jsonl', 'r') as players_file:
                    initialize_VPN(save=1, area_input=['complete rotation'])
                    rotate_VPN()

                    for player_entry in players_file.readlines():
                        parsed_player = json.loads(player_entry)
                        print('{}| Downloading matches for player {}'
                              .format(current_timestamp(),
                                      parsed_player['username']))

                        for match in parsed_player['matches']:
                            match_id = match['id'].strip()
                            if match_id in known_match_ids:
                                print('{}| Skipped match {}'.format(current_timestamp(), match_id))
                            else:
                                print('{}| Processing match {}'.format(current_timestamp(), match_id))
                                time.sleep(3 + randrange(2))
                                url = base_url.format(match_id)
                                browser.get(url)

                                raw_response = browser.page_source
                                response_json = raw_response.replace(opening_response_tag, '')
                                response_json = response_json.replace(closing_response_tag, '')

                                if response_json.startswith('{') and response_json.endswith('}'):
                                    response_json = json.loads(response_json)
                                    if 'errors' not in response_json:
                                        data = response_json['data']
                                        metadata = data['metadata']
                                        segments = data['segments']

                                        known_match_ids.add(match_id)
                                        raw_matches_file.write('{}\n'.format(json.dumps(response_json)))
                                        processed_matches_file.write('{}\n'.format(json.dumps({
                                            'id': match_id.strip(),
                                            'map': metadata['mapName'],
                                            'mode': metadata['modeName'],
                                            'stats': {
                                                'duration_ms': metadata['duration'],
                                                'timestamp': metadata['timestamp'],
                                                'player_count': metadata['playerCount'],
                                                'team_count': metadata['teamCount'],
                                            },
                                            'players': [serialized_player(segment) for segment in segments]
                                        })))
                                    else:
                                        errors = response_json['errors']

                                        if any(error['code'] == 'RateLimited' for error in errors):
                                            print('{}| Rate Limited. Sleeping for 5 minutes..'
                                                  .format(current_timestamp()))
                                            time.sleep(300)
                                            rotate_VPN()
                                            print('{}| Resuming...'.format(current_timestamp()))
                                        else:
                                            match_errors_file.write('{}\n'.format(json.dumps({
                                                'id': match_id,
                                                'response': raw_response
                                            })))
                                else:
                                    error_message = 'Failed querying for match {}'.format(match_id)
                                    print('{}| {}'.format(current_timestamp(), error_message))
                                    match_errors_file.write('{}\n'.format(json.dumps({
                                        'id': match_id,
                                        'response': raw_response
                                    })))
                                    rotate_VPN()
                                    raise Exception(error_message)
                    terminate_VPN()


download_matches_for_players()
