import json
import datetime
from random import randrange


def log(message):
    print("{}| {}".format(current_timestamp(), message))


def current_timestamp():
    return datetime.datetime.now().isoformat()


peer_ids = set()

with open('../data/peers.processed.jsonl', 'r') as peers_file:
    for peer_str in peers_file.readlines():
        if peer_str.startswith('{'):
            peer_json = json.loads(peer_str)
            if len(peer_json['matches']) > 0:
                peer_ids.add(peer_json['username'].strip())

with open('../data/sampled_players.raw.jsonl', 'r') as players_raw_file:
    for player_str in players_raw_file:
        player_json = json.loads(player_str)
        peer_ids.add(player_json['id'])


def resample_players(n=5):
    player_ids = list(peer_ids)
    player_ids.sort()

    i = 0
    while i < n:
        i += 1
        player_index = randrange(0, len(player_ids))
        player_id = player_ids[player_index]
        log('Sampled player \'{}\''.format(player_id))

        with open('../data/sampled_players.processed.jsonl', 'a') as players_processed_file:
            players_processed_file.write('{}\n'.format(json.dumps({'id': player_id})))
    log('Done!')


resample_players()
