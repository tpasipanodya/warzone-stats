import json

delimiter = ','
header_row = [
    'Match Id',
    'Player Id',
    'Peer Id',
    'Peer KD',
    'Player\'s Friend or Foe'
]


#
def reformat_all_matches():
    with open('../data/peers.csv', 'w') as matches_csv_file:
        matches_csv_file.write('{}\n'.format(delimiter.join(header_row)))

        matches_by_id = {}
        enriched_peers_by_id = {}
        enriched_matches_by_id = {}

        with open('../data/peers.processed.jsonl') as enriched_peers_file:
            for enriched_peer_str in enriched_peers_file:
                enriched_peer = json.loads(enriched_peer_str)
                enriched_peers_by_id[enriched_peer['username']] = enriched_peer

        with open('../data/matches.processed.jsonl', 'r') as matches_jsonl_file:
            for match_str in matches_jsonl_file:
                match = json.loads(match_str)
                matches_by_id[match['id']] = match

        with open('../data/matches.raw.jsonl', 'r') as raw_matches_file:
            for match_str in raw_matches_file:
                match = json.loads(match_str)['data']
                enriched_matches_by_id[match['attributes']['id']] = match

        with open('../data/enriched_players.processed.jsonl', 'r') as players_file:
            for player_str in players_file:
                player = json.loads(player_str)
                index_matches(player)

                for match_id in player['matches'].keys():
                    if match_id in matches_by_id:
                        enriched_match = matches_by_id[match_id]
                        for row in peer_rows(enriched_match, player, enriched_peers_by_id, enriched_matches_by_id):
                            matches_csv_file.write('{}\n'.format(row))
                    else:
                        print("Unknown match #{} for player {}"
                              .format(match_id, player['username']))

    print("Done!")


#
def peer_rows(enriched_match, player, enriched_peers_by_id, enriched_matches_by_id):
    return [peer_row(enriched_match['id'], player, peer, enriched_peers_by_id, enriched_matches_by_id)
            for peer in enriched_match['players']]


def peer_row(match_id, player, peer, enriched_peers_by_id, enriched_matches_by_id):
    return '{},{},{},{},{}'\
        .format(match_id,
                player['username'],
                peer['username'],
                peer_kd_for_match(peer, enriched_peers_by_id, match_id),
                friend_or_foe(match_id, player, peer, enriched_matches_by_id))


def peer_kd_for_match(peer, enriched_peers_by_id, match_id):
    peer_username = peer['username']
    if peer_username in enriched_peers_by_id:
        enriched_peer = enriched_peers_by_id[peer_username]
        kills = 1.0
        deaths = 1.0
        match_found = False

        if peer_username == 'DJ_GILS':
            print(peer_username)

        for match in reversed(enriched_peer['matches']):
            kills += float(match.get('kills', 0))
            deaths += float(match.get('deaths', 0))

            if match['id'].strip() == match_id.strip():
                match_found = True
                break

        if not match_found:
            print("PEER_MATCH_NOT_FOUND. peer: {}, match: {}"
                  .format(peer_username, match_id))
        else:
            print("PEER_MATCH_FOUND. peer: {}, match: {}"
                  .format(peer_username, match_id))
        return kills / max(deaths, 1)
    else:
        print("PEER_NOT_FOUND. peer: {}"
              .format(peer_username))
        return 0


def index_matches(player):
    indexed_matches = {}
    for match in player['matches']:
        indexed_matches[match['id']] = match
    player['matches'] = indexed_matches


def friend_or_foe(match_id, player, peer, enriched_matches_by_id):
    peer_id = peer['username']
    player_id = player['username']
    match = enriched_matches_by_id[match_id]
    players = match['segments']

    if peer_id == 'DJ_GILS':
        print(peer_id)

    if len(list(filter_for_raw_player(players, player_id))) > 0 and len(list(filter_for_raw_player(players, peer_id))) > 0:
        raw_player = list(filter_for_raw_player(players, player_id))[0]
        raw_peer = list(filter_for_raw_player(players, peer_id))[0]
        if raw_player['attributes']['team'] == raw_peer['attributes']['team']:
            return 'friend'
        else:
            return 'foe'
    else:
        return 'foe'


def filter_for_raw_player(players, player_id):
    return filter(lambda in_game_player: in_game_player['attributes']['platformUserIdentifier'] == player_id or
                                  in_game_player['metadata']['platformUserHandle'] == player_id,
           players)


#
reformat_all_matches()
