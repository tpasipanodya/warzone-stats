import json

delimiter = ','
header_row = [
    'Match Id',
    'Player Id',
    'Peer Id',
    'Peer KD',
    'Peer Level',
    'Peer Wins',
    'Player\'s Friend or Foe',
    'Peer SPM'
]


#
def reformat_all_matches():
    with open('../data/peers.csv', 'w') as matches_csv_file:
        matches_csv_file.write('{}\n'.format(delimiter.join(header_row)))

        matches_by_id = {}

        with open('../data/matches.processed.jsonl', 'r') as matches_jsonl_file:
            for match_str in matches_jsonl_file.readlines():
                match = json.loads(match_str)
                matches_by_id[match['id']] = match

        with open('../data/enriched_players.processed.jsonl', 'r') as players_file:
            for player_str in players_file.readlines():
                player = json.loads(player_str)
                index_matches(player)

                for match_id in player['matches'].keys():
                    enriched_match = matches_by_id.get(match_id, False)

                    if enriched_match:
                        for row in peer_rows(enriched_match, player):
                            matches_csv_file.write('{}\n'.format(row))


#
def peer_rows(match, player):
    return [peer_row(match['id'], player, peer) for peer in match['players']]


def peer_row(match_id, player, peer):
    peer_stats = peer['stats']
    lifetime_stats = peer['lifetime_stats']
    return '{},{},{},{},{},{},{},{}'.format(match_id,
                                            player['username'],
                                            peer['username'],
                                            peer_stats['kd_ratio'], # kd for this game. we'll need to calculate l
                                            friend_or_foe(match_id, player, peer),
                                            peer_stats['spm'])


def index_matches(player):
    indexed_matches = {}
    for match in player['matches']:
        indexed_matches[match['id']] = match
    player['matches'] = indexed_matches


def friend_or_foe(match_id, player, peer):
    match = player['matches'][match_id]
    teammate_ids = [teammate['id'] for teammate in match['teammates']]
    if peer['username'] in teammate_ids:
        return 'friend'
    else:
        return 'foe'


#
reformat_all_matches()
