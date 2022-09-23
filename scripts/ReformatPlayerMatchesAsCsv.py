import json

delimiter = ','
header_row = [
    'username',
    'id',
    'timestamp',
    'map',
    'mode',
    'kills',
    'score',
    'deaths',
    'kd_ratio',
    'longest_streak',
    'damage_done',
    'damage_taken',
    'damage_done_per_minute',
    'percent_time_moving',
    'executions',
    'nearmisses',
    'time_played',
    'assists',
    'spm',
    'headshots',
    'match_xp',
    'total_xp',
    'score_xp',
    'placement'
]

with open('../data/matches.csv', 'w') as matches_csv_file:
    matches_csv_file.write('{}\n'.format(delimiter.join(header_row)))

    with open('../data/enriched_players.processed.jsonl', 'r') as matches_jsonl_file:
        for player_json_str in matches_jsonl_file.readlines():
            player_json = json.loads(player_json_str)

            username = player_json['username']
            for match_json in player_json['matches']:
                row = [match_json[key] for key in header_row[1:-1]]
                row.insert(0, username)
                matches_csv_file.write('{}\n'.format(delimiter.join([str(entry) for entry in row])))

print("Done!")

