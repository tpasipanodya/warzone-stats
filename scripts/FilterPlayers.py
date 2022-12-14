import json

with open('../data/enriched_players.processed.jsonl', 'r') as players_processed_file:
    with open('../data/enriched_players.2.processed.jsonl', 'a') as filtered_players_processed_file:
        for player_str in players_processed_file:
            player_json = json.loads(player_str)

            if len(player_json['matches']) > 0:
                filtered_players_processed_file.write('{}'.format(player_str))
