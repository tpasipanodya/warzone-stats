import json

with open('../data/peers.processed.jsonl', 'r') as peers_processed_file:
    with open('../data/peers.2.processed.jsonl', 'a') as filtered_peers_processed_file:
        for peer_str in peers_processed_file.readlines():
            peer_json = json.loads(peer_str)

            if len(peer_json['matches']) > 0:
                filtered_peers_processed_file.write('{}'.format(peer_str))
