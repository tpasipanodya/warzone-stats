import json

delimiter = ','
header_row = [
    'username',
    'id',
    'timestamp',
    'map',
    'mode',
    'kills',
    'score'
]


def reformat_all_matches():
    with open('../data/matches.csv', 'w') as matches_csv_file:
        matches_csv_file.write('{}\n'.format(delimiter.join(header_row)))

        with open('') as matches_jsonl_file:
            for match_str in matches_jsonl_file.readlines():
                match_json = json.loads(match_str)

                row = reformat_match(match_json)
                matches_csv_file.write('{}\n'.format(row))


def reformat_match(match_json):
    return '{},{},{}'


reformat_all_matches()
