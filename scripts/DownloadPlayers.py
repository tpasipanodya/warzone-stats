import json
import time
import urllib.request

base_url = "https://www.callofduty.com/api/papi-client/leaderboards/v2/title/mw/platform/psn/time/alltime/type/core" \
           "/mode/career/page/"

with open("players.jsonl", "w") as file:
    page = 1
    error_count = 0
    player_count = 0

    while player_count < 20_000:
        url = base_url + str(page)

        try:
            response = json.loads(urllib.request.urlopen(url).read().decode())

            if response["status"] != "error":
                columns = response["data"]["columns"]
                file.write(",".join(columns) + "\n")

                players = response["data"]["entries"]
                for player in players:
                    file.write(json.dumps(player) + "\n")
                    player_count += 1

                print("downloaded " + str(player_count) + " players")
                time.sleep(0.5)
                page += 1
            else:
                error_count += 1
                print("Errors_so_far: {}. Failed loading players on page {}. Response: {}".format(str(error_count), str(page), response))
        except Exception as e:
            error_count += 1
            print("Total error count: {}. Failed loading players on page {} due to error {}".format(str(error_count), str(page), str(e)))
