import pandas as pd
import requests
import time
import logging
import json



def fetch_steam_game_data_json(json_path="games_data.json"):
    logging.info("Loading Steam game data from local JSON file...")

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            all_games = json.load(f)
            logging.info(f"Loaded {len(all_games)} records from JSON.")
            return all_games
    except Exception as e:
        logging.error(f"Failed to read {json_path}: {e}")
        return []
    



def fetch_steam_game_data(csv_path="steam_games.csv"):
    logging.info("Loading Steam games from CSV...")
    try:
        df = pd.read_csv(csv_path, sep=";")
    except Exception as e:
        logging.error(f"Failed to read {csv_path}: {e}")
        return []

    all_games = []
    timestamp = time.time()

    for idx, row in df.iterrows():
        appid = str(row["AppID"])
        store_url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
        players_url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={appid}"

        try:
            store_resp = requests.get(store_url).json()
            if not store_resp.get(appid) or not store_resp[appid]["success"]:
                logging.warning(f"[{appid}] Store data not available.")
                continue

            data = store_resp[appid]["data"]
            players_resp = requests.get(players_url).json()
            player_count = players_resp.get("response", {}).get("player_count", None)

            game_data = {
                "appid": appid,
                "name": data.get("name"),
                "type": data.get("type"),
                "developers": data.get("developers", []),
                "publishers": data.get("publishers", []),
                "release_date": data.get("release_date", {}).get("date"),
                "supported_languages": data.get("supported_languages"),
                "categories": [cat["description"] for cat in data.get("categories", [])],
                "genres": [genre["description"] for genre in data.get("genres", [])],
                "platforms": data.get("platforms"),
                "price": data.get("price_overview", {}).get("final_formatted") if "price_overview" in data else "Free",
                "tags": [tag["description"] for tag in data.get("genres", [])],  # genres as tags proxy
                "current_player_count": player_count,
                "timestamp": timestamp
            }

            all_games.append(game_data)
            logging.info(f"Collected: {game_data['name']}")

        except Exception as e:
            logging.error(f"Error processing AppID {appid}: {e}")

        time.sleep(0.1) 

    return all_games