import requests
import time
import json
import pandas as pd
import logging

# Optional: configure logging level here
logging.basicConfig(level=logging.INFO)

API_KEY = " MIRA MIRA"

PLAYER_SUMMARY_URL = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
STEAM_LEVEL_URL = "https://api.steampowered.com/IPlayerService/GetSteamLevel/v1/"
OWNED_GAMES_URL = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/"




def fetch_steam_user_data_json(json_path="steam_users.json"):
    logging.info(f" Loading Steam user data from JSON file: {json_path}")
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            logging.info(f"Loaded {len(data)} records from JSON.")
            return data
    except Exception as e:
        logging.error(f"Failed to read or parse JSON: {e}")
        return []

def safe_request(url, params, retries=5, sleep_time=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(sleep_time)
    return None


def get_player_summaries(steam_ids):
    params = {"key": API_KEY, "steamids": ",".join(steam_ids)}
    data = safe_request(PLAYER_SUMMARY_URL, params)
    return data.get("response", {}).get("players", []) if data else []

def get_steam_level(steam_id):
    params = {"key": API_KEY, "steamid": steam_id}
    data = safe_request(STEAM_LEVEL_URL, params)
    return data.get("response", {}).get("player_level", 0) if data else 0

def get_owned_games(steam_id):
    params = {"key": API_KEY, "steamid": steam_id, "include_appinfo": "true"}
    data = safe_request(OWNED_GAMES_URL, params)
    if data and "response" in data and "games" in data["response"]:
        return data["response"]["games"]
    return []

# Option 1: Load from a list of IDs in a CSV and fetch via API
def fetch_steam_user_data(csv_path="steam_users.csv", delay=0.1):
    logging.info(f" Loading Steam IDs from CSV: {csv_path}")

    try:
        df_ids = pd.read_csv(csv_path)
        steam_ids = df_ids["steam_id"].astype(str).tolist()
    except Exception as e:
        logging.error(f" Failed to read CSV: {e}")
        return []

    return _fetch_user_data_by_ids(steam_ids, delay=delay)




def _fetch_user_data_by_ids(steam_ids, delay=0.1):
    user_data = []

    for idx, steam_id in enumerate(steam_ids, start=1):
        logging.info(f"Processing user {idx}/{len(steam_ids)} - {steam_id}")

        try:
            users = get_player_summaries([steam_id])
            if not users:
                continue

            user = users[0]
            username = user.get("personaname", "Unknown")
            country = user.get("loccountrycode", "N/A")
            profile_url = user.get("profileurl", "N/A")

            steam_level = get_steam_level(steam_id)
            owned_games = get_owned_games(steam_id)


            if isinstance(owned_games, list):
                for game in owned_games:
                    for k, v in game.items():
                        if not isinstance(v, str):
                            game[k] = str(v) if v is not None else ""
            else:
                owned_games = []

            user_data.append({
                "SteamID": steam_id,
                "Username": username,
                "Country": country,
                "Steam_Level": steam_level,
                "Owned_Games": owned_games,
                "Profile_URL": profile_url,
            })

        except Exception as e:
            logging.error(f"Error processing SteamID {steam_id}: {e}")

        time.sleep(delay)

    logging.info(f"Finished {len(user_data)} users.")
    return user_data
