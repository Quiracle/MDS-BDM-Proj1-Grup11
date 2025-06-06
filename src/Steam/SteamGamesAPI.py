import pandas as pd
import requests
import time
import logging
import json
import os


def fetch_steam_game_data_json(json_path="src/Steam/games_data.json"):
    """
    Loads Steam game data from a local JSON file and standardizes keys and types
    to match the expected schema of the DeltaWriter.
    """
    logging.info(f"Loading Steam game data from local JSON file: {json_path}")

    # Ensure the path is correct for the execution environment
    absolute_json_path = os.path.abspath(json_path) 
    
    try:
        with open(absolute_json_path, "r", encoding="utf-8") as f:
            raw_games = json.load(f)
            logging.info(f"Loaded {len(raw_games)} raw records from JSON.")
            
            processed_games = []
            current_timestamp = time.time() # Use a single timestamp for this batch
            
            for game in raw_games:
                # --- START OF MINIMAL CHANGES ---
                # Standardize keys from JSON (e.g., "Name" to "name")
                # and handle specific type conversions (e.g., platforms booleans to strings).
                processed_game = {
                    "appid": game.get("AppID"),
                    "name": game.get("Name"), # Renamed from 'Name' to 'name'
                    "type": game.get("Type"),
                    "developers": game.get("Developers", []),
                    "publishers": game.get("Publishers", []),
                    "release_date": game.get("Release Date"), # Renamed from 'Release Date'
                    "supported_languages": game.get("Supported Languages"), # Renamed from 'Supported Languages'
                    "categories": game.get("Categories", []),
                    "genres": game.get("Genres", []),
                    # Convert boolean platform values to strings to match MapType(StringType(), StringType())
                    "platforms": {k: str(v) for k, v in game.get("Platforms", {}).items()},
                    "price": game.get("Price", "Free"), # Renamed from 'Price'
                    "tags": game.get("Tags", []), # Renamed from 'Tags'
                    "current_player_count": game.get("Current Player Count"), # Renamed from 'Current Player Count'
                    "timestamp": current_timestamp # Added timestamp as expected by the Delta schema
                }
                
                # Add an immediate check for null/empty name to filter out bad records early
                if not processed_game.get("name") or str(processed_game["name"]).strip() == "":
                    logging.warning(f"Skipping game with AppID {processed_game.get('appid')} due to null or empty 'name' after JSON load and key standardization.")
                    continue # Skip this record if name is invalid

                processed_games.append(processed_game)
                # --- END OF MINIMAL CHANGES ---
            
            logging.info(f"Processed {len(processed_games)} records for Delta Lake after key standardization and initial name check.")
            return processed_games
            
    except FileNotFoundError:
        logging.error(f"JSON file not found at {absolute_json_path}. Please check the path and ensure it's accessible from the Airflow worker.")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {absolute_json_path}: {e}. Ensure the JSON is well-formed.")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading or processing {absolute_json_path}: {e}")
        return []
    



def fetch_steam_game_data(csv_path="src/Steam/steam_games.csv"):
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