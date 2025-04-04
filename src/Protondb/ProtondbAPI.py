#!/usr/bin/env python3

import requests
import json
from delta_writer import DeltaWriter

BASE_URL = "http://proton:3000/api"
delta_writer = DeltaWriter()

def get_game_summary(game_id, retries=1, delay=0.3):
    """Get ProtonDB summary for a game using its ID, with basic retry logic"""
    import time
    url = f"{BASE_URL}/games/{game_id}/summary"
    for attempt in range(retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"[Attempt {attempt+1}] Error {response.status_code}: {response.text}")
        except requests.RequestException as e:
            print(f"[Attempt {attempt+1}] Request failed: {e}")
        time.sleep(delay)
    return None

def process_json(input_file, delta_writer):

    """Read a JSON file with game names and Steam IDs, query summaries, and write results to Delta Lake"""
    with open(input_file, 'r', encoding='utf-8') as infile:
        games = json.load(infile)

    results = []
    for game in games:
        game_name = game.get("Name")
        game_id = game.get("AppID")

        if not game_name or not game_id:
            print(f"Skipping invalid entry: {game}")
            continue

        print(f"\nFetching data for {game_name} (ID: {game_id})...")
        game_summary = get_game_summary(game_id) or {}

        result = {
            "Name": game_name,
            "AppID": game_id,
            "protonBestReportedTier": game_summary.get("bestReportedTier", ""),
            "protonConfidence": game_summary.get("confidence", ""),
            "protonScore": game_summary.get("score", ""),
            "protonTier": game_summary.get("tier", ""),
            "protonTotal": game_summary.get("total", ""),
            "protonTrendingTier": game_summary.get("trendingTier", "")
        }
        results.append(result)

    delta_writer.write_records(results)

if __name__ == "__main__":
    input_json = "../../landing_zone/steam_games.json"

    writer = DeltaWriter()
    process_json(input_json, writer)
