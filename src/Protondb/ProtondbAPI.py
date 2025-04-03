#!/usr/bin/env python3

import requests
import json

BASE_URL = "http://proton:8080/api"

def get_game_summary(game_id):
    """Get ProtonDB summary for a game using its ID"""
    url = f"{BASE_URL}/games/{game_id}/summary"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def process_json(input_file, output_file):
    """Read a JSON file with game names and Steam IDs, query summaries, and write results to a new JSON file"""
    with open(input_file, 'r', encoding='utf-8') as infile:
        games = json.load(infile)

    results = []
    for game in games:
        game_name = game.get("Name")
        game_id = game.get("AppID")
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

    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(results, outfile, indent=4)
        print(f"\nResults written to {output_file}")

if __name__ == "__main__":
    input_json = "../../landing_zone/steam_games.json"
    output_json = "proton_results.json"
    process_json(input_json, output_json)
