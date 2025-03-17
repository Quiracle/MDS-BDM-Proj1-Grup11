#!/usr/bin/env python3

import requests
import csv

BASE_URL = "http://localhost:8080/api"

def get_game_by_id(game_id):
    """Get details of a game using its ID"""
    """BUG: Returns None?"""
    url = f"{BASE_URL}/games/{game_id}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def get_game_summary(game_id):
    """Get ProtonDB summary for a game using its ID"""
    url = f"{BASE_URL}/games/{game_id}/summary"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def process_csv(file_path):
    """Read a CSV file with game names and Steam IDs, then do some queries for each"""
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            game_name = row["Game Name"]
            game_id = row["Steam ID"]
            print(f"\nFetching data for {game_name} (ID: {game_id})...")

            game_details = get_game_by_id(game_id)
            game_summary = get_game_summary(game_id)

            print(f"Game Details: {game_details}")
            print(f"Game Summary: {game_summary}")

if __name__ == "__main__":
    csv_file = "steam_games.csv" # TODO: Add the actual CSV
    process_csv(csv_file)
