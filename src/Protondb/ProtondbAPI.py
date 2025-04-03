#!/usr/bin/env python3

import requests
import csv

BASE_URL = "http://localhost:8080/api"

def get_game_summary(game_id):
    """Get ProtonDB summary for a game using its ID"""
    url = f"{BASE_URL}/games/{game_id}/summary"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def process_csv(input_file, output_file):
    """Read a CSV file with game names and Steam IDs, query summaries, and write results to a new CSV file"""
    with open(input_file, newline='', encoding='utf-8') as csvfile, \
         open(output_file, mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(csvfile)
        fieldnames = [
            "Name", "AppID",
            "protonBestReportedTier", "protonConfidence", "protonScore",
            "protonTier", "protonTotal", "protonTrendingTier"
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            game_name = row["Name"]
            game_id = row["AppID"]
            print(f"\nFetching data for {game_name} (ID: {game_id})...")

            game_summary = get_game_summary(game_id) or {}

            writer.writerow({
                "Name": game_name,
                "AppID": game_id,
                "protonBestReportedTier": game_summary.get("bestReportedTier", ""),
                "protonConfidence": game_summary.get("confidence", ""),
                "protonScore": game_summary.get("score", ""),
                "protonTier": game_summary.get("tier", ""),
                "protonTotal": game_summary.get("total", ""),
                "protonTrendingTier": game_summary.get("trendingTier", "")
            })

if __name__ == "__main__":
    input_csv = "steam_games.csv"
    output_csv = "proton_results.csv"
    process_csv(input_csv, output_csv)
    print(f"\nResults written to {output_csv}")
