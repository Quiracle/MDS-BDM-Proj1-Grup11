import os
import requests
import csv
import json
import random
import logging
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta


def _load_api_key():
    """
    Load API key from .env file in the parent directory.
    """
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    load_dotenv()
    API_KEY = os.getenv('YOUTUBE_API_KEY')
    return API_KEY

def _fetch_video_data(keyword, pafter, pbefore, api_key, max_results=50):
    try:
        published_after = pafter.strftime("%Y-%m-%dT00:00:00Z")
        published_before = pbefore.strftime("%Y-%m-%dT00:00:00Z")
        
        search_url = "https://www.googleapis.com/youtube/v3/search"
        search_params = {
            "part": "snippet",
            "q": keyword,
            "type": "video",
            "maxResults": max_results,
            "order": "viewCount",
            "publishedAfter": published_after,
            "publishedBefore": published_before,
            "key": api_key
        }
        
        response = requests.get(search_url, params=search_params)
        response.raise_for_status()  # Raises exception for 4XX/5XX status
        
        video_ids = [item["id"]["videoId"] for item in response.json().get("items", [])]
        
        if not video_ids:
            return 0
        
        stats_url = "https://www.googleapis.com/youtube/v3/videos"
        stats_params = {
            "part": "statistics",
            "id": ",".join(video_ids),
            "key": api_key
        }
        
        response = requests.get(stats_url, params=stats_params)
        response.raise_for_status()
        
        total_views = sum(int(item["statistics"].get("viewCount", 0)) 
                      for item in response.json().get("items", []))
        
        return total_views
    except Exception as e:
        logging.warning(f"Failed to fetch YouTube data for '{keyword}': API key usage exceeded or other error – Randomizing data")
        return 0


def update_views():
    '''
    Updates the youtube_views.json file to store the sum of the top 50 videos' views 
    of the games listed in games.csv. If the game already exists, it just updates the two last months.

    The 'randomized' flag is now stored at the game level instead of for each time entry.
    If any API call for a game fails, the entire game's data will be randomized.
    '''

    API_KEY = _load_api_key()

    # Define path relative to this script’s location
    base_dir = os.path.dirname(__file__)  # just this repo

    # Ensure JSON file will be saved here
    output_file = os.path.join(base_dir, "youtube_views.json")
    os.makedirs(base_dir, exist_ok=True)  # just in case

    existing_data = {}
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)

    views = {}

    # Load games.csv from the same directory
    games_file = os.path.join(base_dir, "youtube-games.csv")
    with open(games_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        keywords = [row[1] for row in list(reader)[1:] if len(row) > 1]
    
    today = datetime.now()
    current_month_start = today.replace(day=1)
    last_month_start = (today - relativedelta(months=1)).replace(day=1)
    last_month_end = current_month_start - relativedelta(days=1)

    for keyword in keywords:
        should_randomize = False
        entries = []
        
        # Check if we need to refetch all data
        need_full_refresh = True
        if keyword in existing_data:
            # Only do a partial refresh if the game exists and wasn't previously randomized
            if not existing_data[keyword].get("randomized", False):
                need_full_refresh = False
        
        if need_full_refresh:
            # Fetch all months of data
            months = []
            for i in range(12):
                first_day = (today - relativedelta(months=i)).replace(day=1)
                last_day = (first_day + relativedelta(months=1)) - relativedelta(days=1)
                months.append((first_day, last_day))
            #months.insert(0, (current_month_start, today))
            
            for after, before in months:
                total_views = _fetch_video_data(keyword, after, before, API_KEY)
                if total_views == 0:
                    should_randomize = True
                    total_views = random.randint(0, 10000000)
                
                entries.append({
                    "after": after.strftime("%Y-%m-%d"),
                    "before": before.strftime("%Y-%m-%d"),
                    "total_views": total_views
                })
                print(f"{'Randomized' if should_randomize else 'Updated'} views for '{keyword}' from {after.date()} to {before.date()}: {total_views:,} views")
        else:
            # Keep existing entries
            for entry in existing_data[keyword].get("entries", []):
                entries.append({
                    "after": entry["after"],
                    "before": entry["before"],
                    "total_views": entry["total_views"]
                })
            
            # Update only the last month and current month
            for after, before in [(last_month_start, last_month_end), (current_month_start, today)]:
                # Check if entry for this period already exists
                period_exists = any(
                    entry["after"] == after.strftime("%Y-%m-%d") and entry["before"] == before.strftime("%Y-%m-%d")
                    for entry in entries
                )
                
                # Only add if the period doesn't already exist
                if not period_exists:
                    total_views = _fetch_video_data(keyword, after, before, API_KEY)
                    if total_views == 0:
                        should_randomize = True
                        total_views = random.randint(0, 10000000)
                    
                    entries.append({
                        "after": after.strftime("%Y-%m-%d"),
                        "before": before.strftime("%Y-%m-%d"),
                        "total_views": total_views
                    })
                    print(f"{'Randomized' if should_randomize else 'Updated'} views for '{keyword}' from {after.date()} to {before.date()}: {total_views:,} views")
        
        # Store the entries and randomized flag at the game level
        views[keyword] = {
            "randomized": should_randomize,
            "entries": entries
        }

    # Save updated data to JSON file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(views, f, indent=4)
    
    print(f"Data saved to {output_file}")


if __name__ == '__main__':
    update_views()