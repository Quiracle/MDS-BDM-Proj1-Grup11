import os
import requests
import csv
import json
import random
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta


def _load_api_key():
    """
    Load API key from .env file in the parent directory.
    """
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))
    return os.getenv("YOUTUBE_API_KEY")

def _fetch_video_data(keyword, pafter, pbefore, api_key, max_results=50):
    """Fetch video IDs and total views for the top 50 videos of
      a given keyword within a date range."""
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
    total_views = sum(int(item["statistics"].get("viewCount", 0)) for item in response.json().get("items", []))
    
    return total_views



def update_views():
    '''
    Updates the youtube_views.json file to store the sum of the top 50 videos' views 
    of the games listed in games.csv. If the game already exists, it just updates the two last months.

    The 'randomized' flag is now stored at the game level instead of for each time entry.
    If any API call for a game fails, the entire game's data will be randomized.
    '''

    API_KEY = _load_api_key()
    
    # Create path to landing_zone directory
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'landing_zone')
    # Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "youtube_views.json")
    
    existing_data = {}
    
    #Load existing data if JSON file exists
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)

    views = {}  # New, clean data structure
    
    games_file = os.path.join(output_dir, 'youtube-games.csv')
    with open(games_file, newline='') as f:
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