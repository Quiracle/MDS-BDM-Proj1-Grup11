import os
import requests
import csv
import json
import random
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta


def _load_api_key():
    """Load API key from .env file in parent directory."""
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

    PROVISIONAL: If there is no credit left in our key, the funcion randomizes the
    views for the empty months.
    FUTURE: To obtain more keys and if one is empty, to use the second one.
    '''

    API_KEY = _load_api_key()
    output_file = "youtube_views.json"
    existing_data = {}
    
    #Load existing data if JSON file exists
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)

    views = existing_data.copy() #To check the existance of the games 
    
    with open('games.csv', newline='') as f:
        reader = csv.reader(f)
        keywords = [row[1] for row in list(reader)[1:] if len(row) > 1]
    
    today = datetime.now()
    current_month_start = today.replace(day=1)
    last_month_start = (today - relativedelta(months=1)).replace(day=1)
    last_month_end = current_month_start - relativedelta(days=1)
    
    for keyword in keywords:

        if keyword in existing_data:

            #Update only the last month and current month
            for after, before in [(current_month_start, today), (last_month_start, last_month_end)]:
                total_views = _fetch_video_data(keyword, after, before, API_KEY)

                if total_views == 0: #We have limited usage of the api, provisional random data
                    total_views = random.randint(0,10000000)

                #print(f"Updated views for '{keyword}' from {after.date()} to {before.date()}: {total_views:,} views")
                views[keyword].append({"after": after.strftime("%Y-%m-%d"), "before": before.strftime("%Y-%m-%d"), "total_views": total_views})
        
        else:
            #If game is new, fetch all months as before
            months = []
            for i in range(12):
                first_day = (today - relativedelta(months=i)).replace(day=1)
                last_day = (first_day + relativedelta(months=1)) - relativedelta(days=1)
                months.append((first_day, last_day))
            months.insert(0, (current_month_start, today))
            
            views[keyword] = []
            for after, before in months:
                total_views = _fetch_video_data(keyword, after, before, API_KEY)
                if total_views == 0: #We have limited usage of the api, provisional random data
                    total_views = random.randint(0,10000000)
                #print(f"Total views for '{keyword}' from {after.date()} to {before.date()}: {total_views:,} views")


                views[keyword].append({"after": after.strftime("%Y-%m-%d"), "before": before.strftime("%Y-%m-%d"), "total_views": total_views})

    #Save updated data to JSON file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(views, f, indent=4)
    
    print(f"Data saved to {output_file}")



if __name__ == '__main__':
    update_views()