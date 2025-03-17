import requests
import time
import os
from dotenv import load_dotenv
from collections import defaultdict

# Load environment variables
load_dotenv()
CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')

# Get OAuth token
def get_access_token():
    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    print("ACCESS TOKEN AQUIRED!")
    return response.json()['access_token']

# Get all active streams with pagination
def get_all_streams(access_token):
    url = 'https://api.twitch.tv/helix/streams'
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'first': 100,  # Maximum allowed per request
        'after': ''
    }
    i = 0
    all_streams = []
    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        all_streams.extend(data['data'])
        print(data["pagination"])
        if 'pagination' in data and 'cursor' in data['pagination'] and i < 30:
            params['after'] = data['pagination']['cursor']
            i+=1
        else:
            break  # No more pages
        
        time.sleep(1)  # To avoid hitting rate limits
    
    return all_streams

# Aggregate viewer counts per game
def aggregate_viewers_by_game(streams):
    game_viewers = defaultdict(int)
    for stream in streams:
        game_id = stream['game_id']
        viewer_count = stream['viewer_count']
        game_viewers[game_id] += viewer_count
        print(viewer_count)
    return game_viewers

# Get game names for the collected game IDs
def get_game_names(access_token, game_ids):
    url = 'https://api.twitch.tv/helix/games'
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {access_token}'
    }
    
    game_names = {}
    game_ids = list(game_ids)
    for i in range(0, len(game_ids), 100):  # Twitch allows max 100 IDs per request
        params = [('id', game_id) for game_id in game_ids[i:i+100]]
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        for game in response.json()['data']:
            game_names[game['id']] = game['name']
    
    return game_names

if __name__ == '__main__':
    token = get_access_token()
    streams = get_all_streams(token)
    game_viewers = aggregate_viewers_by_game(streams)
    game_names = get_game_names(token, game_viewers.keys())
    
    # Combine and print results
    results = [(game_names.get(game_id, 'Unknown'), viewers) for game_id, viewers in game_viewers.items()]
    results.sort(key=lambda x: x[1], reverse=True)  # Sort by viewers descending
    
    for game, viewers in results:
        print(f'{game}: {viewers} viewers')
