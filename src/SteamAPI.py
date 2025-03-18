import requests
import random
import time
import json


# API URLs
GET_ALL_GAMES_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
GAME_DETAILS_URL = "https://store.steampowered.com/api/appdetails?appids={}"
PLAYER_COUNT_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={}"


def get_all_steam_games():
    response = requests.get(GET_ALL_GAMES_URL)
    if response.status_code == 200:
        games = response.json().get("applist", {}).get("apps", [])
        return games
    return []


def get_game_details(app_id):
    url = GAME_DETAILS_URL.format(app_id)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if str(app_id) in data and data[str(app_id)]["success"]:
            return data[str(app_id)]["data"]
    return None


def get_player_count(app_id):
    url = PLAYER_COUNT_URL.format(app_id)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get("response", {}).get("player_count", 0)
    return 0


all_games = get_all_steam_games()

sampled_games = random.sample(all_games, 10000) if len(all_games) > 10000 else all_games

game_data = []
for index, game in enumerate(sampled_games):
    app_id = game.get("appid")
    name = game.get("name")
    details = get_game_details(app_id)
    if not details:
        continue
    price = details.get("price_overview", {}).get("final", 0) / 100 
    currency = details.get("price_overview", {}).get("currency", "N/A")
    player_count = get_player_count(app_id)
    game_data.append({
        "AppID": app_id,
        "Name": name,
        "Price": f"{price} {currency}",
        "Players Online": player_count,
        "Genres": ", ".join(details.get("genres", [{}])[0].get("description", "N/A")) if "genres" in details else "N/A",
        "Release Date": details.get("release_date", {}).get("date", "Unknown"),
    })
    time.sleep(0.5)


with open("steam_game_details.json", "w", encoding="utf-8") as f:
    json.dump(game_data, f, indent=4, ensure_ascii=False)


API_KEY = ""

START_STEAM_ID = "76561197960435530"  #Gabe Newell Steam ID

USER_LIMIT = 1000
BATCH_SIZE = 100  

# API URLs
PLAYER_SUMMARY_URL = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
STEAM_LEVEL_URL = "https://api.steampowered.com/IPlayerService/GetSteamLevel/v1/"
OWNED_GAMES_URL = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/"
FRIEND_LIST_URL = "https://api.steampowered.com/ISteamUser/GetFriendList/v1/"


def safe_request(url, params, retries=5, sleep_time=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException as e:
            print(f" Request failed (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(sleep_time)  
    return None  


def get_friends(steam_id):
    params = {"key": API_KEY, "steamid": steam_id, "relationship": "friend"}
    data = safe_request(FRIEND_LIST_URL, params)
    if data and "friendslist" in data:
        return [friend["steamid"] for friend in data["friendslist"]["friends"]]
    return []

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
        return len(data["response"]["games"])  
    return 0  

user_queue = [START_STEAM_ID]  
visited_users = set()  
user_data = []

while user_queue and len(user_data) < USER_LIMIT:
    current_id = user_queue.pop(0)
    if current_id in visited_users:
        continue  
    visited_users.add(current_id)

    print(f"Checking user {len(user_data) + 1}/{USER_LIMIT} - {current_id}")

    
    users = get_player_summaries([current_id])
    if not users:
        continue  

    user = users[0]
    username = user.get("personaname", "Unknown")
    country = user.get("loccountrycode", "N/A")
    profile_url = user.get("profileurl", "N/A")

    # Get additional details
    steam_level = get_steam_level(current_id)
    owned_games = get_owned_games(current_id)

    user_data.append({
        "SteamID": current_id,
        "Username": username,
        "Country": country,
        "Steam Level": steam_level,
        "Owned Games": owned_games,
        "Profile URL": profile_url,
    })

    if len(user_data) >= USER_LIMIT:
        break

    friends = get_friends(current_id)
    for friend_id in friends:
        if friend_id not in visited_users:
            user_queue.append(friend_id)

    time.sleep(0.5)  


with open("steam_users_from_friends.json", "w", encoding="utf-8") as f:
    json.dump(user_data, f, indent=4, ensure_ascii=False)



