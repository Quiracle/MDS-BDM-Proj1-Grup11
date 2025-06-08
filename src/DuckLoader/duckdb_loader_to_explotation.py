import os
import duckdb
import pandas as pd
import ast

def safe_eval_list(val, dtype=str):
    if val is None:
        return []

    if hasattr(val, '__iter__') and not isinstance(val, str):
        try:

            if hasattr(val, 'tolist'):
                val_list = val.tolist()
            else:
                val_list = list(val)
            return [dtype(x) for x in val_list if x is not None and str(x).strip()]
        except:
            return []
    try:
        if pd.isna(val):
            return []
    except (ValueError, TypeError):

        pass

    if isinstance(val, str):
        if not val.strip():
            return []
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, (list, tuple)):
                return [dtype(x) for x in parsed if x is not None and str(x).strip()]
            return [dtype(parsed)] if parsed is not None else []
        except:
            return [dtype(val)]

    return [dtype(val)] if val is not None else []

def run():
    trusted_db = "/opt/airflow/data/trusted_zone/trusted.duckdb"
    exploit_dir = "/opt/airflow/data/exploitation_zone/"
    os.makedirs(exploit_dir, exist_ok=True)
    exploit_db = os.path.join(exploit_dir, "explotation.duckdb")
    
    trusted_con = duckdb.connect(trusted_db, read_only=True)
    exploit_con = duckdb.connect(exploit_db, read_only=False)
    
    try:
        print("Loading data from trusted zone...")
        
        users_df = trusted_con.execute("""
            SELECT SteamID, Username, Country, Steam_Level, Owned_Games_Count,
                   Owned_Game_AppIDs, Owned_Game_PlayedHours, Profile_URL
            FROM steam_users
        """).fetchdf()
        
        games_df = trusted_con.execute("""
            SELECT appid, name, price, current_player_count, release_date,
                   genres, developers, publishers, categories, supported_languages, tags, type
            FROM steam_games
        """).fetchdf()
        
        twitch_df = trusted_con.execute("""
            SELECT game_name, viewers, timestamp FROM twitch
        """).fetchdf()
        
        print(f"Loaded {len(users_df)} users, {len(games_df)} games, {len(twitch_df)} twitch records")
        
        if 'Owned_Game_AppIDs' in users_df.columns:
            users_df['Owned_Game_AppIDs'] = users_df['Owned_Game_AppIDs'].apply(lambda x: safe_eval_list(x, str))
        if 'Owned_Game_PlayedHours' in users_df.columns:
            users_df['Owned_Game_PlayedHours'] = users_df['Owned_Game_PlayedHours'].apply(lambda x: safe_eval_list(x, float))

        list_cols = ['genres', 'developers', 'publishers', 'categories', 'supported_languages', 'tags']
        for col in list_cols:
            if col in games_df.columns:
                games_df[col] = games_df[col].apply(lambda x: safe_eval_list(x, str))
        
        users_df['TotalPlaytime'] = users_df['Owned_Game_PlayedHours'].apply(
            lambda x: round(sum(x) / 60.0, 2) if x else 0.0
        )
        
        print("Calculating game metrics...")
        

        twitch_summary = twitch_df.groupby('game_name')['viewers'].sum().reset_index()
        twitch_summary.columns = ['game_name', 'TotalTwitchViewers']
        
        user_game_data = []
        for _, user in users_df.iterrows():
            if user['Owned_Game_AppIDs'] and user['Owned_Game_PlayedHours']:
                for appid, hours in zip(user['Owned_Game_AppIDs'], user['Owned_Game_PlayedHours']):
                    if appid and pd.notna(hours):
                        user_game_data.append({
                            'SteamID': user['SteamID'],
                            'appid': appid,
                            'played_hours': round(hours / 60.0, 2)  # Convert to hours
                        })
        
        if user_game_data:
            user_game_df = pd.DataFrame(user_game_data)
            game_playtime = user_game_df.groupby('appid').agg({
                'played_hours': 'sum',
                'SteamID': 'nunique'
            }).reset_index()
            game_playtime.columns = ['appid', 'TotalPlayedHoursByUsers', 'NumberOfOwners']
        else:
            game_playtime = pd.DataFrame(columns=['appid', 'TotalPlayedHoursByUsers', 'NumberOfOwners'])
        
        game_metrics = games_df[['appid', 'name', 'price', 'current_player_count']].copy()
        game_metrics.columns = ['appid', 'GameName', 'Price', 'CurrentPlayerCount']
        
        game_metrics = game_metrics.merge(
            twitch_summary, 
            left_on=game_metrics['GameName'].str.lower(), 
            right_on=twitch_summary['game_name'].str.lower(), 
            how='left'
        )
        
        game_metrics = game_metrics.merge(game_playtime, on='appid', how='left')
        
        game_metrics = game_metrics[['appid', 'GameName', 'Price', 'CurrentPlayerCount', 'TotalTwitchViewers', 'TotalPlayedHoursByUsers', 'NumberOfOwners']]
        game_metrics = game_metrics.fillna({
            'CurrentPlayerCount': 0,
            'TotalTwitchViewers': 0,
            'TotalPlayedHoursByUsers': 0.0,
            'NumberOfOwners': 0
        })
        
        print("Calculating country metrics...")
        
        country_metrics = users_df.groupby('Country').agg({
            'SteamID': 'count',
            'Steam_Level': ['mean', 'min', 'max'],
            'TotalPlaytime': 'mean',
            'Owned_Games_Count': 'mean'
        }).round(2)
        
        country_metrics.columns = ['UserCount', 'AverageSteamLevel', 'MinSteamLevel', 'MaxSteamLevel', 'AverageTotalPlaytime', 'AverageOwnedGamesPerUser']
        country_metrics = country_metrics.reset_index()
        
        high_playtime = users_df[users_df['TotalPlaytime'] > 1000].groupby('Country').size().reset_index(name='HighPlaytimeUserCount')
        country_metrics = country_metrics.merge(high_playtime, on='Country', how='left')
        country_metrics['HighPlaytimeUserCount'] = country_metrics['HighPlaytimeUserCount'].fillna(0)
        
        print("Calculating user metrics...")
        
        user_metrics = users_df.copy()
        user_metrics = user_metrics.rename(columns={'SteamID': 'UserID'})
        
        user_metrics['AvgPlaytimePerOwnedGame'] = (
            user_metrics['TotalPlaytime'] / user_metrics['Owned_Games_Count']
        ).fillna(0).round(2)
        
        user_metrics['HasHighPlaytime'] = user_metrics['TotalPlaytime'] > 1000
        
        user_metrics['TotalPlaytime'] = user_metrics['TotalPlaytime'].clip(upper=100000)
        
        user_metrics['ListOfOwnedGames'] = user_metrics['Owned_Game_AppIDs'].apply(
            lambda x: str(x) if x else "[]"
        )
        
        user_metrics = user_metrics[[
            'UserID', 'Username', 'Country', 'Steam_Level', 'Owned_Games_Count',
            'TotalPlaytime', 'AvgPlaytimePerOwnedGame', 'HasHighPlaytime', 'ListOfOwnedGames'
        ]]
        
        print("Saving to exploitation database...")
        
        exploit_con.execute("DROP TABLE IF EXISTS game_metrics")
        exploit_con.register("game_metrics_temp", game_metrics)
        exploit_con.execute("CREATE TABLE game_metrics AS SELECT * FROM game_metrics_temp")
        
        exploit_con.execute("DROP TABLE IF EXISTS country_metrics")
        exploit_con.register("country_metrics_temp", country_metrics)
        exploit_con.execute("CREATE TABLE country_metrics AS SELECT * FROM country_metrics_temp")
        
        exploit_con.execute("DROP TABLE IF EXISTS user_metrics")
        exploit_con.register("user_metrics_temp", user_metrics)
        exploit_con.execute("CREATE TABLE user_metrics AS SELECT * FROM user_metrics_temp")
        
        print("All metrics calculated and stored successfully!")
        print(f"- Game metrics: {len(game_metrics)} records")
        print(f"- Country metrics: {len(country_metrics)} records")
        print(f"- User metrics: {len(user_metrics)} records")
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        trusted_con.close()
        exploit_con.close()

if __name__ == "__main__":
    run()