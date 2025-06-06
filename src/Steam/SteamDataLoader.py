import logging
import os
#from dotenv import load_dotenv
from .SteamGamesAPI import fetch_steam_game_data , fetch_steam_game_data_json
from .SteamUsersAPI import fetch_steam_user_data , fetch_steam_user_data_json
from .DeltaWriterGames import DeltaWriter as DG
from .DeltaWriterUsers import DeltaWriter as DU

# Load environment variables
#load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Delta writers


# Function to fetch and store data
def fetch_and_store_steam_data():
    delta_writer_games = DG()  
    delta_writer_users = DU()
    logging.info('Fetching Steam game data...')
    records = fetch_steam_game_data_json() #Change this to call API instead of json
    if records:
        logging.info(f"Writing {len(records)} Game records to Delta Lake...")
        delta_writer_games.write_records(records)
    else:
        logging.info("No Game data to write.")

    records= fetch_steam_user_data_json() #Change this to call API instead of json
    if records:
        logging.info(f"Writing {len(records)} User records to Delta Lake...")
        delta_writer_users.write_records(records)
    else:
        logging.info("No User data to write.")

if __name__ == '__main__':
    logging.info(f'Starting one-time Steam data collection...')
    fetch_and_store_steam_data()