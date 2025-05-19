# twitch_to_deltalake.py

import logging
import time
from dotenv import load_dotenv
from .TwitchAPI import get_access_token, get_all_streams, aggregate_viewers_by_game, get_game_names
from .DeltaWriter import DeltaWriter
import os

# Load environment variables
load_dotenv()
FETCH_INTERVAL = int(os.getenv('TWITCH_FETCH_INTERVAL', 300))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Delta writer
  # You can pass a path or master URL if needed

# Function to fetch and store data
def fetch_and_store_twitch_data():
    delta_writer = DeltaWriter()
    logging.info('Fetching Twitch data...')
    token = get_access_token()
    streams = get_all_streams(token)
    game_viewers = aggregate_viewers_by_game(streams)
    game_names = get_game_names(token, game_viewers.keys())

    records = []
    timestamp = time.time()
    for game_id, viewers in game_viewers.items():
        records.append({
            'game_id': game_id,
            'game_name': game_names.get(game_id, 'Unknown'),
            'viewers': viewers,
            'timestamp': timestamp
        })

    if records:
        logging.info(f"Writing {len(records)} records to Delta Lake...")
        delta_writer.write_records(records)
    else:
        logging.info("No data to write.")

if __name__ == "__main__":
    while True:
        fetch_and_store_twitch_data()