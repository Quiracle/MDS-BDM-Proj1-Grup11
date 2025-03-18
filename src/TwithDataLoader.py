import logging
import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from TwitchAPI import get_access_token, get_all_streams, aggregate_viewers_by_game, get_game_names

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI', "mongodb://admin:password@localhost:27017/twitch_data?authSource=admin&retryWrites=true&w=majority")
MONGO_URI = "mongodb://admin:password@localhost:27017/twitch_data?authSource=admin&retryWrites=true&w=majority"

DB_NAME = os.getenv('MONGO_DB_NAME', 'twitch_data')
COLLECTION_NAME = os.getenv('MONGO_COLLECTION_NAME', 'categories_viewers')
FETCH_INTERVAL = int(os.getenv('TWITCH_FETCH_INTERVAL', 300))  # Default: 300 seconds (5 minutes)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to MongoDB
logging.info(f"Connecting to {MONGO_URI}")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Scheduler instance
scheduler = BlockingScheduler()

# Function to fetch and store data
def fetch_and_store_twitch_data():
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
        collection.insert_many(records)
        logging.info(f'Successfully inserted {len(records)} records into MongoDB.')
    else:
        logging.info('No data to insert.')

# Schedule the job
scheduler.add_job(fetch_and_store_twitch_data, 'interval', seconds=FETCH_INTERVAL)

if __name__ == '__main__':
    logging.info(f'Starting scheduled Twitch data collection every {FETCH_INTERVAL} seconds...')
    scheduler.start()
