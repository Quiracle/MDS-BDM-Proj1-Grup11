import logging
import json
import os
from datetime import datetime
from .YoutubeAPI import update_views
from .DeltaWriter import DeltaWriter

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_store_youtube_data():
    delta_writer = DeltaWriter()
    logging.info('Fetching YouTube views data...')

    # Update views data (this will create/update youtube_views.json in the same dir)
    update_views()

    # Define path to the local youtube_views.json
    script_dir = os.path.dirname(__file__)
    json_path = os.path.join(script_dir, 'src/Youtube/youtube_views.json')

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            records = json.load(f)

        if records:
            # Transform data to match Delta table schema
            delta_records = []
            for game, data in records.items():
                for entry in data['entries']:
                    delta_records.append({
                        'game': game,
                        'period_start': entry['after'],
                        'period_end': entry['before'],
                        'total_views': entry['total_views'],
                        'is_randomized': data['randomized'],
                        'timestamp': datetime.now().isoformat()
                    })

            logging.info(f"Writing {len(delta_records)} YouTube view records to Delta Lake...")
            delta_writer.write_records(delta_records)
        else:
            logging.info("No YouTube data to write.")
    except Exception as e:
        logging.error(f"Failed to process YouTube data: {e}")

if __name__ == '__main__':
    logging.info('Starting YouTube data collection...')
    fetch_and_store_youtube_data()
