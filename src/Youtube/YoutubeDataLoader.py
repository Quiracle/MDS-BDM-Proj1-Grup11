import logging
import json
from YoutubeAPI import update_views
from DeltaWriter import DeltaWriter
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Delta writer
delta_writer = DeltaWriter()

def fetch_and_store_youtube_data():
    logging.info('Fetching YouTube views data...')
    
    # Update views data (this will create/update youtube_views.json)
    update_views()
    
    # Load the generated JSON data
    try:
        with open('../landing_zone/youtube_views.json', 'r') as f:
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