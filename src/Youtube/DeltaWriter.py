from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
import logging

class DeltaWriter:
    def __init__(self):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # Initialize Spark session with Delta Lake
        self.spark = SparkSession.builder \
            .appName("YouTubeDataWriter") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
            
        # Define schema for YouTube data
        self.schema = StructType([
            StructField("game", StringType(), nullable=False),
            StructField("period_start", StringType(), nullable=False),
            StructField("period_end", StringType(), nullable=False),
            StructField("total_views", LongType(), nullable=False),
            StructField("is_randomized", BooleanType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False)  # For recording when data was loaded
        ])
        
        # Delta table path
        self.table_path = "../data/delta/youtube_data"
    
    def write_records(self, records):
        try:
            # Convert records to DataFrame
            df = self.spark.createDataFrame(records, schema=self.schema)
            
            # Write to Delta Lake
            df.write.format("delta") \
                .mode("append") \
                .save(self.table_path)
                
            logging.info(f"Successfully wrote {df.count()} records to Delta Lake")
        except Exception as e:
            logging.error(f"Failed to write records to Delta Lake: {e}")
            raise
    
    def __del__(self):
        self.spark.stop()