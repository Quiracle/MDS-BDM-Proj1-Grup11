from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
import logging
from datetime import datetime


class DeltaWriter:
    def __init__(self, delta_path="/opt/airflow/data/delta/youtube_data", master="local[*]"):
        self.base_delta_path = delta_path
        self.master = master  # Defer spark creation
        # Remove self.spark and self.schema from __init__
        
        # Initialize Spark session with Delta Lake
    def _create_spark_session(self):
        builder = (
            SparkSession.builder
            .appName("TwitchDeltaLakeWriter")
            .master(self.master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Delta table path
        self.table_path = "../data/delta/youtube_data"
    
    def _get_schema(self):
        return StructType([
            StructField("game", StringType(), nullable=False),
            StructField("period_start", StringType(), nullable=False),
            StructField("period_end", StringType(), nullable=False),
            StructField("total_views", LongType(), nullable=False),
            StructField("is_randomized", BooleanType(), nullable=False),
            StructField("timestamp", StringType(), nullable=True)  # For recording when data was loaded
        ])
    
    def write_records(self, records):
        try:
            # Convert records to DataFrame
            spark = self._create_spark_session()
            schema = self._get_schema()
            df = spark.createDataFrame(records, schema=schema)

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            delta_path_with_timestamp = f"{self.base_delta_path}_{timestamp}"

            # Write to Delta Lake
            df.write.format("delta") \
                .mode("append") \
                .save(delta_path_with_timestamp)
                
            logging.info(f"Successfully wrote {df.count()} records to Delta Lake")
        except Exception as e:
            logging.error(f"Failed to write records to Delta Lake: {e}")
            raise
    
    def __del__(self):
        # Avoid doing this; moved to `stop()` method
        pass