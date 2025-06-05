from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import SparkSession
import glob
import os
import logging
import subprocess

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run():
    # Influx config
    bucket = "steammetrics"
    org = "steamorg"
    token = "admintoken"
    url = "http://influx-trusted:8086"

    logger.info("Initializing InfluxDB client...")
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Spark session
    logger.info("Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("InfluxLoader")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.cores", "1")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )

    # Load Twitch data from delta
    base_path = "/opt/airflow/data/delta/"
    pattern = os.path.join(base_path, "twitch_data_*")
    
    logger.info(f"Looking for twitch data folders in {base_path}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Directory contents of {base_path}:")
    try:
        for item in os.listdir(base_path):
            logger.info(f"  {item}")
    except Exception as e:
        logger.error(f"Error listing directory contents: {e}")

    folders = sorted(glob.glob(pattern))
    logger.info(f"Found {len(folders)} twitch_data folders: {folders}")

    # Load only valid ones
    dfs = []
    for folder in folders:
        try:
            logger.info(f"Attempting to load data from {folder}")
            # Try to read the folder with Spark
            twitch_df = spark.read.format("delta").load(folder)
            row_count = twitch_df.count()
            logger.info(f"Successfully loaded {row_count} rows from {folder}")
            logger.info("Schema:")
            twitch_df.printSchema()
            logger.info("First 5 rows:")
            twitch_df.show(5, truncate=False)
            dfs.append(twitch_df)
        except Exception as e:
            logger.error(f"Error loading {folder}: {e}")
            # Try to fix permissions
            try:
                logger.info(f"Attempting to fix permissions for {folder}")
                subprocess.run(["chmod", "-R", "755", folder], check=True)
                # Try reading again after fixing permissions
                twitch_df = spark.read.format("delta").load(folder)
                row_count = twitch_df.count()
                logger.info(f"Successfully loaded {row_count} rows from {folder} after fixing permissions")
                logger.info("Schema:")
                twitch_df.printSchema()
                logger.info("First 5 rows:")
                twitch_df.show(5, truncate=False)
                dfs.append(twitch_df)
            except Exception as e2:
                logger.error(f"Failed to fix permissions or load data from {folder}: {e2}")

    if not dfs:
        logger.error("No data was loaded from any folder!")
        return

    # Write to InfluxDB
    total_points = 0
    for df in dfs:
        try:
            rows = df.collect()
            points = []
            for row in rows:
                try:
                    point = (
                        Point("twitch_views")
                        .tag("game", str(row["game_name"]))
                        .field("viewers", int(row["viewers"]))
                        .time(int(row["timestamp"] * 1e9))
                    )
                    points.append(point)
                except Exception as e:
                    logger.error(f"Error creating point from row: {e}")
                    continue

            if points:
                logger.info(f"Writing {len(points)} points to InfluxDB...")
                write_api.write(bucket=bucket, org=org, record=points)
                total_points += len(points)
                logger.info(f"Successfully wrote {len(points)} points to InfluxDB")
        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")

    logger.info(f"Successfully wrote {total_points} points to InfluxDB")

if __name__ == "__main__":
    run()