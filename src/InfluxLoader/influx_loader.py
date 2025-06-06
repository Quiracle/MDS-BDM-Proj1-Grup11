from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import SparkSession
import glob
import os
import logging
import subprocess

def run():
    # Influx config
    bucket = "steammetrics"
    org = "steamorg"
    token = "admintoken"
    url = "http://influx-trusted:8086"

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Spark session
    spark = (
        SparkSession.builder
        .appName("InfluxLoader")
        # .master("spark://spark-master:7077")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # Load Twitch data from delta
    base_path = "/opt/airflow/data/delta/"
    pattern = os.path.join(base_path, "twitch_data_*")

    folders = sorted(glob.glob(pattern))

    # Load only valid ones
    dfs = []
    for folder in folders:
        delta_log_path = os.path.join(folder, "_delta_log")
        if not os.path.exists(delta_log_path):
            print(f"Skipping {folder} because _delta_log does not exist")
            continue
        try:
            # Try to read the folder with Spark
            twitch_df = spark.read.format("delta").load(folder)
            dfs.append(twitch_df)
        except Exception as e:
            print(f"Skipping {folder} due to error: {e}")
    if not dfs:
        print(f"Skipping {folder} due to error: {e}")

    # Write to InfluxDB
    # Write to InfluxDB
    for df in dfs:
        rows = df.collect()
        points = []
        for row in rows:
            point = (
                Point("twitch_views")
                .tag("game", row["game_name"])
                .field("viewers", int(row["viewers"]))
                .time(int(row["timestamp"] * 1e9))
            )
            points.append(point)

        write_api.write(bucket=bucket, org=org, record=points)
    print("Twitch viewers inserted into InfluxDB (trusted zone)")

if __name__ == "__main__":
    run()