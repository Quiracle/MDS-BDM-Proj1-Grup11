#!/usr/bin/env python3

import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pymongo import MongoClient
from datetime import datetime

def run():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ProtonDeltaToTrustedMongo") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Find all matching Delta folders
    base_path = "/opt/airflow/data/delta"
    pattern = os.path.join(base_path, "proton_data_*")
    folders = sorted(glob.glob(pattern))

    print(f"Found {len(folders)} proton_data folders")

    # Load only valid ones
    dfs = []
    for folder in folders:
        try:
            df = spark.read.format("delta").load(folder)
            dfs.append(df)
        except Exception as e:
            print(f"Skipping {folder} due to error: {e}")

    # Merge & deduplicate
    if dfs:
        # Combine all DataFrames
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)
        
        # Deduplicate based on AppID (assuming AppID is unique for each game)
        # Keep the most recent record for each AppID
        deduped_df = combined_df.dropDuplicates(["AppID"])
        
        # Remove entries with missing scores (current, trending, historical_max)
        required_score_cols = ["protonTier", "protonTrendingTier", "protonBestReportedTier"]
        for col in required_score_cols:
            deduped_df = deduped_df.filter(F.col(col).isNotNull())
        
        # Normalize tier values to lowercase
        for col in required_score_cols:
            deduped_df = deduped_df.withColumn(col, F.lower(F.col(col)))

        # Add timestamp for when the data was loaded to trusted zone
        deduped_df = deduped_df.withColumn("trusted_zone_timestamp", 
                                         F.lit(datetime.now().isoformat()))

        # Convert to JSON rows
        json_data = [row.asDict() for row in deduped_df.collect()]

        # Write to MongoDB trusted zone
        client = MongoClient("mongodb://admin:password@mongo-trusted:27017/")
        db = client["trusted_zone"]
        collection = db["proton_data_trusted"]
        
        # Clear existing data and insert new deduplicated data
        collection.delete_many({})
        collection.insert_many(json_data)

        print(f"Inserted {len(json_data)} deduplicated records into MongoDB trusted zone.")
    else:
        print("No valid Delta folders found.")

if __name__ == "__main__":
    run()