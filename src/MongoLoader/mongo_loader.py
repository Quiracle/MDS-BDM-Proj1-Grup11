import os
import glob
from pyspark.sql import SparkSession
from pymongo import MongoClient

def run():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DeltaToMongo") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Find all matching Delta folders
    base_path = "/opt/airflow/data/delta"
    pattern = os.path.join(base_path, "youtube_data_*")
    folders = sorted(glob.glob(pattern))

    print(f"Found {len(folders)} youtube_data folders")

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
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)
        combined_df = combined_df.dropDuplicates()

        # Convert to JSON rows
        json_data = [row.asDict() for row in combined_df.collect()]

        # Write to MongoDB
        client = MongoClient("mongodb://admin:password@mongo_trusted:27017/")
        db = client["trusted_zone"]
        collection = db["reviews"]
        collection.delete_many({})
        collection.insert_many(json_data)

        print(f"Inserted {len(json_data)} records into MongoDB.")
    else:
        print("No valid Delta folders found.")

if __name__ == "__main__":
    run()

