from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder
        .appName("CheckData")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # List all twitch data folders
    import glob
    import os
    
    base_path = "/data/delta/"
    pattern = os.path.join(base_path, "twitch_data_*")
    folders = sorted(glob.glob(pattern))
    
    print(f"Found {len(folders)} twitch_data folders:")
    for folder in folders:
        print(f"\nChecking folder: {folder}")
        try:
            df = spark.read.format("delta").load(folder)
            print(f"Schema:")
            df.printSchema()
            print("\nFirst 5 rows:")
            df.show(5, truncate=False)
            print(f"\nTotal rows: {df.count()}")
        except Exception as e:
            print(f"Error reading {folder}: {e}")

if __name__ == "__main__":
    main()