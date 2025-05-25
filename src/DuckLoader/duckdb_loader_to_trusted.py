import os
import glob
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DeltaToDuckDB") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    base_path = "/opt/airflow/data/delta"
    sources = {
        "steam_users": ("steam_users_*", "name"),
        "steam_games": ("steam_games_*", "name"),
        "twitch": ("twitch_data_*", "name")
    }

    db_path = "/opt/airflow/data/trusted_zone/trusted.duckdb"
    con = duckdb.connect(db_path)

    for table_name, (pattern, name_field) in sources.items():
        folders = sorted(glob.glob(os.path.join(base_path, pattern)))
        print(f"Found {len(folders)} folders for {table_name}")

        if not folders:
            print(f"No valid folders found for {table_name}. Skipping.")
            continue

        latest_folder = folders[-1]
        print(f"Using latest folder for {table_name}: {latest_folder}")

        try:
            df = spark.read.format("delta").load(latest_folder)
        except Exception as e:
            print(f"Failed to load {latest_folder} due to: {e}")
            continue

        # Validate name/username fields
        if name_field in df.columns:
            df = df.filter(
                col(name_field).isNotNull() &
                col(name_field).rlike(r"^[a-zA-Z0-9 _\-\.]{1,30}$")  # Accept alphanum, space, _, -, .
            )

        df = df.dropDuplicates()

        pandas_df = df.toPandas()

        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.register("pandas_df", pandas_df)
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM pandas_df")

        print(f"Inserted {len(pandas_df)} records into DuckDB table `{table_name}`.")

if __name__ == "__main__":
    run()
