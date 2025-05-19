# delta_writer.py

import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from datetime import datetime

class DeltaWriter:
    def __init__(self, delta_path="file:///data/delta/twitch_data", master="local[*]"):
        self.base_delta_path = delta_path
        self.master = master  # Defer spark creation
        # Remove self.spark and self.schema from __init__

    def _create_spark_session(self):
        builder = (
            SparkSession.builder
            .appName("DeltaLakeWriter")
            .master(self.master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    def _get_schema(self):
        return StructType([
            StructField("game_id", StringType(), True),
            StructField("game_name", StringType(), True),
            StructField("viewers", LongType(), True),
            StructField("timestamp", DoubleType(), True),
        ])

    def write_records(self, records):
        if not records:
            print("No records to write.")
            return

        spark = self._create_spark_session()
        schema = self._get_schema()

        df = spark.createDataFrame(records, schema=schema)

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        delta_path_with_timestamp = f"{self.base_delta_path}_{timestamp}"

        df.write.format("delta").mode("append").save(delta_path_with_timestamp)
        print(f"Wrote {len(records)} records to Delta Lake at {delta_path_with_timestamp}")
