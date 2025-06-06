import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, ArrayType, MapType
)
from datetime import datetime

class DeltaWriter:
    def __init__(self, delta_path="/opt/airflow/data/delta/Steam_games_data", master="local[*]"):
        self.base_delta_path = delta_path
        self.spark = self._create_spark_session(master)
        self.schema = StructType([
            StructField("appid", StringType(), True),
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("developers", ArrayType(StringType()), True),
            StructField("publishers", ArrayType(StringType()), True),
            StructField("release_date", StringType(), True),
            StructField("supported_languages", StringType(), True),
            StructField("categories", ArrayType(StringType()), True),
            StructField("genres", ArrayType(StringType()), True),
            StructField("platforms", MapType(StringType(), StringType()), True),
            StructField("price", StringType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("current_player_count", LongType(), True),
            StructField("timestamp", DoubleType(), True),
        ])

    def _create_spark_session(self, master):
        builder = (
            SparkSession.builder
            .appName("DeltaLakeWriter")
            .master(master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

        return configure_spark_with_delta_pip(builder).getOrCreate()

    def write_records(self, records):
        if not records:
            print("No records to write.")
            return

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        delta_path_with_timestamp = f"{self.base_delta_path}_{timestamp}"

        df = self.spark.createDataFrame(records, schema=self.schema)
        df.write.format("delta").mode("append").save(delta_path_with_timestamp)
        print(f"Wrote {len(records)} records to Delta Lake at {delta_path_with_timestamp}")
