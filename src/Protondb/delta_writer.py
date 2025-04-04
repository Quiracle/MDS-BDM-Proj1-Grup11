#!/usr/bin/env python3

import os
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class DeltaWriter:
    def __init__(self, delta_path="file:///data/delta/proton_data", master="local[*]"):
        self.base_delta_path = delta_path
        self.spark = self._create_spark_session(master)
        self.schema = StructType([
            StructField("Name", StringType(), True),
            StructField("AppID", StringType(), True),
            StructField("protonBestReportedTier", StringType(), True),
            StructField("protonConfidence", StringType(), True),
            StructField("protonScore", StringType(), True),
            StructField("protonTier", StringType(), True),
            StructField("protonTotal", StringType(), True),
            StructField("protonTrendingTier", StringType(), True),
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
