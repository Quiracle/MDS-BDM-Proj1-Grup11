from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import SparkSession

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
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # Load Twitch data from delta
    twitch_df = spark.read.format("delta").load("file:///data/delta/twitch_data_*/")

    # Write to InfluxDB
    for row in twitch_df.collect():
        point = (
            Point("twitch_views")
            .tag("game", row["game_name"])
            .field("viewers", int(row["viewers"]))
            .time(int(row["timestamp"] * 1e9))  # nanoseconds
        )
        write_api.write(bucket=bucket, org=org, record=point)

    print("Twitch viewers inserted into InfluxDB (trusted zone)")

if __name__ == "__main__":
    run()