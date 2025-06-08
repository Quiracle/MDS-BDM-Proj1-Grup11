import os
import glob
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, when, size, lit,
    to_date, to_timestamp, split, current_date, current_timestamp, transform, regexp_replace,instr, coalesce,expr,regexp_extract
)
from pyspark.sql.types import (
    ArrayType, MapType, IntegerType, StringType, FloatType, DateType, TimestampType,DoubleType
)


def clean_steam_users(df):
    df = df.withColumn("Username", lower(trim(regexp_replace(col("Username"), "\\s+", " ")))) \
           .filter(col("Username").isNotNull()) \
           .filter(col("Username") != "") \
           .filter(col("Username").rlike(r"^[a-zA-Z0-9_.]+$"))

    if "Owned_Games" in df.columns:
        df = df.withColumn(
            "Owned_Game_AppIDs",
            expr("transform(Owned_Games, x -> cast(coalesce(x['appid'], x['app_id']) as int))")
        )

        df = df.withColumn(
            "Owned_Game_PlayedHours",
            expr("transform(Owned_Games, x -> cast(coalesce(x['played_hours'], x['playtime_forever']) as float))")
        )

        df = df.withColumn("Owned_Games_Count", when(col("Owned_Game_AppIDs").isNotNull(), size(col("Owned_Game_AppIDs"))).otherwise(0))
        df = df.filter(col("Owned_Games_Count") > 0)
        df = df.drop("Owned_Games")
    else:
        df = df.withColumn("Owned_Games_Count", lit(0))
        df = df.withColumn("Owned_Game_AppIDs", lit([]).cast(ArrayType(IntegerType())))
        df = df.withColumn("Owned_Game_PlayedHours", lit([]).cast(ArrayType(FloatType())))

    return df.dropDuplicates(["SteamID"])



def clean_steam_games(df):
    df = df.withColumn("price_cleaned",
                       when(lower(trim(col("price"))) == "free", lit("0.0"))
                       .when(col("price").isNull() | (trim(col("price")) == ""), lit("0.0"))
                       .otherwise(col("price")))


    df = df.withColumn("price_value",
        when(instr(col("price_cleaned"), "$") > 0, regexp_replace(regexp_extract(col("price_cleaned"), r"(\d[\d.,]*)", 1), ",", "."))
        .when(instr(col("price_cleaned"), "€") > 0, regexp_replace(regexp_extract(col("price_cleaned"), r"(\d[\d.,]*)", 1), ",", "."))
        .otherwise(lit(None).cast(StringType())) 
    )


    df = df.withColumn("price", coalesce(col("price_value").cast(DoubleType()), lit(0.0)))
    df = df.drop("price_cleaned", "price_value")

    if "release_date" in df.columns:
        df = df.withColumn("release_date", to_date(col("release_date"), "d MMM, yyyy"))
        df = df.filter(col("release_date").isNotNull() & (col("release_date") <= current_date()))
    else:
        df = df.withColumn("release_date", lit(None).cast("date"))

    df = df.withColumn("price",when(col("price") == "Free", "0,0€").otherwise(col("price")))
    df = df.withColumn("name", lower(trim(col("name"))))
    df = df.filter(col("name").isNotNull() & (col("name") != ""))
    df = df.filter(col("name").rlike(r"^[a-zA-Z0-9\s\._,\-']{1,100}$"))


    for field in ["supported_languages", "tags", "genres", "developers", "publishers"]:
        if field in df.columns:
            dtype = df.schema[field].dataType
            if isinstance(dtype, ArrayType):
                df = df.withColumn(field, expr(f"transform({field}, x -> regexp_replace(x, '<[^>]*>', ''))"))
            elif isinstance(dtype, StringType):
                df = df.withColumn(field, regexp_replace(col(field), "<[^>]*>", ""))

    return df.dropDuplicates(["appid"])

def clean_twitch(df):
    df = df.withColumnRenamed("viewer_count", "view_count") if "viewer_count" in df.columns else df
    df = df.withColumnRenamed("started_at", "start_time") if "started_at" in df.columns else df

    df = df.withColumn("game_id", lower(trim(col("game_id"))))
    df = df.filter(col("game_id").isNotNull() & (col("game_id") != ""))
    df = df.filter(col("game_id").rlike(r"^.{5,}$"))

    if "view_count" in df.columns:
        df = df.filter(col("view_count").cast("long") >= 10)

    if "start_time" in df.columns:
        df = df.withColumn("start_time", to_timestamp(col("start_time")))
        df = df.filter(col("start_time").isNotNull() & (col("start_time") <= current_timestamp()))

    if "tag_ids" in df.columns and df.schema["tag_ids"].dataType.simpleString() == "string":
        df = df.withColumn("tag_ids", split(col("tag_ids"), ","))

    return df.dropDuplicates(["id"]) if "id" in df.columns else df.dropDuplicates()

def run():
    spark = SparkSession.builder \
        .appName("EnhancedSimpleETL") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    con = duckdb.connect("/opt/airflow/data/trusted_zone/trusted.duckdb")

    sources = {
        "steam_users": ("Steam_user_*", clean_steam_users),
        "steam_games": ("Steam_games_*", clean_steam_games),
        "twitch": ("twitch_data_*", clean_twitch)
    }

    base_path = "/opt/airflow/data/delta"

    for name, (pattern, cleaner) in sources.items():
        folders = sorted(glob.glob(os.path.join(base_path, pattern)))
        if not folders:
            print(f"No data found for {name}")
            continue

        df = spark.read.format("delta").load(folders[-1])
        df_clean = cleaner(df)

        if df_clean.count() > 0:
            con.execute(f"DROP TABLE IF EXISTS {name}")
            con.register("temp_df", df_clean.toPandas())
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM temp_df")
            print(f" {name} loaded into DuckDB.")
        else:
            print(f"No valid records for {name} after cleaning.")

    con.close()
    spark.stop()

if __name__ == "__main__":
    run()



