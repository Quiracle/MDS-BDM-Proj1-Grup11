import os
import duckdb
import pandas as pd
import numpy as np
import ast # Import for safely evaluating string representations of lists
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, countDistinct, lit,
    datediff, to_date, current_date, round,
    expr, coalesce, explode,
    when, array_min, array_max, array_join,
    first, mean, stddev, collect_set, collect_list,
    arrays_zip, min, max, lower, array_contains,
    count as spark_count # Renamed to avoid conflict with Python's built-in count
)
from pyspark.sql.types import (
    LongType, StringType, FloatType, DateType, IntegerType, DoubleType,
    StructType, StructField, ArrayType, DateType as SparkDateType
)

def run():
    # Paths
    trusted_db_path = "/opt/airflow/data/trusted_zone/trusted.duckdb"
    exploitation_db_dir = "/opt/airflow/data/exploitation_zone/"
    os.makedirs(exploitation_db_dir, exist_ok=True)
    exploitation_db_path = os.path.join(exploitation_db_dir, "explotation.duckdb")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("TrustedToExploitationKPIs") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    print("Spark Session initialized.")

    # Connect to source (trusted) and target (exploitation) DuckDBs
    trusted_con = duckdb.connect(database=trusted_db_path, read_only=True)
    exploit_con = duckdb.connect(database=exploitation_db_path, read_only=False)
    print(f"Connected to trusted DuckDB at {trusted_db_path}")
    print(f"Connected to exploitation DuckDB at {exploitation_db_path}")

    # --- Displaying HEAD of tables in Trusted DuckDB (as requested) ---
    print("\n--- Displaying HEAD of tables in Trusted DuckDB ---")
    try:
        tables = trusted_con.execute("SHOW TABLES").fetchdf()['name'].tolist()
        
        if not tables:
            print("No tables found in the trusted DuckDB.")
        else:
            for table_name in tables:
                print(f"\n--- Head of table: {table_name} (first 10 rows) ---")
                try:
                    # Fetch first 10 rows and print without truncation
                    table_head = trusted_con.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchdf()
                    print(table_head.to_string()) # Use .to_string() for full pandas DataFrame print
                except Exception as e:
                    print(f"   Error fetching head for {table_name}: {e}")
    except Exception as e:
        print(f"Error listing tables in trusted DuckDB: {e}")
    print("--------------------------------------------------")
    # --- END OF TRUSTED DB TABLE HEADS SECTION ---


    # --- Define Schemas explicitly for createDataFrame ---
    users_spark_schema = StructType([
        StructField("SteamID", StringType(), True),
        StructField("Username", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Steam_Level", LongType(), True),
        StructField("Owned_Games_Count", LongType(), True),
        StructField("Owned_Game_AppIDs", ArrayType(StringType()), True),
        StructField("Owned_Game_PlayedHours", ArrayType(DoubleType()), True),
        StructField("Profile_URL", StringType(), True)
    ])

    games_spark_schema = StructType([
        StructField("appid", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("current_player_count", LongType(), True),
        StructField("release_date", SparkDateType(), True),
        StructField("genres", ArrayType(StringType()), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("developers", ArrayType(StringType()), True),
        StructField("publishers", ArrayType(StringType()), True),
        StructField("supported_languages", ArrayType(StringType()), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("type", StringType(), True)
    ])

    twitch_spark_schema = StructType([
        StructField("game_name", StringType(), True),
        StructField("viewers", LongType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Custom function to ensure values are lists and handle NaNs/None/array-like inputs
    def process_list_column(val, dtype):
        # Initialize an empty list for accumulation
        processed_elements = []

        # Case 1: Value is a string, potentially a string representation of a list
        if isinstance(val, str):
            try:
                # Safely evaluate the string to a Python literal (list, tuple, etc.)
                parsed_val = ast.literal_eval(val)
                if isinstance(parsed_val, (list, tuple, set)):
                    # If parsing yields a list-like, process its elements
                    for item in parsed_val:
                        if pd.notna(item):
                            try:
                                processed_elements.append(dtype(item))
                            except (ValueError, TypeError):
                                pass # Skip non-convertible items
                else:
                    # If it's a single scalar string that's not NaN
                    if pd.notna(parsed_val):
                        try:
                            processed_elements.append(dtype(parsed_val))
                        except (ValueError, TypeError):
                            pass
            except (ValueError, SyntaxError):
                # If it's a normal string that can't be parsed as a list literal,
                # treat it as a single item if not NaN
                if pd.notna(val):
                    try:
                        processed_elements.append(dtype(val))
                    except (ValueError, TypeError):
                        pass

        # Case 2: Value is already a list, tuple, set, numpy array, or pandas Series
        elif isinstance(val, (list, tuple, set, np.ndarray, pd.Series)):
            for item in val:
                # Recursively call for each item, or directly process if simple
                if isinstance(item, (list, tuple, np.ndarray, pd.Series)):
                    # This handles nested lists/arrays by flattening them
                    processed_elements.extend(process_list_column(item, dtype))
                elif pd.notna(item):
                    try:
                        processed_elements.append(dtype(item))
                    except (ValueError, TypeError):
                        pass

        # Case 3: Value is a single non-string scalar (int, float, bool, None, NaN)
        elif pd.notna(val): # Check if it's not a NaN/None
            try:
                processed_elements.append(dtype(val))
            except (ValueError, TypeError):
                pass
        
        # Finally, return the cleaned list. If val was None or NaN, processed_elements will be empty.
        return processed_elements


    try:
        print("\nLoading data from Trusted DuckDB into Spark DataFrames...")

        # Read data from DuckDB into Pandas DataFrames first
        pandas_users_df = trusted_con.execute(
            """
            SELECT
                SteamID, Username, Country, Steam_Level,
                Owned_Games_Count, Owned_Game_AppIDs, Owned_Game_PlayedHours,
                Profile_URL
            FROM steam_users
            """
        ).fetchdf()

        # Apply list processing functions to array-like columns
        # The .apply(lambda x: ...) ensures process_list_column receives one cell value at a time.
        if 'Owned_Game_AppIDs' in pandas_users_df.columns:
            pandas_users_df['Owned_Game_AppIDs'] = pandas_users_df['Owned_Game_AppIDs'].apply(lambda x: process_list_column(x, str))
        
        if 'Owned_Game_PlayedHours' in pandas_users_df.columns:
            pandas_users_df['Owned_Game_PlayedHours'] = pandas_users_df['Owned_Game_PlayedHours'].apply(lambda x: process_list_column(x, float))

        # Ensure 'Owned_Games_Count' is correctly numeric (integer)
        if 'Owned_Games_Count' in pandas_users_df.columns:
            pandas_users_df['Owned_Games_Count'] = pd.to_numeric(pandas_users_df['Owned_Games_Count'], errors='coerce').fillna(0).astype(int)
        
        # Create Spark DataFrame from processed Pandas DataFrame
        df_users = spark.createDataFrame(pandas_users_df, schema=users_spark_schema)
        
        # Process games DataFrame
        pandas_games_df = trusted_con.execute(
            """
            SELECT
                appid, name, price, current_player_count, release_date,
                genres, categories, developers, publishers, supported_languages, tags, type
            FROM steam_games
            """
        ).fetchdf()

        for col_name in ['genres', 'developers', 'publishers', 'categories', 'supported_languages', 'tags']:
            if col_name in pandas_games_df.columns:
                pandas_games_df[col_name] = pandas_games_df[col_name].apply(lambda x: process_list_column(x, str))

        # Convert release_date to datetime and then to date objects for Spark DateType
        if 'release_date' in pandas_games_df.columns:
            pandas_games_df['release_date'] = pd.to_datetime(pandas_games_df['release_date'], errors='coerce').dt.date

        # --- FIX: Ensure 'current_player_count' is integer type in Pandas before Spark conversion ---
        if 'current_player_count' in pandas_games_df.columns:
            # Convert to numeric, coerce errors to NaN, fill NaN with 0, then cast to integer
            pandas_games_df['current_player_count'] = pd.to_numeric(
                pandas_games_df['current_player_count'], errors='coerce'
            ).fillna(0).astype(np.int64) # Use np.int64 for LongType compatibility
            print(f"   Converted 'current_player_count' to integer type in Pandas DataFrame.")
        # --- END FIX ---
        
        df_games = spark.createDataFrame(pandas_games_df, schema=games_spark_schema)

        # Process twitch DataFrame
        pandas_twitch_df = trusted_con.execute(
            """
            SELECT game AS game_name, viewers, timestamp FROM twitch -- Changed 'game_name' to 'game AS game_name'
            """
        ).fetchdf()
        df_twitch = spark.createDataFrame(pandas_twitch_df, schema=twitch_spark_schema)

        print(f"Loaded {df_users.count()} users, {df_games.count()} games, {df_twitch.count()} twitch records.")

        # --- Pre-calculate total playtime per user (needed for country metrics and user KPIs) ---
        df_users_with_playtime = df_users.withColumn(
            "TotalPlaytime",
            when(col("Owned_Game_PlayedHours").isNotNull() & (expr("size(Owned_Game_PlayedHours)") > 0),
                 # FIXED: Explicitly cast the initial value in aggregate to DOUBLE
                 expr("aggregate(Owned_Game_PlayedHours, CAST(0.0 AS DOUBLE), (acc, x) -> acc + x)").cast(DoubleType()))
            .otherwise(0.0)
        )
        print("Calculated 'TotalPlaytime' for each user.")
        
        # --- KPI Group 1: Game-related KPIs (`game_metrics`) ---
        print("\nCalculating Game-related KPIs...")

        twitch_game_summary = df_twitch.groupBy("game_name") \
                                       .agg(sum("viewers").alias("TotalTwitchViewers"))

        # --- FIX STARTS HERE ---
        df_user_game_playtime = df_users.select("SteamID", arrays_zip("Owned_Game_AppIDs", "Owned_Game_PlayedHours").alias("game_details")) \
                                         .withColumn("game_detail", explode(col("game_details"))) \
                                         .select(
                                             col("SteamID"),
                                             # Corrected field access
                                             col("game_detail.Owned_Game_AppIDs").alias("appid"),
                                             col("game_detail.Owned_Game_PlayedHours").alias("played_hours")
                                         ).filter(col("appid").isNotNull() & col("played_hours").isNotNull())
        # --- FIX ENDS HERE ---

        game_playtime_summary = df_user_game_playtime.groupBy("appid") \
                                                     .agg(
                                                         sum("played_hours").alias("TotalPlayedHoursByUsers"),
                                                         countDistinct("SteamID").alias("NumberOfOwners")
                                                     )

        df_game_metrics = df_games.alias("g").join(
            twitch_game_summary.alias("t"),
            lower(col("g.name")) == lower(col("t.game_name")),
            "left_outer"
        ).join(
            game_playtime_summary.alias("p"),
            col("g.appid") == col("p.appid"),
            "left_outer"
        ).select(
            col("g.appid"),
            col("g.name").alias("GameName"),
            col("g.price").alias("Price"),
            coalesce(col("g.current_player_count"), lit(0)).alias("CurrentPlayerCount"),
            coalesce(col("t.TotalTwitchViewers"), lit(0)).alias("TotalTwitchViewers"),
            coalesce(col("p.TotalPlayedHoursByUsers"), lit(0.0)).alias("TotalPlayedHoursByUsers"),
            coalesce(col("p.NumberOfOwners"), lit(0)).alias("NumberOfOwners")
        ).orderBy(col("NumberOfOwners").desc(), col("CurrentPlayerCount").desc(), col("TotalPlayedHoursByUsers").desc())
        
        print("KPIs for `game_metrics` calculated.")
        df_game_metrics.show(5, truncate=False) # Display all columns

        # --- KPI Group 2: Country-related KPIs (`country_metrics`) ---
        print("\nCalculating Country-related KPIs...")

        kpi_user_count_by_country = df_users.groupBy("Country") \
                                             .agg(spark_count(col("SteamID")).alias("UserCount")) # Use spark_count

        kpi_avg_steam_level_by_country = df_users.groupBy("Country") \
                                                 .agg(round(avg("Steam_Level"), 2).alias("AverageSteamLevel"),
                                                      min("Steam_Level").alias("MinSteamLevel"),
                                                      max("Steam_Level").alias("MaxSteamLevel"))

        kpi_avg_playtime_by_country = df_users_with_playtime.groupBy("Country") \
                                                             .agg(round(avg("TotalPlaytime"), 2).alias("AverageTotalPlaytime"),
                                                                  spark_count(when(col("TotalPlaytime") > 1000, True)).alias("HighPlaytimeUserCount")) # Use spark_count

        kpi_avg_owned_games_by_country = df_users.groupBy("Country") \
                                                 .agg(round(avg("Owned_Games_Count"), 2).alias("AverageOwnedGamesPerUser"))

        df_country_metrics = kpi_user_count_by_country.alias("uc").join(
            kpi_avg_steam_level_by_country.alias("sl"),
            col("uc.Country") == col("sl.Country"),
            "full_outer"
        ).join(
            kpi_avg_playtime_by_country.alias("pt"),
            coalesce(col("uc.Country"), col("sl.Country")) == col("pt.Country"),
            "full_outer"
        ).join(
            kpi_avg_owned_games_by_country.alias("aog"),
            coalesce(col("uc.Country"), col("sl.Country"), col("pt.Country")) == col("aog.Country"),
            "full_outer"
        ).select(
            coalesce(col("uc.Country"), col("sl.Country"), col("pt.Country"), col("aog.Country")).alias("Country"),
            coalesce(col("uc.UserCount"), lit(0)).alias("UserCount"),
            coalesce(col("sl.AverageSteamLevel"), lit(0.0)).alias("AverageSteamLevel"),
            coalesce(col("sl.MinSteamLevel"), lit(0)).alias("MinSteamLevel"),
            coalesce(col("sl.MaxSteamLevel"), lit(0)).alias("MaxSteamLevel"),
            coalesce(col("pt.AverageTotalPlaytime"), lit(0.0)).alias("AverageTotalPlaytime"),
            coalesce(col("pt.HighPlaytimeUserCount"), lit(0)).alias("HighPlaytimeUserCount"),
            coalesce(col("aog.AverageOwnedGamesPerUser"), lit(0.0)).alias("AverageOwnedGamesPerUser")
        ).orderBy(col("UserCount").desc())
        print("KPIs for `country_metrics` calculated.")
        df_country_metrics.show(5, truncate=False) # Display all columns


        # --- KPI Group 3: User-specific KPIs (`user_kpis`) ---
        print("\nCalculating User-specific KPIs...")

        df_user_owned_game_names = df_users.select("SteamID", explode(
            when(col("Owned_Game_AppIDs").isNotNull(), col("Owned_Game_AppIDs")).otherwise(lit(None))
        ).alias("appid")) \
                                           .join(df_games.select("appid", "name"), "appid", "left_outer") \
                                           .groupBy("SteamID") \
                                           .agg(collect_list(when(col("name").isNotNull(), col("name"))).alias("ListOfOwnedGames")) \
                                           .withColumn("ListOfOwnedGames",
                                                       when(col("ListOfOwnedGames").isNull(), lit([])).otherwise(col("ListOfOwnedGames")))


        df_user_kpis = df_users_with_playtime.select(
            col("SteamID"),
            col("Username"),
            col("Country"),
            col("Steam_Level"),
            col("Owned_Games_Count"),
            col("TotalPlaytime"),
            when(col("Owned_Games_Count") > 0, round(col("TotalPlaytime") / col("Owned_Games_Count"), 2)).otherwise(0.0).alias("AvgPlaytimePerOwnedGame"),
            when(col("TotalPlaytime") > 1000, True).otherwise(False).alias("HasHighPlaytime")
        ).join(df_user_owned_game_names, "SteamID", "left_outer") \
        .orderBy(col("TotalPlaytime").desc(), col("Owned_Games_Count").desc())

        print("KPIs for `user_kpis` calculated.")
        df_user_kpis.show(5, truncate=False) # Display all columns

        # --- KPI Group 4: Overall Summary KPIs (`overall_summary_kpis`) ---
        print("\nCalculating Overall Summary KPIs...")

        kpi_total_users = df_users.count()
        kpi_total_games = df_games.count()
        kpi_total_twitch_records = df_twitch.count()
        kpi_overall_avg_steam_level = df_users.agg(round(avg("Steam_Level"), 2).alias("AvgSteamLevel")).collect()[0]["AvgSteamLevel"]
        kpi_overall_avg_current_players = df_games.agg(round(avg("current_player_count"), 2).alias("AvgCurrentPlayers")).collect()[0]["AvgCurrentPlayers"]
        kpi_most_expensive_game_price = df_games.agg(max("price").alias("MaxPrice")).collect()[0]["MaxPrice"]
        
        kpi_oldest_game_release_date_row = df_games.agg(min("release_date").alias("MinDate")).collect()[0]
        kpi_oldest_game_release_date = kpi_oldest_game_release_date_row["MinDate"].isoformat() if kpi_oldest_game_release_date_row["MinDate"] else None

        kpi_newest_game_release_date_row = df_games.agg(max("release_date").alias("MaxDate")).collect()[0]
        kpi_newest_game_release_date = kpi_newest_game_release_date_row["MaxDate"].isoformat() if kpi_newest_game_release_date_row["MaxDate"] else None

        top_game_row = df_games.orderBy(col("current_player_count").desc()).select("name", "current_player_count").limit(1).collect()
        top_game_name = top_game_row[0]["name"] if top_game_row else None
        top_game_players = top_game_row[0]["current_player_count"] if top_game_row else None

        game_with_most_owners_row = game_playtime_summary.orderBy(col("NumberOfOwners").desc()).limit(1).select("appid", "NumberOfOwners").collect()
        game_with_most_owners_appid = game_with_most_owners_row[0]["appid"] if game_with_most_owners_row else None
        game_with_most_owners_count = game_with_most_owners_row[0]["NumberOfOwners"] if game_with_most_owners_row else None

        game_with_most_owners_name = None
        if game_with_most_owners_appid:
            game_name_row = df_games.filter(col("appid") == game_with_most_owners_appid).select("name").limit(1).collect()
            game_with_most_owners_name = game_name_row[0]["name"] if game_name_row else "Unknown Game"

        game_with_most_played_hours_row = game_playtime_summary.orderBy(col("TotalPlayedHoursByUsers").desc()).limit(1).select("appid", "TotalPlayedHoursByUsers").collect()
        game_with_most_played_hours_appid = game_with_most_played_hours_row[0]["appid"] if game_with_most_played_hours_row else None
        game_with_most_played_hours_total = game_with_most_played_hours_row[0]["TotalPlayedHoursByUsers"] if game_with_most_played_hours_row else None

        game_with_most_played_hours_name = None
        if game_with_most_played_hours_appid:
            game_name_row = df_games.filter(col("appid") == game_with_most_played_hours_appid).select("name").limit(1).collect()
            game_with_most_played_hours_name = game_name_row[0]["name"] if game_name_row else "Unknown Game"


        overall_summary_data = {
            "TotalUsers": kpi_total_users,
            "TotalGames": kpi_total_games,
            "TotalTwitchRecords": kpi_total_twitch_records,
            "OverallAverageSteamLevel": kpi_overall_avg_steam_level,
            "OverallAverageCurrentPlayersPerGame": kpi_overall_avg_current_players,
            "MostExpensiveGamePrice": kpi_most_expensive_game_price,
            "OldestGameReleaseDate": kpi_oldest_game_release_date,
            "NewestGameReleaseDate": kpi_newest_game_release_date,
            "TopCurrentPlayersGameName": top_game_name,
            "TopCurrentPlayersCount": top_game_players,
            "GameWithMostOwnersName": game_with_most_owners_name,
            "GameWithMostOwnersCount": game_with_most_owners_count,
            "GameWithMostPlayedHoursName": game_with_most_played_hours_name,
            "GameWithMostPlayedHoursTotal": game_with_most_played_hours_total
        }
        df_overall_summary_kpis = spark.createDataFrame([overall_summary_data])
        print("KPIs for `overall_summary_kpis` calculated.")
        df_overall_summary_kpis.show(truncate=False) # Display all columns


        # --- Store KPIs in Exploitation DuckDB ---
        print("\nStoring calculated KPIs in Exploitation DuckDB...")

        exploit_con.execute("DROP TABLE IF EXISTS game_metrics")
        exploit_con.register("spark_df_game_metrics", df_game_metrics.toPandas())
        exploit_con.execute("CREATE TABLE game_metrics AS SELECT * FROM spark_df_game_metrics")
        print("   Table `game_metrics` stored.")

        exploit_con.execute("DROP TABLE IF EXISTS country_metrics")
        exploit_con.register("spark_df_country_metrics", df_country_metrics.toPandas())
        exploit_con.execute("CREATE TABLE country_metrics AS SELECT * FROM spark_df_country_metrics")
        print("   Table `country_metrics` stored.")

        exploit_con.execute("DROP TABLE IF EXISTS user_metrics")
        exploit_con.register("spark_df_user_kpis", df_user_kpis.toPandas())
        exploit_con.execute("CREATE TABLE user_metrics AS SELECT * FROM spark_df_user_kpis")
        print("   Table `user_metrics` stored.")
        
        exploit_con.execute("DROP TABLE IF EXISTS overall_summary_kpis")
        exploit_con.register("spark_df_overall_summary_kpis", df_overall_summary_kpis.toPandas())
        exploit_con.execute("CREATE TABLE overall_summary_kpis AS SELECT * FROM spark_df_overall_summary_kpis")
        print("   Table `overall_summary_kpis` stored.")

    except Exception as e:
        print(f"An error occurred during KPI calculation or storage: {e}")
        raise

    finally:
        trusted_con.close()
        exploit_con.close()
        spark.stop()
        print("\n--- KPI generation and storage process completed. ---")

if __name__ == "__main__":
    run()