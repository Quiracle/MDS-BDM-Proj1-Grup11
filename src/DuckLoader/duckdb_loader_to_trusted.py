import os
import glob
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, regexp_replace, datediff, to_date,
    current_date, when, lit, size, avg, countDistinct, to_timestamp, current_timestamp,
    mean, stddev, transform, element_at, coalesce,
    instr, substring_index, regexp_extract, split # Added split for tag_ids in twitch
)
from pyspark.sql.types import (
    IntegerType, LongType, StringType, FloatType, DateType, TimestampType,
    StructType, StructField, ArrayType, MapType, DoubleType
)

# Define a constant for URL regex
URL_REGEX = r"^(http|https):\/\/[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,3}(\/\S*)?$"

# --- Table-Specific Cleaning Functions ---

def _clean_steam_users_df(df, cleaning_rules):
    """
    Applies specific cleaning rules for the 'steam_users' DataFrame.
    """
    print("   Applying steam_users specific cleaning rules...")

    # SPECIAL HANDLING FOR 'Owned_Games'
    # Check if 'Owned_Games' column exists in the DataFrame
    if "Owned_Games" in df.columns:
        current_owned_games_type = df.schema["Owned_Games"].dataType

        if isinstance(current_owned_games_type, ArrayType) and \
           isinstance(current_owned_games_type.elementType, MapType):
            print(f"   Extracting 'appid' and 'played_hours' from 'Owned_Games' (type: {current_owned_games_type}).")

            df = df.withColumn(
                "Owned_Game_AppIDs",
                transform(
                    col("Owned_Games"),
                    lambda x: coalesce(x.getItem("appid"), x.getItem("app_id"), lit(None)).cast(StringType())
                )
            )
            df = df.withColumn(
                "Owned_Game_PlayedHours",
                transform(
                    col("Owned_Games"),
                    lambda x: coalesce(x.getItem("played_hours"), x.getItem("playtime_forever"), lit(None)).cast(LongType())
                )
            )
            df = df.withColumn(
                "Owned_Games_Count",
                when(col("Owned_Games").isNotNull(), size(col("Owned_Games"))).otherwise(0)
            )
            df = df.drop("Owned_Games")
            print(f"   Created new columns: 'Owned_Game_AppIDs', 'Owned_Game_PlayedHours', 'Owned_Games_Count'. Original 'Owned_Games' column dropped.")

        elif isinstance(current_owned_games_type, StringType):
            print(f"   Casting 'Owned_Games' from StringType to LongType (assuming it's a count).")
            # Try to extract a numeric count from the string, defaulting to 0 if not found
            df = df.withColumn("Owned_Games_Count", regexp_extract(col("Owned_Games"), r"(\d+)", 1).cast(LongType()))
            # Fill any resulting NULLs from cast with 0
            df = df.withColumn("Owned_Games_Count", when(col("Owned_Games_Count").isNull(), lit(0)).otherwise(col("Owned_Games_Count")))
            df = df.drop("Owned_Games")
            df = df.withColumn("Owned_Game_AppIDs", lit(None).cast(ArrayType(StringType())))
            df = df.withColumn("Owned_Game_PlayedHours", lit(None).cast(ArrayType(LongType())))
        else:
            print(f"   'Owned_Games' is already {current_owned_games_type}. Adding new columns with nulls/existing count.")
            if "Owned_Games_Count" not in df.columns:
                df = df.withColumnRenamed("Owned_Games", "Owned_Games_Count") # If it's already a number, just rename
            df = df.withColumn("Owned_Game_AppIDs", lit(None).cast(ArrayType(StringType())))
            df = df.withColumn("Owned_Game_PlayedHours", lit(None).cast(ArrayType(LongType())))
    else:
        print(f"   'Owned_Games' column not found in DataFrame. Adding placeholder columns.")
        df = df.withColumn("Owned_Games_Count", lit(0).cast(LongType()))
        df = df.withColumn("Owned_Game_AppIDs", lit(None).cast(ArrayType(StringType())))
        df = df.withColumn("Owned_Game_PlayedHours", lit(None).cast(ArrayType(LongType())))


    if "SteamID" in df.columns:
        pre_steam_id_filter_count = df.count()
        df = df.filter(col("SteamID").rlike(r"^\d+$"))
        dropped_count = pre_steam_id_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows due to non-numeric 'SteamID'.")

    if "Owned_Games_Count" in df.columns and "Owned_Games_min" in cleaning_rules:
        min_val = cleaning_rules["Owned_Games_min"]
        pre_filter_count = df.count()
        df = df.filter(col("Owned_Games_Count").isNotNull() & (col("Owned_Games_Count") >= min_val))
        dropped_count = pre_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows where 'Owned_Games_Count' was null or less than {min_val}.")

    if "Steam_Level" in df.columns and "Steam_Level_min" in cleaning_rules:
        min_val = cleaning_rules["Steam_Level_min"]
        pre_filter_count = df.count()
        df = df.filter(col("Steam_Level").isNotNull() & (col("Steam_Level") >= min_val))
        dropped_count = pre_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows where 'Steam_Level' was null or less than {min_val}.")

    if "Country" in df.columns and "Country_standardize" in cleaning_rules:
        country_map = cleaning_rules["Country_standardize"]
        country_col = col("Country")
        for raw, std in country_map.items():
            country_col = when(country_col == raw, std).otherwise(country_col)
        df = df.withColumn("Country", country_col)
        print(f"   Applied country standardization for 'Country'.")

    if "Profile_URL" in df.columns and cleaning_rules.get("Profile_URL_validate"):
        pre_url_filter_count = df.count()
        df = df.filter(col("Profile_URL").rlike(URL_REGEX) | col("Profile_URL").isNull())
        dropped_count = pre_url_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows due to invalid 'Profile_URL'.")

    return df

def _clean_steam_games_df(df, cleaning_rules):
    """
    Applies specific cleaning rules for the 'steam_games' DataFrame,
    including robust price cleaning and explicit release_date parsing.
    """
    print("   Applying steam_games specific cleaning rules...")

    # --- PRICE CLEANING ---
    # Determine the correct price column name (case-insensitive check for "price" or "Price")
    price_col_name = None
    if "price" in df.columns:
        price_col_name = "price"
    elif "Price" in df.columns:
        price_col_name = "Price"
    
    if price_col_name:
        # Step 1: Handle "Free", NULL, or empty strings by setting to "0.0" as a string.
        df = df.withColumn("price_temp",
                           when(lower(col(price_col_name)) == "free", lit("0.0"))
                           .when(col(price_col_name).isNull() | (trim(col(price_col_name)) == ""), lit("0.0"))
                           .otherwise(col(price_col_name)))

        # Step 2: Remove all characters that are NOT digits, dots, or commas.
        df = df.withColumn("price_temp", regexp_replace(col("price_temp"), "[^0-9.,]", ""))

        # Step 3: Smartly handle decimal vs. thousands separators.
        df = df.withColumn("price_cleaned_str",
            when(
                (col("price_temp").contains(".") & col("price_temp").contains(",")),
                when(instr(col("price_temp"), ".") > instr(col("price_temp"), ","), # e.g. 1.234,56 (dot is thousands, comma is decimal)
                     regexp_replace(col("price_temp"), ",", "")) # Remove thousands comma, then cast
                .otherwise( # e.g. 1,234.56 (comma is thousands, dot is decimal)
                     regexp_replace(regexp_replace(col("price_temp"), "\\.", ""), ",", ".")) # Remove thousands dot, replace thousands comma with decimal dot
            ).when(
                col("price_temp").contains(","), # Only comma, assume decimal
                regexp_replace(col("price_temp"), ",", ".")
            ).otherwise( # Only dot or no separator, assume dot is decimal or no need to change
                col("price_temp")
            )
        )

        # Step 4: Cast the cleaned string to DoubleType.
        df = df.withColumn("price", col("price_cleaned_str").cast(DoubleType()))

        # Step 5: Fill any NULLs resulting from casting errors with 0.0.
        df = df.withColumn("price",
                           when(col("price").isNull(), lit(0.0))
                           .otherwise(col("price")))

        # Apply the price filter from your cleaning rules:
        if "price_min" in cleaning_rules:
            min_price = cleaning_rules["price_min"]
            pre_price_filter_count = df.count()
            df = df.filter(col("price").isNotNull() & (col("price") >= min_price))
            dropped_count = pre_price_filter_count - df.count()
            if dropped_count > 0:
                print(f"   Dropped {dropped_count} rows where 'price' was null or less than {min_price} after cleaning.")
            print(f"   After price range filtering: Current row count: {df.count()}")
    else:
        print(f"   Warning: Neither 'price' nor 'Price' column found for price cleaning. Adding default 'price' column as 0.0.")
        df = df.withColumn("price", lit(0.0).cast(DoubleType()))

    # Drop temporary price column
    df = df.drop("price_temp", "price_cleaned_str")
    if price_col_name and price_col_name != "price" and price_col_name in df.columns: # Only drop if original column name was different and still exists
        df = df.drop(price_col_name)


    # --- RELEASE DATE CLEANING ---
    # The schema shows 'release_date' as string. The format is 'DD Mon, YYYY'.
    # Example: "21 Aug, 2012", "29 Aug, 2024"
    # Determine the correct release_date column name (case-insensitive check for "release_date" or "Release Date")
    release_date_col_name = None
    if "release_date" in df.columns:
        release_date_col_name = "release_date"
    elif "Release Date" in df.columns:
        release_date_col_name = "Release Date"

    if release_date_col_name:
        if isinstance(df.schema[release_date_col_name].dataType, StringType):
            print(f"   Explicitly parsing '{release_date_col_name}' from StringType to DateType using format 'd MMM, yyyy'.")
            print(f"   First 20 non-truncated values for '{release_date_col_name}' before parsing:")
            df.select(release_date_col_name).show(20, truncate=False)

            # Parse the string 'release_date' into a proper DateType
            df = df.withColumn("release_date_parsed", to_date(col(release_date_col_name), "d MMM, yyyy"))

            # Check for NULLs created by parsing errors
            parsed_null_count = df.filter(col("release_date_parsed").isNull()).count()
            if parsed_null_count > 0:
                print(f"   Warning: {parsed_null_count} rows have NULL 'release_date_parsed' after parsing. Showing problematic string values:")
                df.filter(col("release_date_parsed").isNull()).select(release_date_col_name).show(20, False)
                # These rows will be dropped by the future_date_check if release_date_parsed is still NULL.

            # Overwrite the original 'release_date' with the parsed date, if it was a different column name
            if release_date_col_name != "release_date":
                df = df.withColumn("release_date", col("release_date_parsed"))
                df = df.drop(release_date_col_name) # Drop the original column
            else:
                df = df.withColumn("release_date", col("release_date_parsed")) # Overwrite itself
            df = df.drop("release_date_parsed") # Drop the temporary column
            print(f"   'release_date' column successfully parsed to DateType.")
        elif not isinstance(df.schema[release_date_col_name].dataType, DateType):
            print(f"   Warning: '{release_date_col_name}' column is of type {df.schema[release_date_col_name].dataType}. Attempting to cast to DateType.")
            df = df.withColumn("release_date", col(release_date_col_name).cast(DateType()))
            if release_date_col_name != "release_date":
                df = df.drop(release_date_col_name)
    else:
        print(f"   Warning: Neither 'release_date' nor 'Release Date' column found. Adding as NULL DateType.")
        df = df.withColumn("release_date", lit(None).cast(DateType()))


    # Apply the release_date future check AFTER parsing
    if "release_date" in df.columns and cleaning_rules.get("release_date_future_check"):
        pre_date_filter_count = df.count()
        # Filter out rows where release_date is NULL (due to parsing errors or missing) or in the future
        df = df.filter(col("release_date").isNotNull() & (col("release_date") <= current_date()))
        dropped_count = pre_date_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows with future or null 'release_date'.")
        print(f"   After release_date future check: Current row count: {df.count()}")


    # Generic array column cleaning for genres, tags, categories, developers, publishers
    array_columns = ["developers", "publishers", "categories", "genres", "tags"]
    for col_name in array_columns:
        # Check for case variations
        actual_col_name = None
        if col_name in df.columns:
            actual_col_name = col_name
        elif col_name.capitalize() in df.columns: # e.g., "Developers"
            actual_col_name = col_name.capitalize()
        elif col_name.upper() in df.columns: # e.g., "GENRES"
            actual_col_name = col_name.upper()

        if actual_col_name:
            print(f"   Before checking array column '{actual_col_name}': Current row count: {df.count()}")
            # Rename to consistent lowercase if needed
            if actual_col_name != col_name:
                df = df.withColumnRenamed(actual_col_name, col_name)
                actual_col_name = col_name # Update to the new name

            null_count = df.filter(col(actual_col_name).isNull()).count()
            if null_count > 0:
                print(f"   Count of NULL values in '{actual_col_name}': {null_count}")
                df = df.filter(col(actual_col_name).isNotNull())
                print(f"   Dropped {null_count} rows with NULL '{actual_col_name}'.")

            empty_count = df.filter(size(col(actual_col_name)) == 0).count()
            if empty_count > 0:
                print(f"   Count of empty arrays in '{actual_col_name}': {empty_count}")
                df = df.filter(size(col(actual_col_name)) > 0)
                print(f"   Dropped {empty_count} rows with empty arrays in '{actual_col_name}'.")
            print(f"   After filtering for '{actual_col_name}': Current row count: {df.count()}")
        else:
            print(f"   Warning: Column '{col_name}' (or its variations) not found. Adding as empty array.")
            df = df.withColumn(col_name, lit(None).cast(ArrayType(StringType())))


    # Deduplication by 'appid'
    if "appid" in df.columns:
        pre_dedup_count = df.count()
        df = df.drop_duplicates(["appid"])
        dedup_count = pre_dedup_count - df.count()
        if dedup_count > 0:
            print(f"   Deduplicated steam_games by 'appid'. Dropped {dedup_count} duplicates.")
        else:
            print(f"   Deduplicated steam_games by 'appid'. No duplicates found.")
    else:
        print(f"   Warning: 'appid' column not found for deduplication.")

    return df

def _clean_twitch_df(df, cleaning_rules):
    """
    Applies specific cleaning rules for the 'twitch' DataFrame.
    """
    print("   Applying twitch specific cleaning rules...")

    # Normalize column names for consistency
    if "viewer_count" in df.columns:
        df = df.withColumnRenamed("viewer_count", "view_count")
    if "started_at" in df.columns:
        df = df.withColumnRenamed("started_at", "start_time")
    if "game_name" in df.columns:
        df = df.withColumnRenamed("game_name", "game")

    if "view_count" in df.columns and "view_count_min" in cleaning_rules:
        min_views = cleaning_rules["view_count_min"]
        pre_filter_count = df.count()
        df = df.filter(col("view_count").isNotNull() & (col("view_count") >= min_views))
        dropped_count = pre_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows where 'view_count' was null or less than {min_views}.")

    if "game_id" in df.columns and "game_id_regex" in cleaning_rules:
        regex = cleaning_rules["game_id_regex"]
        pre_filter_count = df.count()
        df = df.filter(col("game_id").rlike(regex))
        dropped_count = pre_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows due to invalid 'game_id' format.")

    if "start_time" in df.columns and cleaning_rules.get("started_at_past_only"):
        pre_filter_count = df.count()
        # Convert to timestamp if it's not already
        if isinstance(df.schema["start_time"].dataType, StringType):
            # Assuming 'started_at' is in ISO 8601 format or similar that to_timestamp can infer
            df = df.withColumn("start_time", to_timestamp(col("start_time")))
            print(f"   Converted 'start_time' from StringType to TimestampType.")

        df = df.filter(col("start_time").isNotNull() & (col("start_time") <= current_timestamp()))
        dropped_count = pre_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows with future or null 'start_time' timestamps.")
    
    if "language" in df.columns and "language_standardize" in cleaning_rules:
        lang_map = cleaning_rules["language_standardize"]
        lang_col = col("language")
        for raw, std in lang_map.items():
            lang_col = when(lower(lang_col) == lower(lit(raw)), std).otherwise(lang_col) # Case-insensitive match
        df = df.withColumn("language", lang_col)
        print(f"   Applied language standardization for 'language'.")

    if "tag_ids" in df.columns:
        pre_empty_array_filter_count = df.count()
        # Ensure tag_ids is an array type. If it's a string, attempt to parse comma-separated.
        if isinstance(df.schema["tag_ids"].dataType, StringType):
            df = df.withColumn("tag_ids", split(col("tag_ids"), ",").cast(ArrayType(StringType())))
            print("   Attempted to parse 'tag_ids' string to array.")
        
        # Filter for non-null and non-empty arrays
        df = df.filter(col("tag_ids").isNotNull() & (size(col("tag_ids")) > 0))
        dropped_count = pre_empty_array_filter_count - df.count()
        if dropped_count > 0:
            print(f"   Dropped {dropped_count} rows with NULL or empty arrays in 'tag_ids'.")
        print(f"   After filtering for 'tag_ids': Current row count: {df.count()}")


    return df

# --- Main Cleaning Pipeline Dispatcher ---
def clean_data_pipeline(df, table_name, name_field, column_types, cleaning_rules):
    """
    Orchestrates the cleaning process for a DataFrame based on table name and rules.
    """
    initial_row_count = df.count()
    print(f"--- Starting cleaning for {table_name} ---")
    print(f"Initial row count: {initial_row_count}")

    # 1. Initial Type Casting and NULL Handling (for cast failures)
    # This loop is for general columns, specific columns like 'price' and 'release_date' in steam_games
    # are handled in their respective cleaning functions.
    for col_name, col_type in column_types.items():
        if col_name in df.columns:
            # Skip 'price' and 'release_date' for steam_games here, as they are handled in _clean_steam_games_df
            if table_name == "steam_games" and col_name in ["price", "release_date"]:
                print(f"   Skipping initial casting for '{col_name}' in steam_games, handled by specific cleaner.")
                continue
            
            initial_col_type = df.schema[col_name].dataType
            if initial_col_type != col_type:
                print(f"   Casting '{col_name}' from {initial_col_type} to {col_type}.")
                df = df.withColumn(col_name, col(col_name).cast(col_type))
                # Drop rows where casting resulted in NULL for numeric/date types
                if col_type in [IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType]:
                    pre_cast_null_filter_count = df.count()
                    df = df.filter(col(col_name).isNotNull())
                    dropped_count = pre_cast_null_filter_count - df.count()
                    if dropped_count > 0:
                        print(f"   Dropped {dropped_count} rows due to un-castable/null '{col_name}' after type coercion.")
        else:
            print(f"   Warning: Column '{col_name}' specified for type casting not found in {table_name}.")

    # 2. Standardize and Validate Name/Username fields (common for all tables)
    # Check for case variations of the name_field
    actual_name_field = None
    if name_field in df.columns:
        actual_name_field = name_field
    elif name_field.capitalize() in df.columns:
        actual_name_field = name_field.capitalize()
    elif name_field.lower() in df.columns:
        actual_name_field = name_field.lower()

    if actual_name_field:
        # Rename to consistent lowercase if needed
        if actual_name_field != name_field:
            df = df.withColumnRenamed(actual_name_field, name_field)
            actual_name_field = name_field # Update the variable to the new name

        print(f"   Before name field cleaning for '{actual_name_field}': Current row count: {df.count()}")
        print(f"   First 20 non-truncated values for '{actual_name_field}' before cleaning:")
        df.select(actual_name_field).show(20, truncate=False)
        print(f"   Count of NULL values in '{actual_name_field}' before cleaning: {df.filter(col(actual_name_field).isNull()).count()}")
        print(f"   Count of empty string values in '{actual_name_field}' before cleaning: {df.filter(trim(col(actual_name_field)) == '').count()}")

        df = df.withColumn(actual_name_field, lower(col(actual_name_field)))
        df = df.withColumn(actual_name_field, trim(col(actual_name_field)))
        df = df.withColumn(actual_name_field, regexp_replace(col(actual_name_field), " +", " "))
        
        print(f"   After initial standardization for '{actual_name_field}': Current row count: {df.count()}")
        print(f"   First 20 non-truncated values for '{actual_name_field}' after initial standardization:")
        df.select(actual_name_field).show(20, truncate=False)
        print(f"   Count of NULL values in '{actual_name_field}' after initial standardization: {df.filter(col(actual_name_field).isNull()).count()}")
        print(f"   Count of empty string values in '{actual_name_field}' after initial standardization: {df.filter(trim(col(actual_name_field)) == '').count()}")

        # Use specific regex from cleaning rules, fallback to a more relaxed one
        # Note: the regex in your provided config was for 'name' but the variable was 'name_field'. Correcting to use name_field's value.
        name_regex = cleaning_rules.get(f"{name_field}_regex", r"^[a-zA-Z0-9\p{L}\s\-_:.,'!@#$%^&*()+\=\[\]{}|;\"'<>,.?/~`™®©’\u00C0-\u00FF]*$")
        print(f"   Applying regex '{name_regex}' to '{actual_name_field}'.")
        
        pre_name_filter_count = df.count()
        print(f"   First 20 non-matching values for '{actual_name_field}' (will be dropped):")
        df.filter(~col(actual_name_field).rlike(name_regex)).select(actual_name_field).show(20, truncate=False)
        
        df = df.filter(col(actual_name_field).isNotNull() & col(actual_name_field).rlike(name_regex))
        name_filter_dropped_count = pre_name_filter_count - df.count()
        if name_filter_dropped_count > 0:
            print(f"   Dropped {name_filter_dropped_count} rows due to invalid or null '{actual_name_field}'.")
        print(f"   After regex filtering for '{actual_name_field}': Current row count: {df.count()}")
    else:
        print(f"   Warning: '{name_field}' column (or its variations) not found in {table_name}. Skipping name validation.")


    # 3. Table-Specific Cleaning Rules Dispatch
    if table_name == "steam_users":
        df = _clean_steam_users_df(df, cleaning_rules)
    elif table_name == "steam_games":
        df = _clean_steam_games_df(df, cleaning_rules)
    elif table_name == "twitch":
        df = _clean_twitch_df(df, cleaning_rules)
    else:
        print(f"   No specific cleaning rules defined for table: {table_name}.")

    # 4. Drop Duplicates (Final common step)
    pre_dedup_count = df.count()
    if table_name == "steam_games" and "appid" in df.columns:
        df = df.drop_duplicates(["appid"])
        print(f"   Deduplicated {table_name} by 'appid'.")
    elif table_name == "steam_users" and "SteamID" in df.columns: # Added deduplication for steam_users
        df = df.drop_duplicates(["SteamID"])
        print(f"   Deduplicated {table_name} by 'SteamID'.")
    else:
        df = df.dropDuplicates()
        print(f"   Performed general deduplication for {table_name}.")

    dedup_dropped_count = pre_dedup_count - df.count()
    if dedup_dropped_count > 0:
        print(f"   Dropped {dedup_dropped_count} duplicate rows.")

    final_row_count = df.count()
    total_dropped = initial_row_count - final_row_count
    print(f"Final row count after all cleaning for {table_name}: {final_row_count}")
    print(f"Total rows dropped for {table_name}: {total_dropped}")

    if final_row_count == 0:
        print(f"WARNING: No records left for {table_name} after cleaning! Review your cleaning rules.")
    else:
        print(f"{table_name} cleaning completed. Ready for insertion.")

    return df


# --- Main Run Function ---
def run():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DeltaToDuckDB") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    base_path = "/opt/airflow/data/delta"
    db_path = "/opt/airflow/data/trusted_zone/trusted.duckdb"

    con = None # Initialize connection to None
    try:
        con = duckdb.connect(db_path)

        sources = {
            "steam_users": {
                "pattern": "Steam_user_*",
                "name_field": "Username",
                "column_types": {
                    "SteamID": StringType(),
                    "Steam_Level": LongType()
                    # 'Owned_Games' is handled dynamically and doesn't need a direct type cast here
                },
                "cleaning_rules": {
                    "Username_regex": r"^[a-zA-Z0-9 _\-\.\u00C0-\u017F]{1,60}$",
                    "Country_standardize": {
                        "USA": "US", "United States": "US", "UK": "GB", "United Kingdom": "GB",
                        "DEU": "DE", "JPN": "JP", "JP": "JP", "FRA": "FR", "CAN": "CA", "AUS": "AU"
                    },
                    "Steam_Level_min": 0,
                    "Owned_Games_min": 0, # Rule to filter owned games count
                    "Profile_URL_validate": True
                }
            },
            "steam_games": {
                "pattern": "Steam_games_*",
                "name_field": "name", # Assumes 'Name' column is lowercased to 'name'
                "column_types": {
                    # 'price' and 'release_date' casting are handled within _clean_steam_games_df, so remove them from here
                    "appid": StringType(), # Ensure appid is read as string
                    "type": StringType(),
                    "supported_languages": StringType(),
                    "current_player_count": LongType(),
                    "timestamp": TimestampType()
                },
                "cleaning_rules": {
                    "name_regex": r"^[a-zA-Z0-9 \p{L}_\-\.:,']{1,100}$",
                    "price_min": 0.0,
                    "release_date_future_check": True, # Ensure release date is not in the future
                    "current_player_count_min": 0,
                    "timestamp_future_check": True
                }
            },
            "twitch": {
                "pattern": "twitch_data_*",
                "name_field": "title", # Changed from game_name to title for consistency
                "column_types": {
                    "id": StringType(),
                    "user_id": StringType(),
                    "user_login": StringType(),
                    "user_name": StringType(),
                    "game_id": StringType(),
                    "game": StringType(), # Changed from game_name to game
                    "type": StringType(),
                    # "title": StringType(), # Handled by name_field
                    "view_count": LongType(), # Changed from viewers to view_count
                    "start_time": StringType(), # Changed from started_at to start_time. Will be cast to TimestampType in cleaning function
                    "language": StringType(),
                    "thumbnail_url": StringType(),
                    "tag_ids": StringType() # Assuming it might come as a string for parsing
                },
                "cleaning_rules": {
                    "title_regex": r"^.{5,}$", # Title must be at least 5 characters
                    "view_count_min": 10, # Changed from viewers_min to view_count_min
                    "game_id_regex": r"^\d+$", # Game ID should be numeric
                    "started_at_past_only": True, # Original rule name, will apply to 'start_time'
                    "language_standardize": {
                        "en": "English", "es": "Spanish", "fr": "French", "de": "German"
                    }
                }
            }
        }

        for table_name, config in sources.items():
            pattern = config["pattern"]
            name_field = config["name_field"]
            column_types = config["column_types"]
            cleaning_rules = config["cleaning_rules"]

            folders = sorted(glob.glob(os.path.join(base_path, pattern)))
            print(f"\n--- Processing {table_name} ---")
            print(f"Found {len(folders)} folders for {table_name}")

            if not folders:
                print(f"No valid folders found for {table_name}. Skipping.")
                continue

            latest_folder = folders[-1]
            print(f"Using latest folder for {table_name}: {latest_folder}")

            try:
                df = spark.read.format("delta") \
                    .option("mode", "PERMISSIVE") \
                    .option("badRecordsPath", "/opt/airflow/data/bad_records") \
                    .load(latest_folder)
                print(f"Successfully loaded {latest_folder}")
                df.printSchema() # Print schema after loading to check actual column names and types

            except Exception as e:
                print(f"Failed to load {latest_folder} due to: {e}")
                continue

            # --- Call the centralized cleaning pipeline ---
            cleaned_df = clean_data_pipeline(df, table_name, name_field, column_types, cleaning_rules)

            # --- Load to DuckDB with Pandas ---
            if cleaned_df.count() > 0:
                try:
                    pandas_df = cleaned_df.toPandas()
                    print(f"Successfully converted Spark DataFrame to Pandas DataFrame with {len(pandas_df)} records.")

                    con.execute(f"DROP TABLE IF EXISTS {table_name}")
                    con.register("pandas_df_temp", pandas_df)
                    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM pandas_df_temp")
                    print(f"Successfully inserted {len(pandas_df)} records into DuckDB table `{table_name}`.")

                except Exception as e:
                    print(f"Failed to convert to Pandas or load into DuckDB for {table_name} due to: {e}")
            else:
                print(f"No records to insert for {table_name} after cleaning.")

    except Exception as e:
        print(f"An error occurred during the overall data processing: {e}")
    finally:
        if con:
            con.close()
            print("DuckDB connection closed.")
        spark.stop()
        print("\n--- Data loading and cleaning process completed ---")

if __name__ == "__main__":
    run()