import duckdb
import os

def run():
    # Paths
    trusted_db_path = "/data/trusted_zone/duckdb/trusted.duckdb"
    exploitation_db_dir = "/data/exploitation_zone/duckdb"
    os.makedirs(exploitation_db_dir, exist_ok=True)
    exploitation_db_path = os.path.join(exploitation_db_dir, "explotation.duckdb")

    # Connect to source (trusted) and target (exploitation) DuckDBs
    trusted_con = duckdb.connect(database=trusted_db_path, read_only=True)
    exploit_con = duckdb.connect(database=exploitation_db_path, read_only=False)

    # List of tables/views to export
    objects_to_export = ["steam_games", "steam_users", "twitch"]  # Matches tables from the trusted loader

    # Export each object into the exploitation DB
    for obj in objects_to_export:
        try:
            df = trusted_con.execute(f"SELECT * FROM {obj}").fetchdf()
            exploit_con.execute(f"DROP TABLE IF EXISTS {obj}")
            exploit_con.register("df", df)
            exploit_con.execute(f"CREATE TABLE {obj} AS SELECT * FROM df")
            print(f" Table `{obj}` exported successfully.")
        except Exception as e:
            print(f" Failed to export `{obj}`: {e}")

    print("DuckDb Data export from trusted to exploitation zone completed.")

if __name__ == "__main__":
    run()
