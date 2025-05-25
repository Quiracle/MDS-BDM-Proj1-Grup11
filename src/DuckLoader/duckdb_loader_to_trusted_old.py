import duckdb
import os

os.makedirs("/data/trusted_zone/duckdb", exist_ok=True)
db_path = "/data/trusted_zone/duckdb/steam_data.db"

con = duckdb.connect(database=db_path, read_only=False)

# Crear tablas desde Delta Parquet
con.execute("""
CREATE TABLE IF NOT EXISTS games AS 
SELECT * FROM read_parquet('/data/delta/Steam_games_data_*/*.parquet');
""")

con.execute("""
CREATE TABLE IF NOT EXISTS users AS 
SELECT * FROM read_parquet('/data/delta/Steam_user_data_*/*.parquet');
""")

con.execute("""
CREATE TABLE IF NOT EXISTS twitch AS 
SELECT * FROM read_parquet('/data/delta/twitch_data/*.parquet');
""")

# View para explotación
con.execute("""
CREATE VIEW IF NOT EXISTS top_games AS 
SELECT game_name, SUM(viewers) AS total
FROM twitch
GROUP BY game_name
ORDER BY total DESC
LIMIT 10;
""")

print(" DuckDB tables created under trusted zone")