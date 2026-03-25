# MDS-BDM Game Recommender for Content Creators

Big Data Management project for the UPC Master in Data Science. The repository contains a small end-to-end data platform that collects gaming-related data, processes it through trusted and exploitation layers, and exposes the results through a Streamlit dashboard with analytics and game recommendations.

## What the project does

The platform combines data from several gaming/content sources:

- Steam game and user data
- Twitch viewer activity
- YouTube game-related view data
- ProtonDB compatibility summaries

The data is orchestrated with Airflow, stored across multiple technologies, transformed into analytics-ready tables, and consumed by a dashboard that offers:

- service quick links
- gaming analytics visualizations
- a simple game recommender based on owned-game overlap

## Architecture overview

At a high level, the project follows this flow:

1. Data collectors fetch or load raw source data.
2. Raw records are written to Delta Lake files under `data/delta`.
3. Trusted-zone loaders clean and store curated data in:
   - DuckDB
   - MongoDB
   - InfluxDB
4. Exploitation-zone loaders derive analytical tables for dashboard consumption.
5. Streamlit reads the exploitation DuckDB database and renders charts and recommendations.

Main orchestration is defined in Airflow:

- `dags/Dag.py`: daily full pipeline
- `dags/TwitchDAG.py`: Twitch ingestion every 5 minutes

## Main services

The default `docker-compose.yml` starts the following services:

- `zookeeper` and `kafka`
- `spark-master` and `spark-worker`
- `postgres` for Airflow metadata
- `airflow-init`, `airflow-webserver`, `airflow-scheduler`
- `mongo-trusted`, `mongo-exploitation`, `mongo-proton`
- `mongo-express-trusted`, `mongo-express-exploitation`
- `influx-trusted`
- `proton` API service
- `streamlit-dashboard`

## Exposed ports

After startup, these are the main local entry points:

- `http://localhost:8501` - Streamlit dashboard
- `http://localhost:8082` - Airflow web UI
- `http://localhost:8080` - Spark master UI
- `http://localhost:8081` - Mongo Express for trusted zone
- `http://localhost:8083` - Mongo Express for exploitation zone
- `http://localhost:8087` - InfluxDB
- `http://localhost:3000` - Proton community API
- `localhost:9092` - Kafka broker

## Repository structure

```text
.
|-- airflow/                 Custom Airflow image and Python dependencies
|-- config/                  Airflow configuration
|-- dags/                    Airflow DAG definitions
|-- src/
|   |-- Steam/               Steam ingestion and Delta writers
|   |-- Twitch/              Twitch ingestion and Delta writers
|   |-- Youtube/             YouTube ingestion and Delta writers
|   |-- Protondb/            ProtonDB ingestion and processing
|   |-- MongoLoader/         Mongo trusted/exploitation loaders
|   |-- InfluxLoader/        Twitch to Influx trusted-zone loader
|   |-- DuckLoader/          DuckDB trusted/exploitation loaders
|   |-- Frontend/            Streamlit dashboard
|   |-- spark/               Spark connector utilities
|   `-- protondb-community-api/
|-- data/                    Generated data volumes and databases
|-- logs/                    Airflow logs
|-- docker-compose.yml       Local multi-service environment
`-- README.md
```

## Prerequisites

To run the project locally you need:

- Docker
- Docker Compose v2

Recommended:

- at least 8 GB RAM available to Docker
- a clean `data/` directory if you want a fresh run

## Quick start

Build and start everything with Docker Compose:

```bash
docker compose build
docker compose up
```

To start in detached mode:

```bash
docker compose up -d
```

To stop the stack:

```bash
docker compose down
```

To rebuild after code changes:

```bash
docker compose build
docker compose up -d
```

## First things to check after startup

1. Open the dashboard at `http://localhost:8501`.
2. Open Airflow at `http://localhost:8082`.
3. Confirm that the DAGs appear:
   - `all_data_loader_dag`
   - `twitch_data_loader_dag`
4. Trigger `all_data_loader_dag` manually if you want to populate the full pipeline immediately.

Airflow credentials created by `airflow-init`:

- username: `admin`
- password: `admin`

## Data storage layers

The project uses different storage technologies for different stages:

- Delta Lake:
  raw/intermediate ingestion output under `data/delta`
- DuckDB trusted zone:
  cleaned source data, for example Steam users, Steam games, and Twitch
- DuckDB exploitation zone:
  analytical tables used by the dashboard:
  - `game_metrics`
  - `country_metrics`
  - `user_metrics`
- MongoDB trusted zone:
  curated ProtonDB records
- MongoDB exploitation zone:
  downstream exploitation data
- InfluxDB trusted zone:
  time-series Twitch viewer metrics

## Environment variables and API keys

Some components can run from bundled local files, while others require live API credentials.

Variables referenced in the codebase include:

- `TWITCH_CLIENT_ID`
- `TWITCH_CLIENT_SECRET`
- `TWITCH_FETCH_INTERVAL`
- `YOUTUBE_API_KEY`
- `STEAM_KEY`
- `HF_TOKEN`

Notes:

- Twitch ingestion expects valid Twitch API credentials.
- YouTube ingestion expects a YouTube API key.
- Steam code references `STEAM_KEY`, but parts of the loader currently use bundled JSON/CSV data files.
- `HF_TOKEN` is needed for the Proton exploitation/comment-generation components.
- Several Docker Compose service credentials are currently hardcoded for local development, for example MongoDB, InfluxDB, and Airflow.

If you want live ingestion, create a local `.env` file at the project root with the required values before starting the stack.

Example:

```env
TWITCH_CLIENT_ID=your_twitch_client_id
TWITCH_CLIENT_SECRET=your_twitch_client_secret
TWITCH_FETCH_INTERVAL=300
YOUTUBE_API_KEY=your_youtube_api_key
STEAM_KEY=your_steam_api_key
HF_TOKEN=your_huggingface_token
```

## Dashboard features

The Streamlit app in `src/Frontend/app.py` provides three areas:

- Quick links to infrastructure services
- Gaming analytics based on exploitation-zone DuckDB tables
- A basic recommender that suggests games from owned-game co-occurrence

The dashboard reads from:

- `/data/exploitation_zone/explotation.duckdb`

If the dashboard opens but shows empty tables or warnings, the exploitation loaders likely have not populated the database yet.

## Development notes

- The project is designed primarily for local Docker-based execution.
- Some optional services are present in `docker-compose.yml` but commented out.
- The current compose file mounts project folders directly into containers, so local code changes can affect running services.
- There are a few naming inconsistencies in the repository, such as `explotation` instead of `exploitation`, and the README keeps those names where they reflect actual file paths.

## Useful commands

View running containers:

```bash
docker compose ps
```

Inspect logs for a service:

```bash
docker compose logs airflow-scheduler
docker compose logs streamlit-dashboard
docker compose logs proton
```

Restart a single service:

```bash
docker compose restart airflow-webserver
```

Remove the stack and volumes:

```bash
docker compose down -v
```

Use the last command carefully because it will remove persisted local container data.

## Troubleshooting

### Dashboard has no data

- Check whether `all_data_loader_dag` has run successfully.
- Confirm that DuckDB files were created under `data/trusted_zone` and `data/exploitation_zone`.
- Inspect Airflow scheduler and task logs.

### Airflow starts but DAG tasks fail

- Confirm all dependent services are healthy.
- Check container logs for Spark, MongoDB, Proton API, and InfluxDB.
- Verify required API keys are available if running live ingestion.

### Mongo Express or Airflow UI is unreachable

- Wait a little longer after `docker compose up`; some services need time to initialize.
- Run `docker compose ps` to confirm containers are still running.

## License

This repository includes a `LICENSE` file at the project root.
