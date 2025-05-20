from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from Twitch.TwitchDataLoader import fetch_and_store_twitch_data
from Steam.SteamDataLoader import fetch_and_store_steam_data
from Youtube.YoutubeDataLoader import fetch_and_store_youtube_data
from MongoLoader.mongo_loader import run as mongo_loader 
from InfluxLoader.influx_loader import run as influx_loader

def run_steam_data_loader(**kwargs):
    fetch_and_store_steam_data()

def run_youtube_data_loader(**kwargs):
    fetch_and_store_youtube_data()

def run_twitch_data_loader(**kwargs):
    fetch_and_store_twitch_data()

def run_mongo_loader(**kwargs):
    mongo_loader()

def run_influx_loader(**kwargs):
    influx_loader()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='twitch_data_loader_dag',
    default_args=default_args,
    description='DAG to execute TwitchDataLoader',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['twitch', 'data'],
) as dag:
    with TaskGroup("collect_data") as collect_data:
        run_steam_loader = PythonOperator(
            task_id='run_steam_data_loader',
            python_callable=run_steam_data_loader,
            provide_context=True,
        )
        run_youtube_loader = PythonOperator(
            task_id='run_youtube_data_loader',
            python_callable=run_youtube_data_loader,
            provide_context=True,
        )
        run_twitch_loader = PythonOperator(
            task_id='run_twitch_data_loader',
            python_callable=run_twitch_data_loader,
            provide_context=True,
        )

    with TaskGroup("load_data") as load_data:
        run_mongo_loader_task = PythonOperator(
            task_id='run_mongo_loader',
            python_callable=run_mongo_loader,
            provide_context=True,
        )
        run_influx_loader_task = PythonOperator(
            task_id='run_influx_loader',
            python_callable=run_influx_loader,
            provide_context=True,
        )

    collect_data >> load_data
