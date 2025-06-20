from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from Steam.SteamDataLoader import fetch_and_store_steam_data
from Youtube.YoutubeDataLoader import fetch_and_store_youtube_data
from Protondb.ProtondbAPI import run as fetch_and_store_proton_data
from MongoLoader.proton_trusted_loader import run as mongo_trusted_loader 
from MongoLoader.proton_exploitation_loader import run as mongo_exploitation_loader
from InfluxLoader.influx_loader import run as influx_loader
from DuckLoader.duckdb_loader_to_trusted import run as duck_loader_trusted
from DuckLoader.duckdb_loader_to_explotation import run as duck_loader_explotation

def run_steam_data_loader(**kwargs):
    fetch_and_store_steam_data()

def run_youtube_data_loader(**kwargs):
    fetch_and_store_youtube_data()

def run_proton_data_loader(**kwargs):
    fetch_and_store_proton_data()

def run_mongo_loader(**kwargs):
    mongo_trusted_loader()

def run_mongo_exploitation_loader(**kwargs):
    mongo_exploitation_loader()

def run_influx_loader(**kwargs):
    influx_loader()

def run_duck_loader_trusted(**kwargs):
    duck_loader_trusted()

def run_duck_loader_explotation(**kwargs):
    duck_loader_explotation()





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='all_data_loader_dag',
    default_args=default_args,
    description='DAG to execute all workflows',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['twitch', 'data', 'loader', 'youtube', 'steam', 'mongo', 'influx', 'duckdb'],
) as dag:
    with TaskGroup("collect_data") as collect_data:
        run_steam_loader = PythonOperator(
            task_id='run_steam_data_loader',
            python_callable=run_steam_data_loader,
            provide_context=True,
            trigger_rule='all_done',
        )
        run_youtube_loader = PythonOperator(
            task_id='run_youtube_data_loader',
            python_callable=run_youtube_data_loader,
            provide_context=True,
            trigger_rule='all_done',
        )
        run_proton_loader = PythonOperator(
            task_id='run_proton_data_loader',
            python_callable=run_proton_data_loader,
            provide_context=True,
            trigger_rule='all_done',
        )

    with TaskGroup("load_data_trusted") as load_data:
        run_mongo_loader_task = PythonOperator(
           task_id='run_mongo_loader',
           python_callable=run_mongo_loader,
           provide_context=True,
           trigger_rule='all_done',
        )
        run_influx_loader_task = PythonOperator(
            task_id='run_influx_loader',
            python_callable=run_influx_loader,
            provide_context=True,
            trigger_rule='all_done',
        )
        run_duck_loader_trusted_task = PythonOperator(
            task_id='run_duck_loader_trusted',
            python_callable=run_duck_loader_trusted,
            provide_context=True,
            trigger_rule='all_done',
        )

    with TaskGroup("load_data_explotation") as load_data_exploitation:
        run_duck_loader_trusted_task = PythonOperator(
            task_id='run_duck_loader_explotation',
            python_callable=run_duck_loader_explotation,
            provide_context=True,
            trigger_rule='all_done',
        )
        run_mongo_exploitation_loader_task = PythonOperator(
            task_id='run_mongo_exploitation_loader',
            python_callable=run_mongo_exploitation_loader,
            provide_context=True,
            trigger_rule='all_done',
        )

    collect_data >> load_data >> load_data_exploitation
