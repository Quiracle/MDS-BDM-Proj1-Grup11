from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from Twitch.TwitchDataLoader import fetch_and_store_twitch_data
from InfluxLoader.influx_loader import run as influx_loader


def run_twitch_data_loader(**kwargs):
    fetch_and_store_twitch_data()

def run_influx_loader(**kwargs):
    influx_loader()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='influx_data_loader_dag',
    default_args=default_args,
    description='DAG to execute influx workflows',
    # run every 5 minutes
    schedule_interval='*/5 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['influx', 'data', 'loader'],
) as dag:

    run_influx_loader = PythonOperator(
        task_id='run_influx_data_loader',
        python_callable=run_influx_loader,
        provide_context=True,
        trigger_rule='all_done',
    )
    run_influx_loader