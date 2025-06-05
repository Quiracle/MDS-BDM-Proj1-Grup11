from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from Twitch.TwitchDataLoader import fetch_and_store_twitch_data


def run_twitch_data_loader(**kwargs):
    fetch_and_store_twitch_data()



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='twitch_data_loader_dag',
    default_args=default_args,
    description='DAG to execute all workflows',
    # run every 5 minutes
    schedule_interval='*/5 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['twitch', 'data', 'loader'],
) as dag:

    run_twitch_loader = PythonOperator(
        task_id='run_twitch_data_loader',
        python_callable=run_twitch_data_loader,
        provide_context=True,
        trigger_rule='all_done',
    )
    run_twitch_loader