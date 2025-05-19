from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from TwitchDataLoader import TwitchDataLoader

# Import your TwitchDataLoader function/class

def run_twitch_data_loader(**kwargs):
    loader = TwitchDataLoader()
    loader.fetch_and_store_twitch_data()  # Adjust method if needed

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

    run_loader = PythonOperator(
        task_id='run_twitch_data_loader',
        python_callable=run_twitch_data_loader,
        provide_context=True,
    )

    run_loader