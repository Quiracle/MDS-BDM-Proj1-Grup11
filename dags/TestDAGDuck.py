from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from Twitch.TwitchDataLoader import fetch_and_store_twitch_data
from InfluxLoader.influx_loader import run as influx_loader



from DuckLoader.duckdb_loader_to_explotation import run as duck_loader_explotation

def run_duck_loader_explotation(**kwargs):
    duck_loader_explotation()



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    dag_id='all_data_loader_dag2',
    default_args=default_args,
    description='DAG to execute all workflows',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['twitch', 'data', 'loader', 'youtube', 'steam', 'mongo', 'influx', 'duckdb'],
) as dag:
    
    run_influx_loader = PythonOperator(
        task_id='run_duck_loader_explotation',
        python_callable=run_duck_loader_explotation,
        provide_context=True,
        trigger_rule='all_done',
    )
    run_influx_loader
