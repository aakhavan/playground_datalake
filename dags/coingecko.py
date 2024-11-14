from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'coingecko_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline for CoinGecko',
    schedule_interval=timedelta(days=1),
)

fetch_and_save = BashOperator(
    task_id='fetch_and_save',
    bash_command='python /path/to/fetch_and_save.py',
    env={'START_DATE': '{{ ds }}', 'END_DATE': '{{ ds }}'},
    dag=dag,
)