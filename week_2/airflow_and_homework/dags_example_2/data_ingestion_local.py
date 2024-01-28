import os
from datetime import datetime
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dags_new.ingest_data import ingest_callable

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

local_workflow = DAG(
    'LocalIngestionDag',
    schedule_interval='0 6 2 * *',
    start_date=datetime(2024, 1, 1)
)

# dataset_file = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# dataset_url = URL_PREFIX + dataset_file
# OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# TABLE_NAME_TEMPLATE = 'yelow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

dataset_file = 'yellow_tripdata_2021-01.parquet'
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
dataset_url = URL_PREFIX + dataset_file
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_2021-01.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_2021_01'

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -LsS {dataset_url} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            parquet_name=OUTPUT_FILE_TEMPLATE
        )
    )

    wget_task >> ingest_task