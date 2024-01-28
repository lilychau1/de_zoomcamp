import os
from datetime import datetime
from datetime import date
from dateutil.rrule import *
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_data import ingest_callable, concat_parquets_and_transform_callable
from gcp_operations import upload_to_gcs_partitioned

# Define directory and config variables

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
GCS_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

LOAD_DATA_FROM_YEAR = int(os.getenv('LOAD_DATA_FROM_YEAR'))
LOAD_DATA_FROM_MONTH = int(os.getenv('LOAD_DATA_FROM_MONTH'))
LOAD_DATA_TO_YEAR = int(os.getenv('LOAD_DATA_TO_YEAR'))
LOAD_DATA_TO_MONTH = int(os.getenv('LOAD_DATA_TO_MONTH'))

TABLE_NAME = 'green_taxi'

#Get the list of year-months to be loaded from source based on environment variables
load_data_from_date = date(LOAD_DATA_FROM_YEAR, LOAD_DATA_FROM_MONTH, 1)
load_data_to_date = date(LOAD_DATA_TO_YEAR, LOAD_DATA_TO_MONTH, 1)

dataset_file_year_months = [
    day.strftime('%Y-%m') 
    for day in rrule(
        MONTHLY, 
        dtstart=load_data_from_date, 
        until=load_data_to_date)
    ]

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

OUTPUT_FILE_NAME = 'green_tripdata_%s_to_%s.parquet' % (load_data_from_date.strftime('%Y-%m'), load_data_to_date.strftime('%Y-%m'))
OUTPUT_FILE = f'{AIRFLOW_HOME}/{OUTPUT_FILE_NAME}'

# Define workflow name, schedule and start date
local_workflow = DAG(
    'green_taxi_etl',
    schedule_interval='0 5 * * *', # Daily 5AM
    start_date=datetime.today()
)

# Get a dictionary of all the source file names and the corresponding output file name to save
source_and_output_file_name_dict = {
    f'{URL_PREFIX}/green_tripdata_{year_month}.parquet': f'{AIRFLOW_HOME}/output_{year_month}.parquet' for year_month in dataset_file_year_months
}

# Create a list of bash commands to download and save each file
download_command_list = [
    f'curl -LsS {source_file_url} > {output_file}'
    for source_file_url, output_file in source_and_output_file_name_dict.items()
]

# Join all commands with '&&' so all the download operations will be performed in one command sequentially
download_command = ' && '.join(download_command_list)

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=download_command
    )

    concat_parquet_and_transform_task = PythonOperator(
        task_id='concat_parquets',
        python_callable=concat_parquets_and_transform_callable,
        op_kwargs=dict(
            parquet_file_list=source_and_output_file_name_dict.values(),
            output_file=OUTPUT_FILE_NAME
        )
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
            table_name=TABLE_NAME,
            parquet_name=OUTPUT_FILE_NAME,
        )
    )

    local_to_gcs_task = PythonOperator(
        task_id='local_to_gcs_task',
        python_callable=upload_to_gcs_partitioned,
        op_kwargs={
            'bucket_name': GCS_BUCKET,
            'table_name': TABLE_NAME,
            'local_file': OUTPUT_FILE,
            'partition_cols': ['lpep_pickup_date'],
        },
    )



    wget_task >> concat_parquet_and_transform_task >> ingest_task >> local_to_gcs_task
