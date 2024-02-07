from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import logging

from typing import List

from transform_data import transform_data

def concat_multiple_parquet_files(parquet_file_list: List[str]) -> pd.DataFrame:
    return pd.concat([pd.read_parquet(parquet_file) for parquet_file in parquet_file_list])
    
def concat_parquets_and_transform_callable(parquet_file_list: List[str], output_file: str) -> None: 
    # Concatenate loaded parquet files
    df = concat_multiple_parquet_files(parquet_file_list)
    logging.info(f'Homework: Shape of dataframe after loading: {df.shape}')

    # Transform data
    df = transform_data(df)
    logging.info(f'Homework: Shape of dataframe after transformation: {df.shape}')

    # Save concatenated and transforme dataframe to parquet
    df.to_parquet(output_file)

def ingest_callable(user, password, host, port, db, table_name, parquet_name, execution_date) -> None:
    print(table_name, parquet_name, execution_date)


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    logging.info(f'Connection established successfully: postgresql://{user}:{password}@{host}:{port}/{db}')
    logging.info('Inserting data...')

    df = pd.read_parquet(parquet_name)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    parquet_file = pq.ParquetFile(parquet_name)

    for batch in parquet_file.iter_batches():
        
        t_start = time()

        df = batch.to_pandas()
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        logging.info('inserted another chunk..., took %.3f second' % (t_end - t_start))

