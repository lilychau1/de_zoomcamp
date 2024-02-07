from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, parquet_name, execution_date):
    print(table_name, parquet_name, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    df = pd.read_parquet(parquet_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print('connection established successfully, inserting data...')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    parquet_file = pq.ParquetFile(parquet_name)

    for batch in parquet_file.iter_batches():
        
        t_start = time()

        df = batch.to_pandas()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f second' % (t_end - t_start))
