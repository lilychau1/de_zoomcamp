from time import time
import os

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url 
    parquet_name = 'output.parquet'

    # Download the parquet
    os.system(f'wget {url} -O {parquet_name}')

    df = pd.read_parquet(parquet_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    parquet_file = pq.ParquetFile(parquet_name)

    for batch in parquet_file.iter_batches():

        t_start = time()

        df = batch.to_pandas()

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f second' % (t_end - t_start))

if __name__ == '__main__': 
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
