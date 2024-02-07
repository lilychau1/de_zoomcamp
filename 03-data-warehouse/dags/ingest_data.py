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
    # Transform data
    df = transform_data(df)
 
    df.to_parquet(output_file)
