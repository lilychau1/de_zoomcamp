import pyarrow.parquet as pq
import pyarrow as pa

def upload_to_gcs_partitioned(bucket_name: str, table_name: str, local_file: str, partition_cols: list) -> None: 
    root_path = f'{bucket_name}/{table_name}'
    table = pq.read_table(local_file)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=partition_cols,
        filesystem=gcs
    )
    
