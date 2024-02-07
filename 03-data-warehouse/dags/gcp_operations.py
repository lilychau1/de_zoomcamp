import pyarrow.parquet as pq
import pyarrow as pa

from google.cloud import storage

def upload_to_gcs(bucket_name: str, table_name: str, local_file: str) -> None: 
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    print(f'table_name: {table_name}')
    print(f'local_file: {local_file}')
    
    blob = bucket.blob(table_name)
    blob.upload_from_filename(local_file)