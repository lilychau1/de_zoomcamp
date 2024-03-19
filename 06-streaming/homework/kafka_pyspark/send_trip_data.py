import json
import pandas as pd
import time

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

df_green = pd.read_csv('/Users/lilychau/Documents/projects/data_engineering/data_engineering_zoomcamp/06-streaming/homework/kafka_pyspark/green_tripdata_2019-10.csv.gz')
df_green = df_green[[
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
]]

t0 = time.time()
topic_name = 'green-trips'

for row in df_green.itertuples(index=False):
    message = {col: getattr(row,col) for col in row._fields}
    producer.send(topic_name, value=message)
    print(f'Sent: {message}')

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
