
# Early working steps from course
docker run -it 
    -e POSTGRES_USER="root" 
    -e POSTGRES_PASSWORD="root" 
    -e POSTGRES_DB="ny_taxi" 
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data 
    -p 5432:5432 
    postgres:13 
    
docker run -it
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com"
    -e PGADMIN_DEFAULT_PASSWORD="root"
    -p 8080:80
    --network=pg-network
    --name pgadmin
    dpage/pgadmin4


# Network
docker run -it 
    -e POSTGRES_USER="root" 
    -e POSTGRES_PASSWORD="root" 
    -e POSTGRES_DB="ny_taxi" 
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data 
    -p 5432:5432 
    --network=pg-network
    --name pg-database
    postgres:13 

# For course example
URL='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'

# For quicker access - download to machine first then retrieve
URL='http://192.168.178.27:8000/yellow_tripdata_2021-01.parquet'

# For homework 

# Using link to parquet data since I used pyarrow as my data ingestion approach
URL='https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet'

#If local script
python ingest_data.py
    --user=root
    --password=root
    --host=localhost
    --port=5432
    --db=ny_taxi
    --table_name=yellow_taxi_trips
    --url=${URL}

# If docker + PGCli
docker build -t taxi_ingest:v001 .

docker run -it
    --network=pg-network
    taxi_ingest:v001
        --user=root
        --password=root
        --host=pg-database
        --port=5432
        --db=ny_taxi
        --table_name=yellow_taxi_trips
        --url=${URL}

pgcli -h localhost -p 5432 -u root ny_taxi

# OR using docker compose + PGAdmin
docker-compose up