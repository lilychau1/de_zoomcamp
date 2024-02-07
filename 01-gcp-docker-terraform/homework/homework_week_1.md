## Module 1 Homework

## Docker & SQL
### (Also see `docker/`)

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - [ ] *Automatically remove the container when it exits* 

- [ ] `--delete`
- [ ] `--rc`
- [ ] `--rmc`
- [x] `--rm`

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- [x] 0.42.0
- [ ] 1.0.0
- [ ] 23.0.1
- [ ] 58.1.0

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)
```
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

lookup_df = pd.read_csv('taxi_zone_lookup.csv')

lookup_df.head(n=0).to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')
lookup_df.to_sql(name='taxi_zone_lookup', con=engine, if_exists='append')
```

## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- [ ] 15767
- [x] 15612
- [ ] 15859
- [ ] 89009

```
select count(*) from green_taxi_trips where cast(lpep_pickup_datetime as date) 
 = '2019-09-18' and cast(lpep_dropoff_datetime as date) = '2019-09-18'
```

 
## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- [ ] 2019-09-18
- [ ] 2019-09-16
- [x] 2019-09-26
- [ ] 2019-09-21

```
select cast(lpep_pickup_datetime as date)
from green_taxi_trips
where trip_distance = (select max(trip_distance) from green_taxi_trips)
```

## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- [x] "Brooklyn" "Manhattan" "Queens"
- [ ] "Bronx" "Brooklyn" "Manhattan"
- [ ] "Bronx" "Manhattan" "Queens" 
- [ ] "Brooklyn" "Queens" "Staten Island"

```
with borough_attached as 
(
	select *
	from green_taxi_trips gt
	left join public.taxi_zone_lookup lt 
	on gt."PULocationID" = lt."LocationID"
	and cast(gt.lpep_pickup_datetime as date) = '2019-09-18'
)
select "Borough", sum(total_amount) as borough_total_amount
from borough_attached
group by "Borough"
having sum(total_amount) > 50000
```
## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- [ ] Central Park
- [ ] Jamaica
- [x] JFK Airport
- [ ] Long Island City/Queens Plaza

```
with borough_attached as 
(
	select  gt.*
	,		ltpu."Borough" as pick_up_borough
	,		ltpu."Zone" as pick_up_zone
	,		ltdo."Borough" as drop_off_borough
	,		ltdo."Zone" as drop_off_zone
	from green_taxi_trips gt
	
	left join public.taxi_zone_lookup ltpu
	on gt."PULocationID" = ltpu."LocationID"
	and cast(gt.lpep_pickup_datetime as date) >= '2019-09-01'
	and cast(gt.lpep_pickup_datetime as date) <= '2019-09-30'
	
	left join public.taxi_zone_lookup ltdo
	on gt."DOLocationID" = ltdo."LocationID"
)
select  drop_off_zone, tip_amount
from borough_attached
where pick_up_zone = 'Astoria'
order by tip_amount desc
```

## Terraform
### (Also see `terraform/`)

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.

Answer: 
```
google_storage_bucket.demo-bucket: Refreshing state... [id=de-zoomcamp-homework-terra-bucket]

Terraform used the selected providers to generate the following execution
plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "de_zoomcamp_hw_dataset"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "EU"
      + max_time_travel_hours      = (known after apply)
      + project                    = "xxxx-yyyy-11111"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

Plan: 1 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 2s [id=projects/xxxx-yyyy-11111/datasets/de_zoomcamp_hw_dataset]

Apply complete! Resources: 1 added, 0 changed, 0 destroyed.
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET
