{{
    config(
        materialized='table'
    )
}}

with 
trip_data as (
    select *,
    'fhv' as service_type
    from {{ ref('stg_external_fhv_trip_data') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    trip_data.tripid, 
    trip_data.dispatching_base_num, 
    trip_data.service_type,
    trip_data.sr_flag, 
    trip_data.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trip_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trip_data.pickup_datetime, 
    trip_data.dropoff_datetime, 

from trip_data
inner join dim_zones as pickup_zone
on trip_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trip_data.dropoff_locationid = dropoff_zone.locationid