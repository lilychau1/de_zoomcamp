{{
    config(
        materialized='view'
    )
}}
with tripdata as 
(
    select *
    -- select *,
    -- row_number() over (partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('staging', 'external_fhv_trip_data') }}
    where dispatching_base_num is not null
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid, 
    cast(dispatching_base_num as string) as dispatching_base_num, 
    cast(Affiliated_base_number as string) as affiliated_base_number,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(SR_Flag as integer) as sr_flag
from tripdata
-- where rn = 1
where   PUlocationID != 0 -- During parquet ingestion, Nan values were replaced by 0 before casting to integer type
and   DOlocationID != 0

-- dbt build --select <model_name> --vars '{"is_test_run": false}'
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}