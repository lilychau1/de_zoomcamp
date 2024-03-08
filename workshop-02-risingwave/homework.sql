-- Q1

CREATE MATERIALIZED VIEW trip_time_summary AS 
with 
trip_time_data as 
(
    select  tzpu.zone as pickup_zone
    ,       tzdo.zone as dropoff_zone
    ,       tpep_dropoff_datetime - tpep_pickup_datetime as trip_time
    from trip_data t

    left join taxi_zone tzpu
    on tzpu.location_id = t.pulocationid
 
    left join taxi_zone tzdo
    on tzdo.location_id = t.dolocationid
)

select      pickup_zone
,           dropoff_zone
,           avg(trip_time) as average_trip_time
,           min(trip_time) as min_trip_time
,           max(trip_time) as max_trip_time
from        trip_time_data
group by    pickup_zone
,           dropoff_zone;

select * from trip_time_summary order by average_trip_time desc;

-- Q2

CREATE MATERIALIZED VIEW trip_time_summary_with_count AS 
with 
trip_time_data as 
(
    select  tzpu.zone as pickup_zone
    ,       tzdo.zone as dropoff_zone
    ,       tpep_dropoff_datetime - tpep_pickup_datetime as trip_time
    from trip_data t

    left join taxi_zone tzpu
    on tzpu.location_id = t.pulocationid
 
    left join taxi_zone tzdo
    on tzdo.location_id = t.dolocationid
)

select      pickup_zone
,           dropoff_zone
,           avg(trip_time) as average_trip_time
,           min(trip_time) as min_trip_time
,           max(trip_time) as max_trip_time
,           count(*) as total_trip_count
from        trip_time_data
group by    pickup_zone
,           dropoff_zone;

select * from trip_time_summary_with_count order by average_trip_time desc;

-- Q3

CREATE MATERIALIZED VIEW trip_count_17h_after_latest AS 
select      tz.zone as pickup_zone
,           count(*) as total_trip_count
from        trip_data t

left join   taxi_zone tz
on          tz.location_id = t.pulocationid

where       tpep_pickup_datetime >= (select max(tpep_pickup_datetime) - interval '17 hours' from trip_data)

group by    1;

select * from trip_count_17h_after_latest order by total_trip_count desc;