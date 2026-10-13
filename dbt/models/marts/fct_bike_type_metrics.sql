with trips as (

    select
        trip_date,
        rideable_type,
        trip_duration_minutes
    from {{ ref("stg_citibike_trips") }}

    where rideable_type is not null

),

daily_bike_type_metrics as (

    select
        trip_date,
        rideable_type as bike_type,

        count(*) as total_trips,

        round(
            avg(trip_duration_minutes)::numeric,
            2
        ) as avg_trip_duration_minutes,

        round(
            min(trip_duration_minutes)::numeric,
            2
        ) as min_trip_duration_minutes,

        round(
            max(trip_duration_minutes)::numeric,
            2
        ) as max_trip_duration_minutes

    from trips
    group by
        trip_date,
        rideable_type

)

select *
from daily_bike_type_metrics
order by
    trip_date,
    bike_type