with trips as (

    select
        trip_date,
        member_casual,
        trip_duration_minutes
    from {{ ref("stg_citibike_trips") }}

    where member_casual is not null

),

daily_member_type_metrics as (

    select
        trip_date,
        member_casual as rider_type,

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
        member_casual

)

select *
from daily_member_type_metrics
order by
    trip_date,
    rider_type