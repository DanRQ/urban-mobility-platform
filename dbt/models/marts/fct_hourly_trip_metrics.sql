with hourly_trips as (

    select
        trip_date,
        start_hour,
        member_casual
    from {{ ref("stg_citibike_trips") }}

),

hourly_metrics as (

    select
        trip_date,
        to_char(trip_date, 'FMDay') as day_of_week,
        start_hour,

        count(*) as total_trips,

        count(*) filter (
            where member_casual = 'member'
        ) as member_trips,

        count(*) filter (
            where member_casual = 'casual'
        ) as casual_trips

    from hourly_trips

    group by
        trip_date,
        start_hour

)

select *
from hourly_metrics
order by
    trip_date,
    start_hour