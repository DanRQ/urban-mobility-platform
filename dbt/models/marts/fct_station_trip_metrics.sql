with trips as (

    select *
    from {{ ref('stg_citibike_trips') }}

),

station_metrics as (

    select
        start_station_id,
        start_station_name,

        count(*) as total_trips,
        count(distinct ride_id) as distinct_trips,

        count(*) filter (
            where member_casual = 'member'
        ) as member_trips,

        count(*) filter (
            where member_casual = 'casual'
        ) as casual_trips,

        count(*) filter (
            where rideable_type = 'classic_bike'
        ) as classic_bike_trips,

        count(*) filter (
            where rideable_type = 'electric_bike'
        ) as electric_bike_trips,

        round(avg(trip_duration_minutes)::numeric, 2) as avg_trip_duration_minutes

    from trips
    where start_station_id is not null
      and start_station_name is not null
    group by
        start_station_id,
        start_station_name

)

select *
from station_metrics
order by total_trips desc