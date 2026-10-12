with trips as (

    select *
    from {{ ref('stg_citibike_trips') }}

),

daily_metrics as (

    select
        trip_date,

        count(*) as total_trips,

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

        count(distinct start_station_id) as active_start_stations,

        round(avg(trip_duration_minutes)::numeric, 2) as avg_trip_duration_minutes,

        round(
            percentile_cont(0.5) within group (
                order by trip_duration_minutes
            )::numeric,
            2
        ) as median_trip_duration_minutes

    from trips

    group by trip_date

)

select * from daily_metrics
order by trip_date