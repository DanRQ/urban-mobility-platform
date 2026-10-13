with source_trips as (

    select * 
    from {{ source('citibike_raw', 'citibike_trips') }}

),

cleaned_trips as (

    select
        ride_id,
        rideable_type,

        started_at,
        ended_at,

        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,

        start_lat,
        start_lng,
        end_lat,
        end_lng,

        member_casual,

        ended_at - started_at as trip_duration,
        extract(epoch from ended_at - started_at) / 60.0 as trip_duration_minutes,

        cast(started_at as date) as trip_date,
        extract(hour from started_at) as start_hour,
        extract(dow from started_at) as start_day_of_week,
        
        round(
            6371 * 2 * asin(
                sqrt(
                    power(sin(radians(end_lat - start_lat) / 2), 2)
                    + cos(radians(start_lat))
                    * cos(radians(end_lat))
                    * power(sin(radians(end_lng - start_lng) / 2), 2)
                )
            )::numeric,
            3
        ) as stations_distance_km

    from source_trips

    where started_at is not null
      and ended_at is not null
      and ended_at >= started_at

)

select * from cleaned_trips