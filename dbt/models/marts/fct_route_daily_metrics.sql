with station_roles as (

    select
        start_station_name as station_name,
        'start' as station_type,
        trip_date as trip_date
    from {{ ref('stg_citibike_trips') }}
    where start_station_name is not null

    union all

    select
        end_station_name as station_name,
        'end' as station_type,
        trip_date as trip_date
    from {{ ref('stg_citibike_trips') }}
    where end_station_name is not null

)

select
    station_name,
    trip_date,
    count(*) filter (where station_type = 'start') as start_station_count,
    count(*) filter (where station_type = 'end') as end_station_count
from station_roles
group by station_name,trip_date
order by station_name