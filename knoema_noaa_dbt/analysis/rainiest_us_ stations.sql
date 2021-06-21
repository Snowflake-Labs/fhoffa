select *
from (
    select *
    from (
        select * 
        from {{ ref('noaa_gsod') }} 
        where country_fips='US'
    )
    match_recognize(
        partition by station_id, name
        order by date
        measures avg(temp) avg_temp, sum(rain) total_rain, min(rain) min_rain, count(*) rainy_days, min(date) date
        one row per match
        pattern(rain{5,})
        define
            rain as rain > 0.1
    )
) a
join {{ ref('stations_city') }} b
using (station_id)
order by rainy_days desc, total_rain desc
