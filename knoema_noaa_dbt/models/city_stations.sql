{{config(materialized='table')}}

select label city, station, st_distance(st_makepoint(a.lon, a.lat), st_makepoint(b.lon, b.lat)) distance, country_fips, b.country
    , station_id
from {{ ref('weather_stations') }} a
join {{ ref('wikidata_large_cities') }} b
on a.country=b.country_fips
and st_distance(st_makepoint(a.lon, a.lat), st_makepoint(b.lon, b.lat)) < 50000
qualify row_number() over(partition by city order by distance) = 1
order by views desc
