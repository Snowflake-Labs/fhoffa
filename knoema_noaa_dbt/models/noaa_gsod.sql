{{ 
  config(
    materialized='incremental'
    , unique_key='rowid'
    , cluster_by=['date', 'station_id']
  )
}}

with source_data as (
    select "Stations", "Date", "Stations Name", "Country", "Indicator Name", "Value"
    from {{ source('knoema_noaa', 'NOAACD2019R') }}
    where "Measure" = 'M1'
    {% if is_incremental() %}
      and "Date" > (select max(date) - 21 from {{ this }})
    {% endif %}
)

select {{ dbt_utils.surrogate_key(['date', 'station_id']) }} rowid
  , *
from source_data
pivot(max("Value") for "Indicator Name" in ('Mean visibility (miles)','Maximum temperature (Fahrenheit)','Mean dew point (Fahrenheit)','Maximum wind gust (Number)','Minimum temperature (Fahrenheit)','Maximum sustained wind speed (knots)','Mean wind speed (knots)','Mean station pressure (millibars)','Precipitation amount (inches)','Mean temperature (Fahrenheit)','Mean sea level pressure (millibars)','Snow depth (inches)'))
as p(station_id, date, name, country_fips, visibility, temp_max, dew, wind_max, temp_min, wind_sustained_max, wind_mean, pressure, rain, temp, pressure_sea, snow_depth)
