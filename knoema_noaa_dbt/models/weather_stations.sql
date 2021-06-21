select distinct "Stations Name" station, "Country" country, "Country Name", "Stations" station_id, "Stations Latitude" lat, "Stations Longitude" lon
from {{ source('knoema_noaa', 'NOAACD2019R') }}
where "Date" between current_date() - 30 and current_date() - 5
