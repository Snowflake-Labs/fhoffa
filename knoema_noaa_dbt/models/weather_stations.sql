select distinct "Stations Name" station, "Country" country, "Country Name", "Stations" station_id, "Stations Latitude" lat, "Stations Longitude" lon
from {{ ref('source_knoema_noaa_gsod') }}
where "Date" between current_date() - 30 and current_date() - 5
