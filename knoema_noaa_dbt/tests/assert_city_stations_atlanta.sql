select *
from {{ ref('city_stations') }}
where city='Atlanta'
and station='HARTSFIELD-JACKSON ATLANTA INTL AP'
limit 100
