select *
from {{ ref('stations_city') }}
where city='Atlanta'
and station='HARTSFIELD-JACKSON ATLANTA INTL AP'
limit 100
