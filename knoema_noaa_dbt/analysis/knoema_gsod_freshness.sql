select date, count(*) c
from {{ ref('noaa_gsod') }} 
group by 1
order by 1 desc
limit 10
