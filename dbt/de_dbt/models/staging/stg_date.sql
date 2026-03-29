select
    date_id,
    date_value,
    year,
    month,
    day,
    day_of_week,
    is_weekend
from {{ source('weather_sources', 'dim_date') }}