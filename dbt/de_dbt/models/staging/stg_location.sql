select
    location_id,
    location_name,
    latitude,
    longitude
from {{ source('weather_sources', 'dim_location') }}