select
    fact_id,
    date_id,
    location_id,
    hour,
    temp_celsius,
    humidity_percent,
    precipitation_mm,
    wind_speed_kmh,
    extraction_time
from {{ source('weather_sources', 'fact_weather_hourly') }}