with facts as (
    select * from {{ ref('stg_weather_hourly') }}
),

dates as (
    select * from {{ ref('stg_date') }}
),

locations as (
    select * from {{ ref('stg_location') }}
)

select
    -- Generate a unique primary key for testing
    facts.location_id || '-' || facts.date_id as summary_id,
    
    -- Dimensions
    dates.date_id,
    locations.location_id,
    locations.location_name,
    dates.date_value as summary_date,
    dates.is_weekend,
    
    -- Aggregated Metrics
    min(facts.temp_celsius) as min_temp_c,
    max(facts.temp_celsius) as max_temp_c,
    avg(facts.temp_celsius) as avg_temp_c,
    avg(facts.humidity_percent) as avg_humidity,
    sum(facts.precipitation_mm) as total_precipitation_mm,
    max(facts.wind_speed_kmh) as max_wind_speed_kmh
    
from facts
join dates on facts.date_id = dates.date_id
join locations on facts.location_id = locations.location_id
group by 
    1, 2, 3, 4, 5, 6