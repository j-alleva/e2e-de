-- q1_sample_data.sql
-- Purpose: Join dimensions to the fact table to show readable data.

SELECT 
    l.location_name,
    d.date_value,
    f.hour,
    f.temperature_2m,
    f.precipitation
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY l.location_name, f.hour
LIMIT 10;