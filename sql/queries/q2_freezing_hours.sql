-- q2_freezing_hours.sql
-- Purpose: Filter for hours where the temperature dropped below freezing.

SELECT 
    l.location_name,
    d.date_value,
    f.hour,
    f.temperature_2m
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.temperature_2m < 0
ORDER BY f.temperature_2m ASC;
