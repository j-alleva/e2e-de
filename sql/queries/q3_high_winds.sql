-- q3_high_winds.sql
-- Purpose: Identify hours with wind speeds greater than 15 km/h.

SELECT 
    l.location_name,
    f.hour,
    f.wind_speed_10m,
    f.temperature_2m
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
WHERE f.wind_speed_10m > 15
ORDER BY f.wind_speed_10m DESC;