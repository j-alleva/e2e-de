-- q6_min_max_temp.sql
-- Purpose: Identify the lowest and highest temperatures recorded for each city.

SELECT 
    l.location_name,
    d.date_value,
    MIN(f.temperature_2m) as min_temp,
    MAX(f.temperature_2m) as max_temp,
    (MAX(f.temperature_2m) - MIN(f.temperature_2m)) as temp_variance
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_Date d on f.date_id = d.date_id
GROUP BY l.location_name, d.date_value
ORDER BY temp_variance DESC;

