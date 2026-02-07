-- q7_rainy_cities.sql
-- Purpose: Filter for cities that had non-zero total precipitation.

SELECT 
    l.location_name,
    SUM(f.precipitation) as total_precipitation
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY l.location_name
HAVING SUM(f.precipitation) > 0
ORDER BY total_precipitation DESC;