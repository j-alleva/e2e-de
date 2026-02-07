-- q13_extreme_weather_cte.sql
-- Purpose: Use a CTE to calculate daily stats, then filter for high-variance cities.

WITH daily_stats AS (
    SELECT 
        l.location_name,
        d.date_value,
        MAX(f.temperature_2m) as max_temp,
        MIN(f.temperature_2m) as min_temp
    FROM fact_weather_hourly f
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_location l ON f.location_id = l.location_id
    GROUP BY l.location_name, d.date_value
)
SELECT 
    location_name,
    date_value,
    max_temp,
    min_temp,
    (max_temp - min_temp) as temp_variance
FROM daily_stats
WHERE (max_temp - min_temp) > 10
ORDER BY temp_variance DESC;