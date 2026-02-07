-- q12_cumulative_rainfall.sql
-- Purpose: Calculate running total of precipitation throughout the day.

SELECT 
    l.location_name,
    d.date_value,
    f.hour,
    f.precipitation,
    SUM(f.precipitation) OVER (
        PARTITION BY l.location_name 
        ORDER BY f.hour
    ) as cumulative_precip
FROM fact_weather_hourly f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_location l ON f.location_id = l.location_id
ORDER BY l.location_name, d.date_id, f.hour;