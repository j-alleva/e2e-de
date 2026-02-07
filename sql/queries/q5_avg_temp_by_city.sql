-- q5_avg_temp_by_city.sql
-- Purpose: Calculate the average temperature for each city on this day.

SELECT 
    l.location_name,
    d.date_value,
    COUNT(*) as hours_recorded,
    ROUND(AVG(f.temperature_2m)::numeric, 1) as avg_temp
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY l.location_name, d.date_value
ORDER BY avg_temp DESC;