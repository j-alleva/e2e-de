-- q4_weekend_weather.sql
-- Purpose: Show weather data only for weekends.

SELECT 
    l.location_name,
    d.date_value,
    d.day_of_week, -- 0=Sunday, 6=Saturday
    f.hour,
    f.temperature_2m
FROM fact_weather_hourly f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_location l ON f.location_id = l.location_id
WHERE d.is_weekend = TRUE
ORDER BY l.location_name, f.hour;