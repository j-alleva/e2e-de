-- q9_hourly_temp_change.sql
-- Purpose: Calculate the change in temperature from the previous hour.
-- Key Concept: LAG(column) OVER (PARTITION BY city ORDER BY time)

SELECT 
    l.location_name,
    d.date_value,
    f.hour,
    f.temperature_2m,
    -- Get the previous hour's temp
    LAG(f.temperature_2m) OVER (
        PARTITION BY l.location_name 
        ORDER BY f.hour
    ) as prev_hour_temp,
    -- Calculate difference
    ROUND((f.temperature_2m - LAG(f.temperature_2m) OVER (
        PARTITION BY l.location_name 
        ORDER BY f.hour
    ))::numeric,1
    )as temp_change
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY l.location_name, d.date_value, f.hour;