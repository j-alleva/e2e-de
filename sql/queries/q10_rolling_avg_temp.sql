-- q10_rolling_avg_temp.sql
-- Purpose: Calculate a 3-hour moving average (current hour + previous 2 hours).

SELECT 
    l.location_name,
    d.date_value,
    f.hour,
    f.temperature_2m,
    -- Window: 2 rows before + current row = 3 total
    ROUND(AVG(f.temperature_2m) OVER (
        PARTITION BY l.location_name 
        ORDER BY f.hour
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )::numeric, 1) as moving_avg_3hr
FROM fact_weather_hourly f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_location l ON f.location_id = l.location_id
ORDER BY l.location_name, d.date_id, f.hour;