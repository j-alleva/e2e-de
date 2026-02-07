-- q8_temp_buckets.sql
-- Purpose: Categorize hours into temperature buckets and count them per city.

SELECT 
    l.location_name,
    d.date_value,
    CASE 
        WHEN f.temperature_2m < 10 THEN 'Cold'
        WHEN f.temperature_2m BETWEEN 10 AND 20 THEN 'Mild'
        ELSE 'Hot'
    END as temp_category,
    COUNT(*) as hours_count
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
join dim_date d ON f.date_id = d.date_id
GROUP BY 
    l.location_name,
    d.date_value,
    CASE 
        WHEN f.temperature_2m < 10 THEN 'Cold'
        WHEN f.temperature_2m BETWEEN 10 AND 20 THEN 'Mild'
        ELSE 'Hot'
    END
ORDER BY l.location_name, hours_count DESC;