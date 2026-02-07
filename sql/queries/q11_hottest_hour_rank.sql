-- q11_hottest_hour_rank.sql
-- Purpose: Rank hours from hottest (1) to coldest for each city.

SELECT 
    l.location_name,
    d.date_value,
    f.hour,
    f.temperature_2m,
    RANK() OVER (
        PARTITION BY l.location_name 
        ORDER BY f.temperature_2m DESC
    ) as heat_rank
FROM fact_weather_hourly f
join dim_date d ON f.date_id = d.date_id
JOIN dim_location l ON f.location_id = l.location_id
-- We can filter the result to see just the #1s if we wrap this in a CTE,
-- but for now, let's just see the rankings.
ORDER BY l.location_name, d.date_value, heat_rank;