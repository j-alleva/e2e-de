{{ config(materialized='table') }}

with days as (
    -- Generates a sequence of days from 2020-01-01 (adjust as needed)
    select
        dateadd(
            day,
            seq4(),
            '2020-01-01'::date
        ) as date_day
    from table(generator(rowcount => 3650)) 
)

select
    date_day
from days