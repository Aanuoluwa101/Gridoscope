{{
    config(
        materialized = 'table',
        tags         = ['mart', 'heatmap', 'demand']
    )
}}

/*
    mart_demand_heatmap.sql

    One row per zone per hour-of-day per day-of-week.
    Feeds the Demand Heatmap dashboard page.

    Powers:
      - Heatmap visual (Matrix)  → avg_kwh by hour_of_day × day_of_week_num
      - Peak hour by zone table  → MAX(avg_kwh) per zone + corresponding hour
      - Overnight trough stats   → filter hour_of_day BETWEEN 0 AND 5

    Grain:
      One row per zone_id per hour_of_day per day_of_week_num.

    Depends on:
      - stg_meter_readings
*/

SELECT
    zone_id,
    hour_of_day,
    day_of_week_num,
    day_of_week_name,
    is_weekend,

    ROUND(AVG(kwh_delta), 4)        AS avg_kwh,
    ROUND(SUM(kwh_delta), 4)        AS total_kwh,
    COUNT(*)                        AS reading_count

FROM {{ ref('stg_meter_readings') }}
GROUP BY 1, 2, 3, 4, 5
ORDER BY zone_id, day_of_week_num, hour_of_day