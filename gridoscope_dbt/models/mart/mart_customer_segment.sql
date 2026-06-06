{{
    config(
        materialized = 'table',
        tags         = ['mart', 'segments']
    )
}}

/*
    mart_customer_segment.sql

    One row per customer type. Total kWh consumed across the full dataset.

    Powers:
      - Consumption by customer segment bars → total_kwh by customer_type

    Grain:
      One row per customer_type.

    Depends on:
      - stg_meter_readings
*/

SELECT
    customer_type,
    ROUND(SUM(kwh_delta), 4)    AS total_kwh
FROM {{ ref('stg_meter_readings') }}
GROUP BY 1
ORDER BY total_kwh DESC