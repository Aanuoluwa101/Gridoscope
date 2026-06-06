{{
    config(
        materialized = 'table',
        tags         = ['mart', 'zone', 'daily']
    )
}}

/*
    mart_zone_daily.sql

    One row per zone per day. Aggregates meter readings to zone-day level.

    Powers the Network Overview dashboard page:
      - Daily consumption trend (14-day bar chart) → total_kwh by reading_date
      - Zone consumption 30-day totals (bars)      → total_kwh by zone_id
      - Total consumption KPI card                 → SUM(total_kwh)
      - Avg daily peak KPI card                    → AVG(peak_power_kw)
      - Avg meter uptime KPI card                  → AVG(uptime_pct)

    Fault-related KPI cards come from mart_fault_summary, not this mart.

    Grain:
      One row per zone_id per reading_date.

    Depends on:
      - stg_meter_readings
*/

WITH daily_zone AS (

    SELECT
        zone_id,
        reading_date,

        -- consumption — the primary metrics for all visuals
        SUM(kwh_delta)                              AS total_kwh,
        AVG(power_kw)                               AS avg_power_kw,
        MAX(power_kw)                               AS peak_power_kw,

        -- fleet counts — needed for uptime calculation
        COUNT(DISTINCT meter_id)                    AS active_meter_count,
        COUNT(DISTINCT CASE
            WHEN is_silent THEN meter_id
        END)                                        AS silent_meter_count

    FROM {{ ref('stg_meter_readings') }}
    GROUP BY 1, 2

)

SELECT
    zone_id,
    reading_date,

    -- consumption
    ROUND(total_kwh, 4)                             AS total_kwh,
    ROUND(avg_power_kw, 3)                          AS avg_power_kw,
    ROUND(peak_power_kw, 3)                         AS peak_power_kw,

    -- fleet
    active_meter_count,
    silent_meter_count,

    -- uptime: active meters as % of 100 expected per zone
    ROUND(active_meter_count / 2 * 100, 2)      AS uptime_pct

FROM daily_zone
ORDER BY zone_id, reading_date