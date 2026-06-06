{{
    config(
        materialized = 'table',
        tags         = ['mart', 'fault', 'health']
    )
}}

/*
    mart_fault_summary.sql

    One row per zone per day. Counts fault events by type.

    Powers the Meter Health dashboard:
      - DEGRADED events KPI card  → SUM(degraded_reading_count)
      - FAULT events KPI card     → SUM(fault_reading_count)
      - SILENT events KPI card    → SUM(silent_reading_count)
      - Fault events KPI card     → SUM(total_fault_readings)
      - Most fault-prone zone     → zone with highest SUM(total_fault_readings)
      - Fault events by zone bar  → total_fault_readings grouped by zone_id

    Grain:
      One row per zone_id per reading_date.

    Depends on:
      - stg_meter_readings
*/

SELECT
    zone_id,
    reading_date,

    SUM(CASE WHEN is_degraded THEN 1 ELSE 0 END)    AS degraded_reading_count,
    SUM(CASE WHEN is_fault    THEN 1 ELSE 0 END)    AS fault_reading_count,
    SUM(CASE WHEN is_silent   THEN 1 ELSE 0 END)    AS silent_reading_count,

    SUM(CASE WHEN is_degraded
             OR is_fault
             OR is_silent
        THEN 1 ELSE 0 END)                          AS total_fault_readings

FROM {{ ref('stg_meter_readings') }}
GROUP BY 1, 2
ORDER BY zone_id, reading_date