{{
    config(
        materialized = 'view',
        tags         = ['staging', 'meter_readings']
    )
}}

/*
    stg_meter_readings.sql

    Staging model for raw meter reading events loaded from S3 into the
    RAW.METER_EVENTS table via Airflow COPY INTO.

    Responsibilities of this model:
      1. Parse the src VARIANT column — extracting every JSON key into
         a typed, named column. No raw VARIANT should leak into the mart layer.
      2. Cast every field to its correct type. Snowflake will silently coerce
         in some cases but explicit casting makes failures loud and obvious.
      3. Rename fields to match our internal naming conventions where the
         producer field names differ (e.g. frequency_hz → grid_frequency_hz).
      4. Add derived fields that are useful across multiple downstream models
         — time bucketing, flags, computed metrics. Anything derived from a
         single row belongs here. Anything requiring aggregation or joins
         belongs in the mart layer.
      5. Add the pipeline timestamp columns (kafka_ingested_at, load_ts)
         so downstream models can compute latency and filter by load time.

    What this model does NOT do:
      - No aggregations
      - No joins to other tables
      - No business logic that belongs in the mart layer
      - No filtering of rows — all raw events pass through

    Source:
      raw.meter_events — one row per Kafka message, value stored as VARIANT in src

    Grain:
      One row per meter reading event. Unique on event_id.

    Downstream models:
      - mart_demand_heatmap
      - mart_zone_daily
      - mart_customer_segment_monthly
      - mart_meter_health
      - mart_fault_summary
*/

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'meter_events') }}

),

parsed AS (

    /*
        Extract every field from the src VARIANT column.

        The src column contains the raw JSON payload exactly as the producer
        sent it. Key names here must match the MeterEvent dataclass field
        names in meter_state_machine.py exactly — they are case-sensitive.

        _kafka_timestamp and load_ts are NOT inside the VARIANT — they are
        top-level columns added by the S3 Sink Connector and Snowflake
        respectively, so they are referenced directly without src: prefix.
    */

    SELECT
        -- ----------------------------------------------------------------
        -- Identity fields
        -- ----------------------------------------------------------------
        src:event_id::VARCHAR               AS event_id,
        src:meter_id::VARCHAR               AS meter_id,
        src:zone_id::VARCHAR                AS zone_id,
        src:customer_type::VARCHAR          AS customer_type,

        -- ----------------------------------------------------------------
        -- Timestamps
        -- reading_ts    : simulated meter time — use this for all analytics.
        --                 At speed_multiplier > 1 this runs ahead of wall clock.
        -- kafka_ingested_at : broker wall-clock time the message arrived at Kafka.
        --                     Use for pipeline latency monitoring only.
        -- load_ts       : wall-clock time Airflow loaded this row into Snowflake.
        --                 Use for incremental load logic.
        -- ----------------------------------------------------------------
        src:timestamp::TIMESTAMP_NTZ        AS reading_ts,
        -- src:_kafka_timestamp::TIMESTAMP_NTZ     AS kafka_ingested_at,
        load_ts,

        -- ----------------------------------------------------------------
        -- Consumption fields
        -- kwh_reading : cumulative odometer on the meter, ever-increasing.
        --               Useful for billing continuity checks — if it ever
        --               decreases the meter was replaced or tampered with.
        -- kwh_delta   : energy consumed since the last reading interval.
        --               This is the primary analytical field used in every
        --               aggregation downstream. Never use kwh_reading for
        --               demand calculations — always use kwh_delta.
        -- ----------------------------------------------------------------
        src:kwh_reading::FLOAT              AS kwh_reading,
        src:kwh_delta::FLOAT                AS kwh_delta,

        -- ----------------------------------------------------------------
        -- Electrical health fields
        -- These are the signals used by anomaly detection and meter health
        -- scoring. Deviations from nominal values indicate degradation.
        --   voltage:        residential ~120V, commercial ~240V, industrial ~480V
        --   current_amps:   back-calculated from power and voltage in producer
        --   power_factor:   healthy range 0.90–0.99. Below 0.85 = degraded.
        --   power_kw:       instantaneous demand. Pre-computed in producer as
        --                   (voltage × current_amps × power_factor) / 1000
        --   frequency_hz:   should be 60.0 ± 0.5 Hz (US grid standard)
        -- ----------------------------------------------------------------
        src:voltage::FLOAT                  AS voltage,
        src:current_amps::FLOAT             AS current_amps,
        src:power_factor::FLOAT             AS power_factor,
        src:power_kw::FLOAT                 AS power_kw,
        src:frequency_hz::FLOAT             AS grid_frequency_hz,

        -- ----------------------------------------------------------------
        -- Meter health fields
        -- meter_state : one of normal | degraded | fault | silent
        -- is_anomaly  : flagged by the consumer's per-hour EMA detector,
        --               not the producer's simpler rolling average flag.
        --               True when kwh_delta > 4× the per-hour EMA baseline
        --               for this meter at this hour of the day.
        -- signal_strength_dbm : RF signal quality in dBm (negative scale).
        --               -50 = excellent, -90 = very poor.
        -- ----------------------------------------------------------------
        src:meter_state::VARCHAR            AS meter_state,
        src:is_anomaly::BOOLEAN             AS is_anomaly,
        src:signal_strength_dbm::INT        AS signal_strength_dbm,

        -- ----------------------------------------------------------------
        -- Metadata
        -- ----------------------------------------------------------------
        src:firmware_version::VARCHAR       AS firmware_version,
        src:event_type::VARCHAR             AS event_type

    FROM source

),

enriched AS (

    /*
        Add derived fields computed from the parsed columns.

        All fields here are row-level derivations — no aggregation, no joins.
        They are added here rather than in mart models because multiple
        mart models use them, and computing them once in staging is cleaner
        than repeating the logic everywhere.
    */

    SELECT

        -- all parsed columns
        parsed.*,

        -- ----------------------------------------------------------------
        -- Time bucketing
        -- Used by mart_demand_heatmap and the live consumer window alignment.
        -- window_bucket rounds reading_ts down to the nearest 5-minute
        -- boundary — aligning all zones to the same window timestamps so
        -- MAX(window_bucket) in Power BI returns all 5 zones simultaneously.
        -- ----------------------------------------------------------------
        DATE_TRUNC('hour', reading_ts)                                  AS reading_hour,
        DATE_TRUNC('day',  reading_ts)                                  AS reading_date,
        DATEADD(
            'minute',
            -(MINUTE(reading_ts) % 5),
            DATE_TRUNC('minute', reading_ts)
        )                                                               AS window_bucket,

        -- ----------------------------------------------------------------
        -- Calendar flags
        -- Used by mart_customer_segment_monthly for weekend vs weekday
        -- comparisons, and by mart_demand_heatmap for the day-of-week axis.
        -- DAYOFWEEK returns 0=Sunday through 6=Saturday in Snowflake.
        -- ----------------------------------------------------------------
        HOUR(reading_ts)                                                AS hour_of_day,
        DAYOFWEEK(reading_ts)                                           AS day_of_week_num,
        DAYNAME(reading_ts)                                             AS day_of_week_name,
        CASE
            WHEN DAYOFWEEK(reading_ts) IN (0, 6) THEN TRUE
            ELSE FALSE
        END                                                             AS is_weekend,

        -- ----------------------------------------------------------------
        -- Meter state flags
        -- Boolean columns derived from meter_state for easier aggregation
        -- in mart models — SUM(is_fault_event) is cleaner than
        -- SUM(CASE WHEN meter_state = 'fault' THEN 1 ELSE 0 END) everywhere.
        -- ----------------------------------------------------------------
        CASE WHEN meter_state = 'degraded' THEN TRUE ELSE FALSE END     AS is_degraded,
        CASE WHEN meter_state = 'fault'    THEN TRUE ELSE FALSE END     AS is_fault,
        CASE WHEN meter_state = 'silent'   THEN TRUE ELSE FALSE END     AS is_silent,
        CASE WHEN meter_state = 'normal'   THEN TRUE ELSE FALSE END     AS is_normal,

        -- ----------------------------------------------------------------
        -- Power factor health flag
        -- Power factor below 0.85 indicates degraded electrical health
        -- regardless of whether the state machine has flagged it yet.
        -- Useful for catching early degradation signals in the mart layer.
        -- ----------------------------------------------------------------
        CASE
            WHEN power_factor < 0.85 THEN TRUE
            ELSE FALSE
        END                                                             AS is_low_power_factor,

        -- ----------------------------------------------------------------
        -- Voltage deviation from nominal
        -- Nominal voltages: residential 120V, commercial 240V, industrial 480V
        -- Deviation > 10% from nominal is a meaningful fault signal.
        -- ----------------------------------------------------------------
        CASE
            WHEN customer_type = 'residential' THEN ABS(voltage - 120) / 120
            WHEN customer_type = 'commercial'  THEN ABS(voltage - 240) / 240
            WHEN customer_type = 'industrial'  THEN ABS(voltage - 480) / 480
        END                                                             AS voltage_deviation_pct

        -- ----------------------------------------------------------------
        -- Pipeline latency
        -- Seconds between the message arriving at the Kafka broker and
        -- being loaded into Snowflake by Airflow. Useful for monitoring
        -- pipeline health — if this grows over time something is slowing down.
        -- NULL when kafka_ingested_at is missing (shouldn't happen in production).
        -- ----------------------------------------------------------------
        -- DATEDIFF(
        --     'second',
        --     kafka_ingested_at,
        --     load_ts
        -- )                                                               AS pipeline_latency_seconds

    FROM parsed

)

SELECT * FROM enriched