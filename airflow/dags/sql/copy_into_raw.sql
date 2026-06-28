COPY INTO GRIDOSCOPE_PROD.RAW.METER_EVENTS (src, load_ts)
FROM (
    SELECT
        PARSE_JSON($1),
        CURRENT_TIMESTAMP()
    FROM @GRIDOSCOPE_PROD.RAW.GRIDOSCOPE_STAGE_PROD
)
-- TEMP: 8-day backfill (Jul 1-8) — restore the dynamic line below when done
-- PATTERN = '.*meter\.readings/year={{ logical_date.strftime("%Y") }}/month={{ logical_date.strftime("%m") }}/day={{ logical_date.strftime("%d") }}/hour={{ logical_date.strftime("%H") }}/.*\.json'
-- TEMP: Jul 9-31
PATTERN = '.*meter\.readings/year=2026/month=07/day=(09|1[0-9]|2[0-9]|3[01])/.*\.json'
-- TEMP: Jul 1 all hours
-- PATTERN = '.*meter\.readings/year=2026/month=07/day=01/.*\.json'
FILE_FORMAT = (TYPE = JSON, STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = 'CONTINUE';
