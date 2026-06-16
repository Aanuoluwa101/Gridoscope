COPY INTO SNOWFLAKE_LEARNING_DB.RAW.METER_EVENTS (src, load_ts)
FROM (
    SELECT
        PARSE_JSON($1),        -- ← parses the string into a proper JSON object
        CURRENT_TIMESTAMP()
    FROM @GRIDOSCOPE_STAGE_DEV
)
PATTERN = '.*meter\.readings/year={{ logical_date.year }}/month={{ '%02d' | format(logical_date.month) }}/day={{ '%02d' | format(logical_date.day) }}/hour={{ '%02d' | format(logical_date.hour) }}/.*\.json'
FILE_FORMAT = (TYPE = JSON, STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = 'CONTINUE';