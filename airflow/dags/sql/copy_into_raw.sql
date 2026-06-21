COPY INTO GRIDOSCOPE_PROD.RAW.METER_EVENTS (src, load_ts)
FROM (
    SELECT
        PARSE_JSON($1),
        CURRENT_TIMESTAMP()
    FROM @GRIDOSCOPE_PROD.RAW.GRIDOSCOPE_STAGE_PROD
)
PATTERN = '.*meter\.readings/year={{ logical_date.strftime("%Y") }}/month={{ logical_date.strftime("%m") }}/day={{ logical_date.strftime("%d") }}/hour={{ logical_date.strftime("%H") }}/.*\.json'
FILE_FORMAT = (TYPE = JSON, STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = 'CONTINUE';
