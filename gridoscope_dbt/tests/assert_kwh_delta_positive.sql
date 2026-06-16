select *
from {{ ref('stg_meter_readings') }}
where kwh_delta < 0

-- where kwh_delta < 0
