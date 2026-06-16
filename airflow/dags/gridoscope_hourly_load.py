"""
gridoscope_hourly_load.py

Hourly DAG that:
1. Senses S3 for the current hour's meter reading Parquet files
2. Validates the partition is complete
3. COPY INTOs Snowflake raw layer
4. Runs dbt staging models
5. Tests dbt staging models
6. Branches: runs marts on success, alerts on test failure
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule



DEFAULT_ARGS = {
    "owner": "gridoscope",
    "retries": 1, #2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email": ["aanuayodeji101@gmail.com"],
}

DBT_DIR = "/opt/airflow/dbt/gridoscope_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt/gridoscope_dbt" 

@dag(
    dag_id="gridoscope_hourly_load",
    schedule=None, #"15 * * * *",  # Modern 'schedule' replacing schedule_interval
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gridoscope", "ingestion", "snowflake"],
    doc_md="""
    ## Gridoscope Hourly Load
    Loads one hour of smart meter readings from S3 into Snowflake
    and runs dbt staging transformations.
    """,
)
def gridoscope_hourly_load():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # --- Sense S3 for the current hour's partition ----------------------------
    # Replaced 'execution_date' with modern 'logical_date'
    sense_meter_readings = S3KeySensor(
        task_id="sense_meter_readings_partition",
        bucket_name="gridoscope-raw-dev",
        # bucket_key=(
        #     "raw/meter.readings/"
        #     "year={{ logical_date.year }}/"
        #     "month={{ '%02d' | format(logical_date.month) }}/"
        #     "day={{ '%02d' | format(logical_date.day) }}/"
        #     "hour={{ '%02d' | format(logical_date.hour) }}/"
        #     "*.parquet"
        # ),
        bucket_key=(
            "raw/meter.readings/"
            "year={{ logical_date.year }}/"
            "month={{ '%02d' | format(logical_date.month) }}/"
            "day=09/"
            "hour=10/"
            "*.json"
        ),
        wildcard_match=True,
        aws_conn_id="gridoscope_s3_connect_dev",
        poke_interval=5,
        timeout=15,
        mode="reschedule",
    )

    # --- Validate partition completeness -------------------------------------
    @task(task_id="validate_partition")
    def validate_partition(logical_date):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="gridoscope_s3_connect_dev")
        # prefix = (
        #     f"raw/meter.readings/"
        #     f"year={logical_date.year}/"
        #     f"month={logical_date.month:02d}/"
        #     f"day={logical_date.day:02d}/"
        #     f"hour={logical_date.logical_date.hour:02d}/"
        # )
        prefix = (
            f"raw/meter.readings/"
            f"year={logical_date.year}/"
            f"month={logical_date.month:02d}/"
            f"day=09/"
            f"hour=10/"
        )
        keys = hook.list_keys(bucket_name="gridoscope-raw-dev", prefix=prefix) or []
        json_files = [k for k in keys if k.endswith(".json")]

        if not json_files:
            raise ValueError(f"Partition empty: s3://gridoscope-raw-dev/{prefix}")

        print(f"Partition validated: {len(json_files)} files at {prefix}")
        # Simply return data to automatically push to XCom
        return {"file_count": len(json_files), "s3_prefix": prefix}
    

    # # --- Load into Snowflake raw layer ----------------------------------------
    copy_into_raw = SQLExecuteQueryOperator(
        task_id="copy_into_raw_meter_readings",
        conn_id="gridoscope_snowflake_dev",
        sql="sql/copy_into_raw.sql"
    )

    # # --- Run dbt staging models ----------------------------------------------
    @task.bash(task_id="dbt_run_staging")
    def dbt_run_staging(dbt_dir: str, profiles_dir: str) -> str:
        return f"cd {dbt_dir} && dbt run --profiles-dir {profiles_dir} --select staging"


    # --- Test staging models --------------------------------------------------
    @task.bash(task_id="dbt_test_staging")
    def dbt_test_staging(dbt_dir: str, profiles_dir: str) -> str:
        # raise ValueError("Simulated dbt test failure for demonstration purposes.")
        return f"cd {dbt_dir} && dbt test --profiles-dir {profiles_dir} --select staging"
    

    # --- Branch: run marts OR alert on test failures --------------------------
    # Modern approach: Use @task.branch. Intercept the up-stream state securely via XComs
    @task.branch(task_id="branch_on_staging_tests", trigger_rule=TriggerRule.ALL_DONE)
    def branch_on_test_results(ti=None):
        # Airflow 3 safe macro method to check if the upstream task generated an output
        # (Failing bash tasks throw an error and never push an XCom value)
        test_output = ti.xcom_pull(task_ids="dbt_test_staging", key="return_value")

        if test_output is not None:
            return "dbt_run_marts"
            
        return "notify_test_failure"

    # --- Run mart models ------------------------------------------------------
    @task.bash
    def dbt_run_marts(dbt_dir: str, profiles_dir: str) -> str:
        """Runs the production dbt marts models."""
        return f"cd {dbt_dir} && dbt run --profiles-dir {profiles_dir} --select mart"


    # --- Alert task -----------------------------------------------------------
    @task(task_id="notify_test_failure")
    def notify_test_failure(ds):
        print(f"ALERT: dbt staging tests failed for execution_date={ds}")
        print("Check task logs for dbt_test_staging details.")

    # # --- Wire up dependencies -------------------------------------------------
    validation_step = validate_partition()
    dbt_run_staging_step = dbt_run_staging(
        dbt_dir=DBT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )
    dbt_test_staging_step = dbt_test_staging(
        dbt_dir=DBT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )
    run_marts_step = dbt_run_marts(dbt_dir=DBT_DIR, profiles_dir=DBT_PROFILES_DIR)
    branch_step = branch_on_test_results()
    alert_step = notify_test_failure()

    start >> sense_meter_readings >> validation_step >> copy_into_raw >> dbt_run_staging_step >> dbt_test_staging_step >> branch_step
    # start >> dbt_test_staging >> branch_step
    branch_step >> [run_marts_step, alert_step] >> end


dag_instance = gridoscope_hourly_load()
