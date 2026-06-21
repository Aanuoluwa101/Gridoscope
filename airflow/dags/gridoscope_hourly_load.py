"""
gridoscope_hourly_load.py

Hourly DAG that:
1. Senses S3 for the current hour's meter reading JSON files
2. Validates the partition is complete
3. COPY INTOs Snowflake raw layer
4. Runs dbt staging models
5. Tests dbt staging models
6. Branches: runs marts on success, alerts on test failure
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule


BUCKET = "gridoscope-raw-prod"

# dbt project lives under mwaa/dags/dbt/ in S3, which MWAA syncs to
# /usr/local/airflow/dags/dbt/ on every worker.
DBT_DIR = "/usr/local/airflow/dags/dbt/gridoscope_dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/gridoscope_dbt"

DEFAULT_ARGS = {
    "owner": "gridoscope",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["aanuayodeji101@gmail.com"],
}


@dag(
    dag_id="gridoscope_hourly_load",
    schedule="15 * * * *",  # 15 past each hour — MSK Connect flushes on the hour
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gridoscope", "ingestion", "snowflake"],
    doc_md="""
    ## Gridoscope Hourly Load
    Loads one hour of smart meter readings from S3 into Snowflake
    and runs dbt staging and mart transformations.
    """,
)
def gridoscope_hourly_load():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # --- Sense S3 for the current hour's partition ----------------------------
    # logical_date is the data interval start — runs at :15 sense for :00 data.
    sense_meter_readings = S3KeySensor(
        task_id="sense_meter_readings_partition",
        bucket_name=BUCKET,
        bucket_key=(
            "raw/meter.readings/"
            "year={{ logical_date.strftime('%Y') }}/"
            "month={{ logical_date.strftime('%m') }}/"
            "day={{ logical_date.strftime('%d') }}/"
            "hour={{ logical_date.strftime('%H') }}/"
            "*.json"
        ),
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    # --- Validate partition completeness -------------------------------------
    @task(task_id="validate_partition")
    def validate_partition(logical_date):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="aws_default")
        prefix = (
            f"raw/meter.readings/"
            f"year={logical_date.strftime('%Y')}/"
            f"month={logical_date.strftime('%m')}/"
            f"day={logical_date.strftime('%d')}/"
            f"hour={logical_date.strftime('%H')}/"
        )
        keys = hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
        json_files = [k for k in keys if k.endswith(".json")]

        if not json_files:
            raise ValueError(f"Partition empty: s3://{BUCKET}/{prefix}")

        print(f"Partition validated: {len(json_files)} files at {prefix}")
        return {"file_count": len(json_files), "s3_prefix": prefix}

    # --- Load into Snowflake raw layer ----------------------------------------
    copy_into_raw = SQLExecuteQueryOperator(
        task_id="copy_into_raw_meter_readings",
        conn_id="gridoscope_snowflake_prod",
        sql="sql/copy_into_raw.sql",
    )

    # --- Run dbt staging models -----------------------------------------------
    @task.bash(task_id="dbt_run_staging")
    def dbt_run_staging(dbt_dir: str, profiles_dir: str) -> str:
        return f"cd {dbt_dir} && dbt run --profiles-dir {profiles_dir} --select staging"

    # --- Test staging models --------------------------------------------------
    @task.bash(task_id="dbt_test_staging")
    def dbt_test_staging(dbt_dir: str, profiles_dir: str) -> str:
        return f"cd {dbt_dir} && dbt test --profiles-dir {profiles_dir} --select staging"

    # --- Branch: run marts OR alert on test failures --------------------------
    @task.branch(task_id="branch_on_staging_tests", trigger_rule=TriggerRule.ALL_DONE)
    def branch_on_test_results(ti=None):
        test_output = ti.xcom_pull(task_ids="dbt_test_staging", key="return_value")
        if test_output is not None:
            return "dbt_run_marts"
        return "notify_test_failure"

    # --- Run mart models ------------------------------------------------------
    @task.bash(task_id="dbt_run_marts")
    def dbt_run_marts(dbt_dir: str, profiles_dir: str) -> str:
        return f"cd {dbt_dir} && dbt run --profiles-dir {profiles_dir} --select mart"

    # --- Alert task -----------------------------------------------------------
    @task(task_id="notify_test_failure")
    def notify_test_failure(ds):
        print(f"ALERT: dbt staging tests failed for execution_date={ds}")
        print("Check task logs for dbt_test_staging details.")

    # --- Wire up dependencies -------------------------------------------------
    validation_step = validate_partition()
    dbt_run_staging_step = dbt_run_staging(dbt_dir=DBT_DIR, profiles_dir=DBT_PROFILES_DIR)
    dbt_test_staging_step = dbt_test_staging(dbt_dir=DBT_DIR, profiles_dir=DBT_PROFILES_DIR)
    run_marts_step = dbt_run_marts(dbt_dir=DBT_DIR, profiles_dir=DBT_PROFILES_DIR)
    branch_step = branch_on_test_results()
    alert_step = notify_test_failure()

    (
        start
        >> sense_meter_readings
        >> validation_step
        >> copy_into_raw
        >> dbt_run_staging_step
        >> dbt_test_staging_step
        >> branch_step
        >> [run_marts_step, alert_step]
        >> end
    )


dag_instance = gridoscope_hourly_load()
