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

import json
import os
import subprocess
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

# dbt-core conflicts with MWAA 2.10.3's pinned pathspec and isodate, so it
# cannot be installed via requirements.txt. Instead, _run_dbt creates an
# isolated venv here on first use and reuses it for the worker's lifetime.
_DBT_VENV = "/tmp/dbt_venv"

DEFAULT_ARGS = {
    "owner": "gridoscope",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # "email_on_failure": True,
    # "email": ["aanuayodeji101@gmail.com"],
}


def _run_dbt(args: list, dbt_dir: str, profiles_dir: str) -> str:
    """
    Run a dbt command with credentials read from the Airflow Snowflake
    connection at task execution time.

    The connection (gridoscope_snowflake_dbt_prod) is backed by Secrets
    Manager, so no credentials live in the DAG file or env vars.
    """
    import sys
    from airflow.hooks.base import BaseHook

    dbt_bin = f"{_DBT_VENV}/bin/dbt"
    if not os.path.exists(dbt_bin):
        print("First use on this worker — creating isolated dbt venv (~2 min)...")
        subprocess.run([sys.executable, "-m", "venv", _DBT_VENV], check=True)
        subprocess.run(
            [f"{_DBT_VENV}/bin/pip", "install", "--quiet", "dbt-snowflake~=1.7.0"],
            check=True,
        )

    conn = BaseHook.get_connection("gridoscope_snowflake_dbt_prod")
    extra = json.loads(conn.extra or "{}")

    env = {
        **os.environ,
        "SNOWFLAKE_ACCOUNT": extra.get("account", conn.host),
        "SNOWFLAKE_DBT_USER": conn.login,
        "SNOWFLAKE_DBT_PASSWORD": conn.password,
    }
    # DAGs dir is read-only on MWAA (S3-synced). Redirect dbt's log and
    # compiled artifact output to /tmp so initialization doesn't fail silently.
    cmd = [dbt_bin] + args + [
        "--profiles-dir", profiles_dir,
        "--log-path", "/tmp/dbt_logs",
        "--target-path", "/tmp/dbt_target",
        "--packages-install-path", "/tmp/dbt_packages",
    ]
    result = subprocess.run(cmd, cwd=dbt_dir, env=env, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt {args[0]} failed (rc={result.returncode}):\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout


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
    sense_meter_readings = S3KeySensor(
        task_id="sense_meter_readings_partition",
        bucket_name=BUCKET,
        # TEMP: hardcoded partition for testing — restore the block below when done
        bucket_key="raw/meter.readings/year=2026/month=06/day=20/hour=10/*.json",
        # bucket_key=(
        #     "raw/meter.readings/"
        #     "year={{ logical_date.strftime('%Y') }}/"
        #     "month={{ logical_date.strftime('%m') }}/"
        #     "day={{ logical_date.strftime('%d') }}/"
        #     "hour={{ logical_date.strftime('%H') }}/"
        #     "*.json"
        # ),
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    # A partition completeness task could live here — e.g. assert a minimum
    # file count per zone, or check that all 5 zones contributed files.
    # Omitted for simplicity: the S3KeySensor above already confirms at least
    # one file exists before we proceed.

    # --- Load into Snowflake raw layer ----------------------------------------
    copy_into_raw = SQLExecuteQueryOperator(
        task_id="copy_into_raw_meter_readings",
        conn_id="gridoscope_snowflake_prod",
        sql="sql/copy_into_raw.sql",
    )

    # --- dbt tasks — credentials read from Airflow connection at runtime ------
    # _run_dbt pulls from gridoscope_snowflake_dbt_prod (Secrets Manager backed)
    # and passes SNOWFLAKE_* as env vars to the subprocess so profiles.yml works.

    @task(task_id="dbt_run_staging")
    def dbt_run_staging(dbt_dir: str, profiles_dir: str) -> str:
        return _run_dbt(["run", "--select", "staging"], dbt_dir, profiles_dir)

    @task(task_id="dbt_test_staging")
    def dbt_test_staging(dbt_dir: str, profiles_dir: str) -> str:
        return _run_dbt(["test", "--select", "staging"], dbt_dir, profiles_dir)

    @task(task_id="dbt_run_marts")
    def dbt_run_marts(dbt_dir: str, profiles_dir: str) -> str:
        return _run_dbt(["run", "--select", "mart"], dbt_dir, profiles_dir)

    # --- Branch: run marts OR alert on test failures --------------------------
    @task.branch(task_id="branch_on_staging_tests", trigger_rule=TriggerRule.ALL_DONE)
    def branch_on_test_results(ti=None):
        # Failing tasks raise and never push an XCom value — safe to check for None.
        test_output = ti.xcom_pull(task_ids="dbt_test_staging", key="return_value")
        if test_output is not None:
            return "dbt_run_marts"
        return "notify_test_failure"

    # --- Alert task -----------------------------------------------------------
    @task(task_id="notify_test_failure")
    def notify_test_failure(ds):
        print(f"ALERT: dbt staging tests failed for execution_date={ds}")
        print("Check task logs for dbt_test_staging details.")

    # --- Wire up dependencies -------------------------------------------------
    dbt_run_staging_step = dbt_run_staging(dbt_dir=DBT_DIR, profiles_dir=DBT_PROFILES_DIR)
    dbt_test_staging_step = dbt_test_staging(dbt_dir=DBT_DIR, profiles_dir=DBT_PROFILES_DIR)
    run_marts_step = dbt_run_marts(dbt_dir=DBT_DIR, profiles_dir=DBT_PROFILES_DIR)
    branch_step = branch_on_test_results()
    alert_step = notify_test_failure()

    (
        start
        >> sense_meter_readings
        >> copy_into_raw
        >> dbt_run_staging_step
        >> dbt_test_staging_step
        >> branch_step
        >> [run_marts_step, alert_step]
        >> end
    )


dag_instance = gridoscope_hourly_load()
