from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
import json 
import os 



def _run_dbt(args: list, dbt_dir: str, profiles_dir: str) -> str:
    """
    Run a dbt command with credentials read from the Airflow Snowflake
    connection at task execution time.

    The connection (gridoscope_snowflake_dbt_prod) is backed by Secrets
    Manager, so no credentials live in the DAG file or env vars.
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("gridoscope_snowflake_dbt_prod")
    extra = json.loads(conn.extra or "{}")

    env = {
        **os.environ,
        "SNOWFLAKE_ACCOUNT": extra.get("account", conn.host),
        "SNOWFLAKE_DBT_USER": conn.login,
        "SNOWFLAKE_DBT_PASSWORD": conn.password,
    }
    cmd = ["dbt"] + args + ["--profiles-dir", profiles_dir]
    print("Got credentials")
    print(f'account: {env.get("SNOWFLAKE_ACCOUNT", "")[-2:]}')
    print(f'user: {env.get("SNOWFLAKE_DBT_USER", "")[-2:]}')
    print(f'password: {env.get("SNOWFLAKE_DBT_PASSWORD", "")[-2:]}')
    print(f'Running {" ".join(cmd)}')


@dag(
    dag_id="gridoscope_test",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["gridoscope", "test"],
)
def gridoscope_test():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1. Use @task instead of PythonOperator. 
    # 2. Inject specific context keys directly into the signature instead of **context.
    @task(task_id="sense_s3_files")
    def simulate_s3_sense():
        print("Simulating S3 sense for logical_date=today")
        print("Found 5 Parquet files in S3 partition (simulated)")

    @task(task_id="simulate_load")
    def simulate_snowflake_load():
        print("Simulating COPY INTO raw.meter_readings")
        # 3. Simply return the value to automatically push it to XCom
        return 12450

    @task(task_id="dbt_run_staging")
    def dbt_run_staging(dbt_dir: str, profiles_dir: str) -> str:
        return _run_dbt(["run", "--select", "staging"], dbt_dir, profiles_dir)
    
    # Call the decorated tasks to instantiate them
    sense_output = simulate_s3_sense()
    rows_output = simulate_snowflake_load()
    dbt_run_staging_step = dbt_run_staging(dbt_dir="dbt_actual_dir", profiles_dir="dbt_profile_actual_dir")

    # Clean, modern dependency layout
    start >> sense_output >> rows_output >> dbt_run_staging_step >> end

dag_instance = gridoscope_test()