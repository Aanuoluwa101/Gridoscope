from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator


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
    def simulate_s3_sense(logical_date):
        print(f"Simulating S3 sense for logical_date={logical_date}")
        print("Found 5 Parquet files in S3 partition (simulated)")

    @task(task_id="simulate_load")
    def simulate_snowflake_load():
        print("Simulating COPY INTO raw.meter_readings")
        # 3. Simply return the value to automatically push it to XCom
        return 12450

    @task(task_id="run_dbt_staging")
    def simulate_dbt_run(rows_loaded: int):
        # 4. Pass the output of the previous task directly into the function signature
        print(f"Running dbt staging models against {rows_loaded} new raw rows (simulated)")

    # Call the decorated tasks to instantiate them
    sense_output = simulate_s3_sense()
    rows_output = simulate_snowflake_load()
    dbt_output = simulate_dbt_run(rows_output)

    # Clean, modern dependency layout
    start >> sense_output >> rows_output >> dbt_output >> end

dag_instance = gridoscope_test()



dag_instance = gridoscope_test()