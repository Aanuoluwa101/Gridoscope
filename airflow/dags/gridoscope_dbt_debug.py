"""
gridoscope_dbt_debug.py 
Runs dbt staging in isolation
"""

import json
import os
import subprocess
import sys
from datetime import datetime

from airflow.decorators import dag, task

DBT_DIR = "/usr/local/airflow/dags/dbt/gridoscope_dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/dags/dbt/gridoscope_dbt"
_DBT_VENV = "/tmp/dbt_venv"


def _run_dbt(args: list, dbt_dir: str, profiles_dir: str) -> str:
    from airflow.hooks.base import BaseHook

    dbt_bin = f"{_DBT_VENV}/bin/dbt"
    if not os.path.exists(dbt_bin):
        print("Creating isolated dbt venv (~2 min)...")
        subprocess.run([sys.executable, "-m", "venv", _DBT_VENV], check=True)
        subprocess.run(
            [f"{_DBT_VENV}/bin/pip", "install", "--quiet", "dbt-snowflake~=1.7.0"],
            check=True,
        )

    # Sanity-check: verify the binary actually works before the real command
    ver = subprocess.run([dbt_bin, "--version"], capture_output=True, text=True)
    print(f"dbt --version returncode: {ver.returncode}")
    print(f"dbt --version stdout: {ver.stdout.strip()}")
    print(f"dbt --version stderr: {ver.stderr.strip()}")

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
    ]
    result = subprocess.run(cmd, cwd=dbt_dir, env=env, capture_output=True, text=True)
    print(f"dbt returncode: {result.returncode}")
    print(result.stdout)

    # Surface the dbt log file for extra context on failures
    dbt_log = "/tmp/dbt_logs/dbt.log"
    if os.path.exists(dbt_log):
        with open(dbt_log) as f:
            print(f"--- /tmp/dbt_logs/dbt.log ---\n{f.read()[-4000:]}")

    if result.returncode != 0:
        raise RuntimeError(
            f"dbt {args[0]} failed (rc={result.returncode}):\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout


@dag(
    dag_id="gridoscope_dbt_debug",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "owner": "gridoscope"},
    tags=["gridoscope", "debug"],
)
def gridoscope_dbt_debug():

    @task(task_id="check_dbt_env")
    def check_dbt_env():
        print(f"DBT_DIR exists: {os.path.exists(DBT_DIR)}")
        if os.path.exists(DBT_DIR):
            for root, dirs, files in os.walk(DBT_DIR):
                dirs[:] = [d for d in dirs if d != "target"]
                level = root.replace(DBT_DIR, "").count(os.sep)
                print("  " * level + os.path.basename(root) + "/")
                for f in files:
                    print("  " * (level + 1) + f)
        print(f"profiles.yml exists: {os.path.exists(os.path.join(DBT_PROFILES_DIR, 'profiles.yml'))}")
        print(f"dbt venv exists: {os.path.exists(f'{_DBT_VENV}/bin/dbt')}")

    @task(task_id="dbt_run_staging")
    def dbt_run_staging():
        return _run_dbt(["run", "--select", "mart_zone_daily"], DBT_DIR, DBT_PROFILES_DIR)

    check_dbt_env() >> dbt_run_staging()


dag_instance = gridoscope_dbt_debug()
