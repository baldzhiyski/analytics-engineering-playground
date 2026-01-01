from __future__ import annotations

from datetime import datetime
import subprocess

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

def run_elt_script():
    script_path = "/opt/airflow/elt/elt_script.py"
    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ELT script failed:\n{result.stderr}")
    print(result.stdout)

with DAG(
    dag_id="elt_and_dbt",
    default_args=default_args,
    description="An ELT workflow with dbt",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",   # use "@once" for a single run
    catchup=False,
    tags=["elt", "dbt"],
) as dag:

    t1 = PythonOperator(
        task_id="run_elt_script",
        python_callable=run_elt_script,
    )

    # IMPORTANT:
    # - These mounts are Linux paths INSIDE the Airflow container (provided by docker-compose volumes)
    # - DockerOperator will re-use them for the dbt container
    t2 = DockerOperator(
        task_id="dbt_run",
        image="ghcr.io/dbt-labs/dbt-postgres:1.4.7",
        command="run --profiles-dir /root/.dbt --project-dir /opt/dbt --full-refresh",
        docker_url="unix:///var/run/docker.sock",
        network_mode="elt_network",
        auto_remove="success",
        mounts=[
            Mount(source="/opt/dbt", target="/opt/dbt", type="bind"),
            Mount(source="/root/.dbt", target="/root/.dbt", type="bind"),
        ],
    )

    t1 >> t2
