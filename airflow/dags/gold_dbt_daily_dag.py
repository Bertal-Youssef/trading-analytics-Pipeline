# airflow/dags/gold_dbt_daily_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# === CONFIG ===
DBT_IMAGE = "dbt-trino-local:1.8.4"
NETWORK_MODE = "trading-analytics-platform_default"
HOST_DBT_PATH = r"C:\Users\MSI\Documents\trading-analytics-platform\dbt"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gold_dbt_daily",
    description="Run dbt Gold models after Silver completes",
    schedule_interval=None,  # Triggered by bronze_to_silver_daily DAG
    start_date=datetime(2025, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "gold", "lakehouse"],
) as dag:

    # dbt run (build Gold table)
    dbt_run_gold = DockerOperator(
        task_id="dbt_run_gold",
        image=DBT_IMAGE,
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK_MODE,
        command="bash -lc 'dbt run --project-dir /usr/app --profiles-dir /usr/app --select gold.daily_trade_metrics'",
        mounts=[
            Mount(source=HOST_DBT_PATH, target="/usr/app", type="bind"),
        ],
        mount_tmp_dir=False,
        auto_remove=True,
        tty=True,
    )

    # dbt test (validate Gold)
    dbt_test_gold = DockerOperator(
        task_id="dbt_test_gold",
        image=DBT_IMAGE,
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK_MODE,
        command="bash -lc 'dbt test --project-dir /usr/app --profiles-dir /usr/app --select gold.daily_trade_metrics'",
        mounts=[
            Mount(source=HOST_DBT_PATH, target="/usr/app", type="bind"),
        ],
        mount_tmp_dir=False,
        auto_remove=True,
        tty=True,
    )

    dbt_run_gold >> dbt_test_gold