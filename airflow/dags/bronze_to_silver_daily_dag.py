from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

# ------------------------------------------------------------
# Default DAG arguments
# ------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------
with DAG(
    dag_id="bronze_to_silver_daily",
    default_args=default_args,
    description="Manual Bronze→Silver Spark batch job (no data quality checks)",
    schedule_interval=None,  # Will set to "0 17 * * *" later
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["iceberg", "spark", "lakehouse"],
) as dag:

    # --------------------------------------------------------
    # Run Spark Bronze→Silver job via Docker
    # --------------------------------------------------------
    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver_batch",
        image="spark-custom:latest",
        command="/opt/spark/bin/spark-submit /opt/spark-apps/batch/BronzeToSilverDailyBatch.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="trading-analytics-platform_default",
        mounts=[
            Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
            Mount(source=f"{os.getcwd()}/spark", target="/opt/spark-apps", type="bind"),
            Mount(source="/opt/airflow/logs", target="/opt/airflow/logs", type="bind"),
        ],
        auto_remove=True,
        tty=True,
    )

    bronze_to_silver
