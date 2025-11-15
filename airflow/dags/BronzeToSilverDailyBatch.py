from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_to_silver_daily",
    default_args=default_args,
    description="Bronze→Silver Spark batch job",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["iceberg", "spark", "lakehouse"],
) as dag:

    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver_batch",
        image="spark-custom:latest",
        command="/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/batch/BronzeToSilverDailyBatch.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="trading-analytics-platform_default",
        
        # Mount both the code AND the config
        mounts=[
            Mount(
                source="/var/lib/docker/volumes/trading-analytics-platform_spark_code/_data",
                target="/opt/spark-apps",
                type="bind"
            ),
            Mount(
                source="/var/lib/docker/volumes/trading-analytics-platform_spark_code/_data/conf/spark-defaults.conf",  # ← Add config mount
                target="/opt/spark/conf/spark-defaults.conf",
                type="bind"
            ),
        ],
        mount_tmp_dir=False,
        
        auto_remove=True,
        tty=True,
    )

    bronze_to_silver