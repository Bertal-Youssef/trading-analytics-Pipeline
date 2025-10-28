from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
default_args = {"owner": "data", "retries": 0}
with DAG(
    dag_id="silver_refresh_daily",
    schedule="0 17 * * *",
    start_date=datetime(2024,1,1),
    catchup=False,
    default_args=default_args,
    tags=["batch","silver"],
) as dag:
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="spark-submit /opt/spark-apps/batch/bronze_to_silver.py"
    )
