
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Pipeline NYC Taxi - Airflow + PySpark + PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["wecogno", "spark", "nyc-taxi"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    raw = BashOperator(
        task_id="raw",
        bash_command=(
            "spark-submit "
            "--master local[2] "
            "--driver-memory 2g "
            "--jars /opt/airflow/jars/postgresql.jar "
            "/opt/airflow/jobs/raw.py"
        ),
    )

    trusted = BashOperator(
        task_id="trusted",
        bash_command=(
            "spark-submit "
            "--master local[2] "
            "--driver-memory 2g "
            "--jars /opt/airflow/jars/postgresql.jar "
            "/opt/airflow/jobs/trusted.py"
        ),
    )

    refined = BashOperator(
        task_id="refined",
        bash_command=(
            "spark-submit "
            "--master local[2] "
            "--driver-memory 2g "
            "--jars /opt/airflow/jars/postgresql.jar "
            "/opt/airflow/jobs/refined.py"
        ),
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> raw >> trusted >> refined >> end
