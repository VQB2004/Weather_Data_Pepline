from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
from weather_etl import load_to_minio, load_from_minio_to_postgre


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="ETL weather data (API → MinIO → PostgreSQL) with TaskGroup",
    schedule="@hourly",  # chạy mỗi giờ
    start_date=datetime(2025, 9, 4),
    catchup=False,
) as dag:

    # TaskGroup Transform
    with TaskGroup("extract") as extract_group:
        task_load_to_minio = PythonOperator(
            task_id="load_to_minio",
            python_callable=load_to_minio
        )

    # TaskGroup Load
    with TaskGroup("load") as load_group:
        task_load_to_postgre = PythonOperator(
            task_id="load_from_minio_to_postgre",
            python_callable=load_from_minio_to_postgre,
        )

    # Thiết lập luồng
    extract_group >> load_group
