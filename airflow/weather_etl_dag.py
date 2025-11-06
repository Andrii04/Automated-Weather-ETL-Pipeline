from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract import extract_data
from clean_transform import clean_data
from load import load_data

default_args = {
    "owner" : "airflow",
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Automated ETL for weather data",
    schedule="0 7 * * *",
    start_date=datetime(2025, 8, 19),
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    extract_task >> clean_task >> load_task