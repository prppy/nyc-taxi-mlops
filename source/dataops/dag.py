from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from dataops.extract import extract_taxi, extract_weather, extract_lookup
from dataops.transform import transform_fact, transform_dim_zone, transform_dim_weather
from dataops.load import load_data
from dataops.validate import validate_raw, validate_processed
from dataops.verify_watermark import validate_data_watermark
from dataops.report import report_data

from utils.alerting import on_failure_alert
from utils.db import setup_tables
from utils.config import (DATAOPS_DAG_ID, DATAOPS_START_YEAR, DATAOPS_START_MONTH, SCHEDULE_INTERVAL, RETRY_COUNT)

default_args = {
    "owner": "dataops",
    "start_date": datetime(DATAOPS_START_YEAR, DATAOPS_START_MONTH, 1),
    "retries": RETRY_COUNT,
    "on_failure_callback": on_failure_alert,
}

with DAG(
    dag_id=DATAOPS_DAG_ID,
    default_args=default_args,
    schedule=SCHEDULE_INTERVAL,
    catchup=True, 
    on_failure_callback=on_failure_alert, 
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["dataops"],
) as dag:
    
    setup_task = PythonOperator(
        task_id="setup_tables",
        python_callable=setup_tables
    )

    extract_taxi_task = PythonOperator(
        task_id="extract_taxi",
        python_callable=extract_taxi
    )

    extract_weather_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather
    )
    
    extract_lookup_task = PythonOperator(
        task_id="extract_lookup",
        python_callable=extract_lookup
    )

    validate_raw_task = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw
    )

    transform_fact_task = PythonOperator(
        task_id="transform_fact",
        python_callable=transform_fact,
        pool="global_serial_pool",
    )
    
    transform_dim_zone_task = PythonOperator(
        task_id="transform_dim_zone",
        python_callable=transform_dim_zone
    )   
    
    transform_dim_weather_task = PythonOperator(
        task_id="transform_dim_weather",
        python_callable=transform_dim_weather
    )

    validate_processed_task = PythonOperator(
        task_id="validate_processed",
        python_callable=validate_processed
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_data,
        pool="global_serial_pool",
    )

    report_task = PythonOperator(
        task_id="report_data",
        python_callable=report_data,
    )

    watermark_audit_task = PythonOperator(
        task_id="watermark_audit",
        python_callable=validate_data_watermark
    )

    # DAG dependencies
    setup_task >> [extract_taxi_task, extract_weather_task, extract_lookup_task] >> validate_raw_task
    transform_tasks = [transform_dim_zone_task, transform_dim_weather_task]
    validate_raw_task >> transform_tasks >> transform_fact_task >> validate_processed_task 
    validate_processed_task >> load_task >> report_task >> watermark_audit_task