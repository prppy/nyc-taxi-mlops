from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from dataops.extract import extract_taxi, extract_weather, extract_lookup
from dataops.transform import transform_fact, transform_dim_zone, transform_dim_weather
from dataops.load import load_data
from dataops.validate import validate_raw, validate_processed
from dataops.lineage import update_lineage
from dataops.watermark import update_watermark

from utils.config import (DAG_ID, SCHEDULE_INTERVAL, RETRY_COUNT)

default_args = {
    "owner": "dataops",
    "start_date": datetime(2024, 1, 1),
    "retries": RETRY_COUNT,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
) as dag:

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
        python_callable=transform_fact
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
        python_callable=load_data
    )

    lineage_task = PythonOperator(
        task_id="lineage",
        python_callable=update_lineage
    )

    watermark_task = PythonOperator(
        task_id="watermark",
        python_callable=update_watermark
    )

    # DAG dependencies
    [extract_taxi_task, extract_weather_task, extract_lookup_task] >> validate_raw_task
    validate_raw_task >> [
        transform_fact_task,
        transform_dim_zone_task,
        transform_dim_weather_task
    ]
    [
        transform_fact_task,
        transform_dim_zone_task,
        transform_dim_weather_task
    ] >> validate_processed_task
    validate_processed_task >> load_task
    load_task >> lineage_task >> watermark_task