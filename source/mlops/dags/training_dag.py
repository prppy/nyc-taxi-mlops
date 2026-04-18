"""
WORKFLOW:
1. Waits for DataOps pipeline (scheduled runs) or skips wait (manual/drift triggers)
2. EDA on the latest ingested data
3. Feature engineering (Genesis backfill on first run, incremental on subsequent runs)
4. Drift detection — always runs, saves results to DB and sends email if needed
5. Branching: train if Jan/July (scheduled 6-month retrain) OR high/critical drift
6. Train model (3-year sliding window ending at execution date)
7. Evaluate against Production model guardrails
8. Register to MLflow if approved (or auto-approve on first ever model)

TRIGGERED:
  - @monthly schedule (last day of month)
  - Manual trigger via Airflow UI
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/source')

from mlops.tasks import (
    check_trigger_source,
    run_eda_task,
    run_feature_engineering_task,
    calculate_and_log_drift_task,
    evaluate_training_trigger,
    train_model_task,
    evaluate_model_task,
    register_model_task,
)
from utils.alerting import on_failure_alert
from utils.config import (
    TRAIN_DAG_ID,
    MLOPS_START_YEAR,
    MLOPS_START_MONTH,
    RETRY_COUNT,
    DATAOPS_DAG_ID,
    SCHEDULE_INTERVAL,
)

default_args = {
    "owner": "mlops",
    "start_date": datetime(MLOPS_START_YEAR, MLOPS_START_MONTH, 1),
    "retries": RETRY_COUNT,
    "retry_delay": timedelta(minutes=5), #change for testing
    "on_failure_callback": on_failure_alert,
}

with DAG(
    dag_id=TRAIN_DAG_ID,
    default_args=default_args,
    description="Monthly MLOps pipeline: EDA → FE → Drift → [Train → Evaluate → Register]",
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=True,  # backfills the 12 MLOps runs from MLOPS_START_DATE onwards
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["mlops", "training"],
) as dag:

    # Trigger source check to decide whether to wait for data pipeline or skip
    check_trigger = BranchPythonOperator(
        task_id="check_trigger_source",
        python_callable=check_trigger_source,
    )

    wait_for_data = ExternalTaskSensor(
        task_id="wait_for_data_pipeline",
        external_dag_id=DATAOPS_DAG_ID,
        external_task_id="watermark_audit", #last task in dataops dag
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        timeout=7200, #2 hours timeout to prevent infinite waiting
        poke_interval=300, #check every 5 minutes
    )

    skip_wait = EmptyOperator(task_id="skip_wait")

    # Core monthly tasks 
    eda_task = PythonOperator(
        task_id="run_eda",
        python_callable=run_eda_task,
        trigger_rule="none_failed_min_one_success",
    )

    feature_eng_task = PythonOperator(
        task_id="feature_engineering",
        python_callable=run_feature_engineering_task,
        pool="global_serial_pool",
    )

    # always runs — saves drift scores to DB for frontend dashboard
    drift_task = PythonOperator(
        task_id="calculate_and_log_drift",
        python_callable=calculate_and_log_drift_task,
        pool="global_serial_pool",
    )

    # trains if: data month is Jan/July (scheduled) OR drift is High/Critical
    training_trigger = BranchPythonOperator(
        task_id="evaluate_training_trigger",
        python_callable=evaluate_training_trigger,
    )

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model_task,
        pool="global_serial_pool",
    )

    evaluate_task = BranchPythonOperator(
        task_id="evaluate_and_decide",
        python_callable=evaluate_model_task,
    )

    register_task = PythonOperator(
        task_id="register_model",
        python_callable=register_model_task,
    )

    skip_registration = EmptyOperator(task_id="skip_registration")
    skip_training = EmptyOperator(task_id="skip_training")

    end_task = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="none_failed_min_one_success",
    )

    # dependencies 
    check_trigger >> [wait_for_data, skip_wait]

    [wait_for_data, skip_wait] >> eda_task
    eda_task >> feature_eng_task >> drift_task >> training_trigger

    training_trigger >> [train_task, skip_training]
    train_task >> evaluate_task
    evaluate_task >> [register_task, skip_registration]

    [register_task, skip_registration, skip_training] >> end_task
