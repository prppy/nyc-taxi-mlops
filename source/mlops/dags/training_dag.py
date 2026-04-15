"""
WORKFLOW:
1. Waits for data pipeline to complete (new month's data available)
2. Runs feature engineering on the new data
3. Trains the production model 
4. Evaluates model performance
5. Registers model to MLflow if it meets acceptance criteria

TRIGGERED:
  - @monthly schedule (automatic after data pipeline)
  - Manual trigger
  - Drift detector trigger
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
    SCHEDULE_INTERVAL
)



# Add source to Python path
sys.path.insert(0, '/opt/airflow/source')

# ============================================================================
# CONFIGURATION
# ============================================================================

# Model acceptance criteria
# ACCEPTANCE_CRITERIA = {
#     "max_val_rmse": 10.0,
#     "max_test_rmse": 10.0,
#     "min_improvement": 0.0     # Must be at least as good as baseline
# }

default_args = {
    "owner": "mlops",
    "start_date": datetime(MLOPS_START_YEAR, MLOPS_START_MONTH, 1),
    "retries": RETRY_COUNT,
    "retry_delay": timedelta(minutes=5), # change this for testing
    "on_failure_callback": on_failure_alert,
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id=TRAIN_DAG_ID,
    default_args=default_args,
    description="Monthly retraining pipeline for demand forecasting",
    schedule_interval=SCHEDULE_INTERVAL,  # Triggered manually, by schedule, or by drift detector
    catchup=False, # TODO: change this to True
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["mlops", "training"],
) as dag:

    # Check trigger source to decide if we need to wait for data pipeline
    check_trigger = BranchPythonOperator(
        task_id="check_trigger_source",
        python_callable=check_trigger_source,
    )

    # Wait for data pipeline to complete (only for scheduled runs)
    wait_for_data = ExternalTaskSensor(
        task_id="wait_for_data_pipeline",
        external_dag_id=DATAOPS_DAG_ID,
        external_task_id="watermark_audit",  # Last task in data pipeline
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        timeout=7200,  # 2 hours
        poke_interval=300,  # Check every 5 minutes
    )

    # Skip waiting (for drift-triggered runs)
    skip_wait = EmptyOperator(
        task_id="skip_wait",
    )

    eda_task = PythonOperator(
        task_id="run_eda",
        python_callable=run_eda_task,
        trigger_rule="none_failed_min_one_success",
    )
    
    # Run feature engineering (convergence point from both branches)
    feature_eng_task = PythonOperator(
        task_id="feature_engineering",
        python_callable=run_feature_engineering_task,
        trigger_rule="none_failed_min_one_success",  # Run if either branch succeeds
        pool="global_serial_pool",
    )

    # Train production model
    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model_task,
        pool="global_serial_pool",
    )

    # Evaluate and decide whether to register
    evaluate_task = BranchPythonOperator(
        task_id="evaluate_and_decide",
        python_callable=evaluate_model_task,
    )

    # Register model if approved
    register_task = PythonOperator(
        task_id="register_model",
        python_callable=register_model_task,
    )

    # Skip registration if not approved
    skip_task = EmptyOperator(
        task_id="skip_registration",
    )

    # End task (convergence point)
    end_task = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="none_failed_min_one_success",
    )

    # Define pipeline
    # Branch based on trigger source
    check_trigger >> [wait_for_data, skip_wait]

    # Both branches converge to eda
    [wait_for_data, skip_wait] >> eda_task
    eda_task >> feature_eng_task >> train_task >> evaluate_task

    # Rest of pipeline (same as before)
    eda_task >> feature_eng_task >> train_task >> evaluate_task
    evaluate_task >> [register_task, skip_task]
    [register_task, skip_task] >> end_task
