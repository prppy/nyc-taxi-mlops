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
from mlops.tasks.train_tasks import run_feature_engineering, train_production_model
from mlops.tasks.evaluate import evaluate_and_decide
from mlops.tasks.register import register_model

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
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1), #change this for testing
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    description="Monthly retraining pipeline for demand forecasting",
    schedule_interval='@monthly',  # Triggered manually, by schedule, or by drift detector
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "training", "production"],
) as dag:

    # Wait for data pipeline to complete
    wait_for_data = ExternalTaskSensor(
        task_id="wait_for_data_pipeline",
        external_dag_id="taxi_data_pipeline",
        external_task_id="watermark_audit",  # Last task in data pipeline
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        timeout=7200,  # 2 hours
        poke_interval=300,  # Check every 5 minutes
    )

    # Run feature engineering
    feature_eng_task = PythonOperator(
        task_id="feature_engineering",
        python_callable=run_feature_engineering,
    )

    # Train production model
    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_production_model,
    )

    # Evaluate and decide whether to register
    evaluate_task = BranchPythonOperator(
        task_id="evaluate_and_decide",
        python_callable=evaluate_and_decide,
    )

    # Register model if approved
    register_task = PythonOperator(
        task_id="register_model",
        python_callable=register_model,
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
    wait_for_data >> feature_eng_task >> train_task >> evaluate_task #for monthly scheduled runs
    #feature_eng_task >> train_task >> evaluate_task  #for immediate test run
    evaluate_task >> [register_task, skip_task]
    [register_task, skip_task] >> end_task
