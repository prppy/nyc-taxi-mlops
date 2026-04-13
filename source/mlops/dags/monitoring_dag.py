"""
Simplified Drift Monitoring DAG

WORKFLOW:
1. Runs drift detection on live_features vs pickup_features (reference)
2. Saves drift reports to filesystem
3. Evaluates drift severity (branching decision)
4. If drift detected:
   - Triggers training DAG for retraining
   - Sends email alert
5. If no drift: skips retraining

TRIGGERED:
  - @daily schedule (automatic)
  - Manual trigger
  - Frontend/API trigger

IMPORTANT: This DAG is kept simple - it only does drift detection and triggering.
No health checks, no cooldown logic, just: drift → trigger training.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys

# Add source to Python path
sys.path.insert(0, '/opt/airflow/source')

from mlops.tasks.drift import (
    run_drift_detection,
    save_drift_reports_task,
    evaluate_drift_severity,
    send_drift_alert_task
)
from utils.alerting import on_failure_alert

# ============================================================================
# CONFIGURATION
# ============================================================================

default_args = {
    "owner": "mlops",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id="drift_monitoring",
    default_args=default_args,
    description="Detect data drift and trigger retraining when needed",
    schedule_interval='@daily',  # Run once per day
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "monitoring", "drift"],
) as dag:

    # Start task
    start = EmptyOperator(
        task_id="start",
    )

    # Run drift detection
    drift_detection = PythonOperator(
        task_id="drift_detection",
        python_callable=run_drift_detection,
    )

    # Save drift reports to filesystem
    save_reports = PythonOperator(
        task_id="save_reports",
        python_callable=save_drift_reports_task,
    )

    # Evaluate drift severity and decide on retraining
    evaluate_severity = BranchPythonOperator(
        task_id="evaluate_severity",
        python_callable=evaluate_drift_severity,
    )

    # Trigger training DAG if drift detected
    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="ml_training_pipeline",
        conf={
            "trigger_source": "drift_monitoring",
            "trigger_reason": "Data drift detected - check drift_summary.txt for details",
            "drift_report_path": "data/monitor/reports/drift_summary.txt",
        },
        wait_for_completion=False,  # Don't block this DAG
        reset_dag_run=True,  # Allow immediate re-trigger
    )

    # Send drift alert email
    send_alert = PythonOperator(
        task_id="send_alert",
        python_callable=send_drift_alert_task,
    )

    # Skip retraining if no drift
    skip_retraining = EmptyOperator(
        task_id="skip_retraining",
    )

    # End task (convergence point)
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",  # Run even if one branch skipped
    )

    # ========================================================================
    # DAG DEPENDENCIES
    # ========================================================================

    # Linear flow: start → drift detection → save reports → evaluate
    start >> drift_detection >> save_reports >> evaluate_severity

    # Branching: if drift detected → trigger + alert, else skip
    evaluate_severity >> trigger_retraining
    evaluate_severity >> skip_retraining

    # Trigger and alert run in parallel when drift detected
    trigger_retraining >> send_alert

    # Both paths converge to end
    [send_alert, skip_retraining] >> end
