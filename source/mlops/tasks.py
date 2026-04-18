import logging
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

import mlflow
from sqlalchemy import text

from mlops import eda as eda_module
from mlops import feature_engineering as fe_module
from mlops import train as train_module
from mlops import drift_detector as drift_module
from mlops.evaluate import evaluate_model_task as _evaluate_model
from utils.config import MLOPS_START_MONTH, MLOPS_START_YEAR
from utils.db import engine as db_engine

logger = logging.getLogger(__name__)

REGISTERED_MODEL_NAME = "nyc-taxi-demand-forecaster"
MIN_HIGH_DRIFT_FEATURES_FOR_ALERT = 2  # matches drift_detector.py


def get_fe_window(**context): #fe window logic: if genesis run (march 2025), backfill all data from jan 2023; otherwise just run for the target month

    ds = context["ds"]
    logical_date = datetime.strptime(ds, "%Y-%m-%d")

    mlops_start_date = datetime(MLOPS_START_YEAR, MLOPS_START_MONTH, 1)
    target_month = logical_date - relativedelta(months=2)
    target_end = (target_month + relativedelta(day=31)).strftime("%Y-%m-%d")

    if logical_date.date() == mlops_start_date.date():
        # Genesis run: full backfill from Jan 2023 to current target month
        return "2023-01-01", target_end

    target_start = target_month.replace(day=1).strftime("%Y-%m-%d")
    return target_start, target_end


def check_trigger_source(**context) -> str: #waits for taxi_data_pipeline.watermark_audit to succeed
    #To skip the sensor for testing: trigger the DAG manually with conf: {"skip_sensor": true}
    
    dag_run = context.get("dag_run")
    trigger_conf = dag_run.conf if dag_run and dag_run.conf else {}

    if trigger_conf.get("skip_sensor", False):
        logger.info("skip_sensor=true — skipping DataOps wait (testing mode)")
        return "skip_wait"

    logger.info("Waiting for DataOps pipeline to complete")
    return "wait_for_data_pipeline"


def run_eda_task(**context):
    logger.info("Running EDA")
    return eda_module.main()


def run_feature_engineering_task(**context): #date-based logic to backfill all data for march 2025 (genesis run), then run monthly for target month only
    """
    Alternative (idempotent empty-table check): #checks if pickup_features is empty 
        _, end_date = get_fe_window(**context)
        with db_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(1) FROM pickup_features")).scalar()
        start_date = "2023-01-01" if count == 0 else (end_month - 1 month start)
        return fe_module.main(start_date, end_date)
    """
    start_date, end_date = get_fe_window(**context)
    logger.info(f"Running feature engineering: {start_date} to {end_date}")
    return fe_module.main(start_date, end_date)


def train_model_task(**context): #for early runs if 3 years of data don't exist, the FE module will just return all available data; no error, just a shorter window
    #trains on an explicit 3-year sliding window ending at the target data month
    """
    window: (target_month - 3 years) to target_month_end
    eg: execution March 2025 → data Jan 2025 → window Jan 2022 – Jan 2025 (i.e. jan 2023 - jan 2025)
             execution March 2026 → data Jan 2026 → window Jan 2023 – Jan 2026
    """
    ti = context["ti"]
    ds = context["ds"]

    # Target data month (2-month lag)
    target_month = datetime.strptime(ds, "%Y-%m-%d") - relativedelta(months=2)
    end_date = (target_month + relativedelta(day=31)).strftime("%Y-%m-%d")
    start_date = (target_month - relativedelta(years=3)).replace(day=1).strftime("%Y-%m-%d")

    logger.info(f"Training on 3-year sliding window: {start_date} to {end_date}")

    result = train_module.main(start_date=start_date, end_date=end_date)

    ti.xcom_push(key="best_run_id", value=result["best_run_id"])
    ti.xcom_push(key="best_model_name", value=result["best_model_name"])
    ti.xcom_push(key="best_metrics", value=result["best_metrics"])

    return result


def evaluate_model_task(**context) -> str:
    ti = context["ti"]

    best_run_id = ti.xcom_pull(task_ids="train_model", key="best_run_id")
    best_metrics = ti.xcom_pull(task_ids="train_model", key="best_metrics")
    best_model_name = ti.xcom_pull(task_ids="train_model", key="best_model_name")

    result = _evaluate_model(best_run_id, best_metrics)

    if result["decision"] == "register_model":
        ti.xcom_push(key="approved_run_id", value=result["approved_run_id"])
        ti.xcom_push(key="approved_metrics", value=result["approved_metrics"])
        ti.xcom_push(key="approved_model_name", value=best_model_name)

    logger.info(result["reason"])
    return result["decision"]


def register_model_task(**context):
    ti = context["ti"]
    ds = context["ds"]

    approved_run_id = ti.xcom_pull(task_ids="evaluate_and_decide", key="approved_run_id")
    approved_metrics = ti.xcom_pull(task_ids="evaluate_and_decide", key="approved_metrics")
    approved_model_name = ti.xcom_pull(task_ids="evaluate_and_decide", key="approved_model_name")

    if not approved_run_id:
        raise ValueError("No approved run ID found for registration")

    client = mlflow.tracking.MlflowClient()
    model_uri = f"runs:/{approved_run_id}/model"

    model_version = mlflow.register_model(
        model_uri=model_uri,
        name=REGISTERED_MODEL_NAME
    )

    client.set_registered_model_alias(
        name=REGISTERED_MODEL_NAME,
        alias="Production",
        version=str(model_version.version)
    )

    description = (
        f"Model: {approved_model_name}\n"
        f"Val RMSE: {approved_metrics['val_rmse']:.4f}\n"
        f"Test RMSE: {approved_metrics['test_rmse']:.4f}\n"
        f"Val MAE: {approved_metrics['val_mae']:.4f}\n"
        f"Val SMAPE: {approved_metrics['val_smape']:.4f}\n"
        f"Registered: {datetime.now().isoformat()}"
    )

    client.update_model_version(
        name=REGISTERED_MODEL_NAME,
        version=model_version.version,
        description=description
    )

    logger.info(f"Registered model version {model_version.version} as Production")

    # Mark training_triggered = TRUE in drift summary for this data month
    data_month = (datetime.strptime(ds, "%Y-%m-%d") - relativedelta(months=2)).replace(day=1).date()
    try:
        with db_engine.begin() as conn:
            conn.execute(text("""
                UPDATE drift_run_summary
                SET training_triggered = TRUE
                WHERE data_month = :m
            """), {"m": data_month})
        logger.info(f"Marked training_triggered=TRUE for data_month={data_month}")
    except Exception as e:
        logger.warning(f"Could not update drift_run_summary (table may not exist yet): {e}")


def calculate_and_log_drift_task(**context): #runs monthly regardless of training trigger
    """
    Actions:
      1. Calls detect_drift()
      2. Saves CSV reports to source/mlops/monitor/
      3. Sends email alert if any drift threshold exceeded
      4. Writes results to drift_run_summary + drift_feature_stats tables
      5. Pushes overall_status + drift_report to XCom for downstream branching
    """
    ti = context["ti"]
    ds = context["ds"]

    data_month = (datetime.strptime(ds, "%Y-%m-%d") - relativedelta(months=2)).replace(day=1).date()
    execution_date = datetime.strptime(ds, "%Y-%m-%d").date()

    logger.info(f"Running full drift detection for data_month={data_month}")

    # Run full drift detection (feature + label + model)
    report = drift_module.detect_drift()

    # Save CSV reports
    drift_module.save_reports(report)

    # Send email alert if thresholds exceeded
    if drift_module.should_alert(report):
        try:
            drift_module.send_drift_alert(report)
        except Exception as e:
            logger.error(f"Failed to send drift alert email: {e}")

    feature_report = report
    label_report = report.get("labelDrift", {})
    model_report = report.get("modelDrift", {})

    # Derive overall status using drift_detector logic
    overall_status = drift_module.get_overall_status(report)
    logger.info(f"Overall drift status: {overall_status}")

    # Write to DB
    with db_engine.begin() as conn:
        # Upsert one row per data_month
        conn.execute(text("""
            INSERT INTO drift_run_summary (
                data_month, execution_date,
                avg_feature_drift, high_drift_count, critical_count,
                label_drift_score, label_severity, label_should_alert,
                model_rmse_ratio, model_severity, model_should_alert,
                overall_status
            ) VALUES (
                :data_month, :execution_date,
                :avg_feature_drift, :high_drift_count, :critical_count,
                :label_drift_score, :label_severity, :label_should_alert,
                :model_rmse_ratio, :model_severity, :model_should_alert,
                :overall_status
            )
            ON CONFLICT (data_month) DO UPDATE SET
                execution_date    = EXCLUDED.execution_date,
                avg_feature_drift = EXCLUDED.avg_feature_drift,
                high_drift_count  = EXCLUDED.high_drift_count,
                critical_count    = EXCLUDED.critical_count,
                label_drift_score = EXCLUDED.label_drift_score,
                label_severity    = EXCLUDED.label_severity,
                label_should_alert = EXCLUDED.label_should_alert,
                model_rmse_ratio  = EXCLUDED.model_rmse_ratio,
                model_severity    = EXCLUDED.model_severity,
                model_should_alert = EXCLUDED.model_should_alert,
                overall_status    = EXCLUDED.overall_status
        """), {
            "data_month": data_month,
            "execution_date": execution_date,
            "avg_feature_drift": feature_report["avgDriftScore"],
            "high_drift_count": feature_report["highDriftCount"],
            "critical_count": feature_report["criticalCount"],
            "label_drift_score": label_report.get("driftScore"),
            "label_severity": label_report.get("severity"),
            "label_should_alert": label_report.get("shouldAlert", False),
            "model_rmse_ratio": model_report.get("rmseDegradationRatio"),
            "model_severity": model_report.get("severity"),
            "model_should_alert": model_report.get("shouldAlert", False),
            "overall_status": overall_status,
        })

        run_id = conn.execute(text(
            "SELECT id FROM drift_run_summary WHERE data_month = :m"
        ), {"m": data_month}).scalar()

        # Replace feature-level rows for this month
        conn.execute(text(
            "DELETE FROM drift_feature_stats WHERE data_month = :m"
        ), {"m": data_month})

        for row in feature_report["featureStats"]:
            conn.execute(text("""
                INSERT INTO drift_feature_stats (
                    run_id, data_month, feature, feature_type,
                    reference_value, current_value, drift_score, severity
                ) VALUES (
                    :run_id, :data_month, :feature, :feature_type,
                    :reference_value, :current_value, :drift_score, :severity
                )
            """), {
                "run_id": run_id,
                "data_month": data_month,
                "feature": row["feature"],
                "feature_type": row["featureType"],
                "reference_value": row["referenceValue"],
                "current_value": row["currentValue"],
                "drift_score": row["driftScore"],
                "severity": row["severity"],
            })

    logger.info(f"Drift results saved to DB for data_month={data_month}")

    ti.xcom_push(key="drift_report", value=report)
    ti.xcom_push(key="overall_status", value=overall_status)


def evaluate_training_trigger(**context) -> str: 
    #trigger training if data month is Jan/July (scheduled) OR drift is High/Critical; otherwise skip training

    ds = context["ds"]
    ti = context["ti"]

    # Data month = execution date minus 2-month lag #dn to do this actl just run it on jan, july
    target_month = datetime.strptime(ds, "%Y-%m-%d") - relativedelta(months=2)
    is_scheduled_retrain = target_month.month in [1, 7]

    overall_status = ti.xcom_pull(task_ids="calculate_and_log_drift", key="overall_status")
    has_high_drift = overall_status in ["High", "Critical"]

    if is_scheduled_retrain:
        logger.info(
            f"Scheduled retrain: data month is {target_month.strftime('%B %Y')} (Jan/Jul retrain)"
        )
    if has_high_drift:
        logger.info(f"Drift-triggered retrain: overall_status={overall_status}")
    if not is_scheduled_retrain and not has_high_drift:
        logger.info(
            f"Skipping training: {target_month.strftime('%B %Y')} is not a retrain month "
            f"and drift status is {overall_status}"
        )

    return "train_model" if (is_scheduled_retrain or has_high_drift) else "skip_training"
