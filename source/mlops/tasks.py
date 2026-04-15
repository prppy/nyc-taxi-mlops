import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

import mlflow

from mlops import eda as eda_module
from mlops import feature_engineering as fe_module
from mlops import train as train_module
from mlops import drift_detector as drift_module
from mlops.evaluate import evaluate_model_task
from utils.config import MLOPS_START_MONTH, MLOPS_START_YEAR

logger = logging.getLogger(__name__)

REGISTERED_MODEL_NAME = "nyc-taxi-demand-forecaster"

def get_fe_window(**context):
    """
    Uses DAG logical date as the end anchor.
    Builds a rolling window ending at that month end.
    """
    ds = context["ds"]
    base_date = datetime.strptime(ds, "%Y-%m-%d")

    target_month = base_date - relativedelta(months=2)

    start_date = "2023-01-01"
    end_date = (target_month + relativedelta(day=31)).strftime("%Y-%m-%d")

    return start_date, end_date


def get_fe_window(**context):
    """
    Feature engineering window logic:
    - On the first MLOps run: backfill from 2023-01-01 up to target month end
    - On later runs: process only the target month
    """
    ds = context["ds"]
    logical_date = datetime.strptime(ds, "%Y-%m-%d")

    mlops_start_date = datetime(MLOPS_START_YEAR, MLOPS_START_MONTH, 1)
    target_month = logical_date - relativedelta(months=2)

    target_end = (target_month + relativedelta(day=31)).strftime("%Y-%m-%d")

    if logical_date.date() == mlops_start_date.date():
        return "2023-01-01", target_end

    target_start = target_month.replace(day=1).strftime("%Y-%m-%d")
    return target_start, target_end


def check_trigger_source(**context) -> str:
    dag_run = context.get("dag_run")
    trigger_conf = dag_run.conf if dag_run and dag_run.conf else {}
    trigger_source = trigger_conf.get("trigger_source", "scheduled")

    logger.info(f"Training DAG triggered by: {trigger_source}")

    if trigger_source == "drift_monitoring":
        logger.info("Drift-triggered run: skipping wait for DataOps")
        return "skip_wait"

    logger.info("Scheduled/manual run: waiting for DataOps")
    return "wait_for_data_pipeline"


def run_eda_task(**context):
    logger.info(f"Running EDA")
    return eda_module.main()


def run_feature_engineering_task(**context):
    _, end_date = get_fe_window(**context)
    logger.info(f"Running feature engineering for month of {end_date}")
    start_date = datetime.strptime(end_date, "%Y-%m-%d") - relativedelta(month=1) + relativedelta(day=1)
    start_date = datetime.strftime(start_date, "%Y-%m-%d")
    return fe_module.main(start_date, end_date)


def train_model_task(**context):
    result = train_module.main()

    ti = context["ti"]
    ti.xcom_push(key="best_run_id", value=result["best_run_id"])
    ti.xcom_push(key="best_model_name", value=result["best_model_name"])
    ti.xcom_push(key="best_metrics", value=result["best_metrics"])

    return result


def evaluate_model_task(**context) -> str:
    ti = context["ti"]

    best_run_id = ti.xcom_pull(task_ids="train_model", key="best_run_id")
    best_metrics = ti.xcom_pull(task_ids="train_model", key="best_metrics")
    best_model_name = ti.xcom_pull(task_ids="train_model", key="best_model_name")

    result = evaluate_model_task(best_run_id, best_metrics)

    if result["decision"] == "register_model":
        ti.xcom_push(key="approved_run_id", value=result["approved_run_id"])
        ti.xcom_push(key="approved_metrics", value=result["approved_metrics"])
        ti.xcom_push(key="approved_model_name", value=best_model_name)

    logger.info(result["reason"])
    return result["decision"]


def register_model_task(**context):
    ti = context["ti"]

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


def run_drift_detection_task(**context):
    report = drift_module.detect_drift()
    context["ti"].xcom_push(key="drift_report", value=report)
    return report


def save_drift_reports_task(**context):
    report = context["ti"].xcom_pull(task_ids="drift_detection", key="drift_report")
    if not report:
        logger.warning("No drift report found; skipping save")
        return

    drift_module.save_reports(report)
    logger.info("Drift reports saved")


def evaluate_drift_severity_task(**context) -> str:
    report = context["ti"].xcom_pull(task_ids="drift_detection", key="drift_report")
    if not report:
        logger.warning("No drift report found; skipping retraining")
        return "skip_retraining"

    return "trigger_retraining" if drift_module.should_alert(report) else "skip_retraining"


def send_drift_alert_task(**context):
    report = context["ti"].xcom_pull(task_ids="drift_detection", key="drift_report")
    if not report:
        logger.warning("No drift report found; skipping alert")
        return

    try:
        drift_module.send_drift_alert(report)
    except Exception as e:
        logger.error(f"Failed to send drift alert: {e}")