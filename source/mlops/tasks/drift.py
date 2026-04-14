"""
Airflow task wrappers for drift detection.

Wraps existing drift_detector.py functions to work with Airflow context.
"""
import logging
import sys

# Add source to Python path
sys.path.insert(0, '/opt/airflow/source')

logger = logging.getLogger(__name__)


def run_drift_detection(**context):
    """
    Execute drift detection using existing drift_detector.detect_drift()

    Pushes drift report to XCom for downstream tasks.
    """
    from mlops.monitor.drift_detector import detect_drift

    ti = context['task_instance']

    logger.info("Starting drift detection...")
    try:
        report = detect_drift()

        # Log summary
        logger.info("Drift detection complete:")
        logger.info(f"  - Features analyzed: {len(report['featureStats'])}")
        logger.info(f"  - Critical drift features: {report['criticalCount']}")
        logger.info(f"  - High drift features: {report['highDriftCount']}")
        logger.info(f"  - Average drift score: {report['avgDriftScore']:.4f}")

        # Push to XCom for downstream tasks
        ti.xcom_push(key='drift_report', value=report)

    except Exception as e:
        logger.error(f"Drift detection failed: {e}")
        raise


def save_drift_reports_task(**context):
    """
    Save drift reports to filesystem using drift_detector.save_reports()

    Saves CSV reports and text summary to source/mlops/monitor/
    """
    from mlops.monitor.drift_detector import save_reports

    ti = context['task_instance']
    report = ti.xcom_pull(task_ids='drift_detection', key='drift_report')

    if not report:
        logger.warning("No drift report found in XCom, skipping save")
        return

    logger.info("Saving drift reports...")
    try:
        save_reports(report)
        logger.info("Drift reports saved to source/mlops/monitor/")
    except Exception as e:
        logger.error(f"Failed to save drift reports: {e}")
        raise


def evaluate_drift_severity(**context) -> str:
    """
    Determine if drift is severe enough to trigger retraining and alert.

    Returns:
        'trigger_retraining' if drift detected, 'skip_retraining' otherwise

    This is used as a BranchPythonOperator to decide the next path.
    """
    from mlops.monitor.drift_detector import should_alert

    ti = context['task_instance']
    report = ti.xcom_pull(task_ids='drift_detection', key='drift_report')

    if not report:
        logger.warning("No drift report found, skipping retraining")
        return 'skip_retraining'

    alert_needed = should_alert(report)

    if alert_needed:
        logger.warning("DRIFT ALERT TRIGGERED!")
        logger.warning(f"  - Critical features: {report['criticalCount']}")
        logger.warning(f"  - High drift features: {report['highDriftCount']}")
        logger.warning("  -> Triggering retraining pipeline")
        return 'trigger_retraining'
    else:
        logger.info("✓ No significant drift detected")
        logger.info(f"  - Critical features: {report['criticalCount']}")
        logger.info(f"  - High drift features: {report['highDriftCount']}")
        logger.info("  -> Skipping retraining")
        return 'skip_retraining'


def send_drift_alert_task(**context):
    """
    Send drift alert email using drift_detector.send_drift_alert()

    Sends HTML email with drift report and CSV attachments.
    Uses ALERT_EMAILS from environment variables.
    """
    from mlops.monitor.drift_detector import send_drift_alert

    ti = context['task_instance']
    report = ti.xcom_pull(task_ids='drift_detection', key='drift_report')

    if not report:
        logger.warning("No drift report found in XCom, skipping alert")
        return

    logger.info("Sending drift alert email...")
    try:
        send_drift_alert(report)
        logger.info("Drift alert email sent successfully")
    except Exception as e:
        logger.error(f"Failed to send drift alert: {e}")
        # Don't raise - we don't want email failure to block retraining
        logger.warning("Continuing despite email failure")
