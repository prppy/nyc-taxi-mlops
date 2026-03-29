import logging
from airflow.utils.email import send_email
import pandas as pd

LOG_CSV_PATH = "logs/task_run_log.csv"
ALERT_EMAIL = "myathetchai441@gmail.com"

logger = logging.getLogger(__name__)

def _build_email_body(context: dict, log_df: pd.DataFrame) -> str:
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    exec_date = context["execution_date"]
    log_url = context["task_instance"].log_url

    # last 20 rows of the run log as an HTML table
    recent = log_df.tail(20).to_html(index=False, border=1)

    return f"""
    <h2>Airflow Task Failure</h2>
    <table>
      <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
      <tr><td><b>Task</b></td><td>{task_id}</td></tr>
      <tr><td><b>Run ID</b></td><td>{run_id}</td></tr>
      <tr><td><b>Execution Date</b></td><td>{exec_date}</td></tr>
      <tr><td><b>Logs</b></td><td><a href="{log_url}">{log_url}</a></td></tr>
    </table>
    <br>
    <h3>Recent Task Run Log</h3>
    {recent}
    """

def on_failure_alert(context: dict):
    """Attach this as on_failure_callback on your DAG or tasks."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id

    try:
        log_df = pd.read_csv(LOG_CSV_PATH)
    except FileNotFoundError:
        log_df = pd.DataFrame(columns=["run_id", "task", "status", "start_time", "duration_s", "error"])

    subject = f"[Airflow] {dag_id}.{task_id} FAILED"
    body = _build_email_body(context, log_df)

    try:
        send_email(
            to=ALERT_EMAIL,
            subject=subject,
            html_content=body,
            files=[LOG_CSV_PATH]   # attaches the CSV directly
        )
        logger.info(f"Failure alert sent for {dag_id}.{task_id}")
    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")