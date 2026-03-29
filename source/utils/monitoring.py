import os
import csv
import logging
import time
import functools
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)

LOG_CSV_PATH = "logs/task_run_log.csv"
CSV_FIELDS = ["run_id", "task", "status", "start_time", "duration_s", "error"]

def _append_csv(row: dict):
    os.makedirs(os.path.dirname(LOG_CSV_PATH), exist_ok=True)
    file_exists = os.path.exists(LOG_CSV_PATH)
    with open(LOG_CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def monitor(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        start_dt = datetime.utcnow()
        start = time.time()

        # pull run_id from Airflow context if available
        context = kwargs if kwargs else {}
        run_id = context.get("run_id", "local")

        logger.info(f"▶ Starting: {func.__name__}")
        try:
            result = func(*args, **kwargs)
            elapsed = round(time.time() - start, 1)
            logger.info(f"✔ Success: {func.__name__} ({elapsed}s)")
            _append_csv({
                "run_id": run_id,
                "task": func.__name__,
                "status": "SUCCESS",
                "start_time": start_dt.isoformat(),
                "duration_s": elapsed,
                "error": ""
            })
            return result
        except Exception as e:
            elapsed = round(time.time() - start, 1)
            logger.error(f"✘ Failed: {func.__name__} ({elapsed}s) — {e}", exc_info=True)
            _append_csv({
                "run_id": run_id,
                "task": func.__name__,
                "status": "FAILED",
                "start_time": start_dt.isoformat(),
                "duration_s": elapsed,
                "error": str(e)
            })
            raise
    return wrapper