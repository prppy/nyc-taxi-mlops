import logging
import time
import functools

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)

def monitor(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        logger.info(f"▶ Starting: {func.__name__}")
        start = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start
            logger.info(f"✔ Success: {func.__name__} ({elapsed:.1f}s)")
            return result
        except Exception as e:
            elapsed = time.time() - start
            logger.error(
                f"✘ Failed: {func.__name__} ({elapsed:.1f}s) — {e}",
                exc_info=True   # prints full traceback
            )
            raise  # important: re-raise so Airflow marks task as failed
    return wrapper