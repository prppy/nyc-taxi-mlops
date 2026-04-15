import logging

from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)

REGISTERED_MODEL_NAME = "nyc-taxi-demand-forecaster"


def evaluate_model_task(new_run_id, new_metrics):
    """
    Compares the newly selected best model against the current Production model.

    Decision rule:
    - val_rmse must be at most 1% worse than current production
    - val_mae and val_smape must not be more than 5% worse
    - if no Production model exists yet, approve automatically
    """
    if not new_run_id or not new_metrics:
        return {
            "decision": "skip_registration",
            "reason": "Missing new run ID or metrics",
        }

    client = MlflowClient()

    try:
        prod_version = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, "Production")
        prev_run = client.get_run(prod_version.run_id)
        prev_metrics = prev_run.data.metrics

        rmse_passed = new_metrics["val_rmse"] <= (prev_metrics.get("val_rmse", 999999) * 1.01)
        mae_passed = new_metrics["val_mae"] <= (prev_metrics.get("val_mae", 999999) * 1.05)
        smape_passed = new_metrics["val_smape"] <= (prev_metrics.get("val_smape", 999999) * 1.05)

        approved = rmse_passed and mae_passed and smape_passed

        return {
            "decision": "register_model" if approved else "skip_registration",
            "approved": approved,
            "reason": (
                "Passed comparison against Production model"
                if approved else
                f"Failed guardrails: rmse={rmse_passed}, mae={mae_passed}, smape={smape_passed}"
            ),
            "approved_run_id": new_run_id if approved else None,
            "approved_metrics": new_metrics if approved else None,
            "previous_run_id": prod_version.run_id,
            "previous_metrics": prev_metrics,
        }

    except Exception as e:
        logger.info(f"No existing Production model found or lookup failed: {e}")
        return {
            "decision": "register_model",
            "approved": True,
            "reason": "No Production model exists yet; approve first model",
            "approved_run_id": new_run_id,
            "approved_metrics": new_metrics,
            "previous_run_id": None,
            "previous_metrics": None,
        }