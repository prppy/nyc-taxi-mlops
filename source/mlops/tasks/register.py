import mlflow
import logging
from datetime import datetime

def register_model(**context):
    #Register approved model to MLflow Model Registry as 'Production'

    logger = logging.getLogger(__name__)
    ti = context['task_instance']

    # Pull approved model info
    approved_run_id = ti.xcom_pull(task_ids='evaluate_and_decide', key='approved_run_id')
    approved_model = ti.xcom_pull(task_ids='evaluate_and_decide', key='approved_model')
    approved_metrics = ti.xcom_pull(task_ids='evaluate_and_decide', key='approved_metrics')

    if not approved_run_id:
        logger.error("No approved run_id found in XCom")
        return

    logger.info(f"Registering model from run: {approved_run_id}")

    # MLflow Model Registry
    client = mlflow.tracking.MlflowClient()
    model_uri = f"runs:/{approved_run_id}/model"
    registered_model_name = "nyc-taxi-demand-forecaster"

    try:
        # Register model
        model_version = mlflow.register_model(
            model_uri=model_uri,
            name=registered_model_name
        )

        logger.info(f"Registered model version: {model_version.version}")

        # Transition to Production
        client.set_registered_model_alias(
            name=registered_model_name,
            alias="Production",
            version=str(model_version.version)
        )

        # Add metadata
        description = (
            f"Model: {approved_model}\n"
            f"Val RMSE: {approved_metrics['val_rmse']:.4f}\n"
            f"Test RMSE: {approved_metrics['test_rmse']:.4f}\n"
            f"Val MAE: {approved_metrics['val_mae']:.4f}\n"
            f"Trained: {datetime.now().isoformat()}"
        )

        client.update_model_version(
            name=registered_model_name,
            version=model_version.version,
            description=description
        )

        logger.info(f"Model promoted to Production: {registered_model_name} v{model_version.version}")

    except Exception as e:
        logger.error(f"Failed to register model: {e}")
        raise
