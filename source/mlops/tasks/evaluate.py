from mlflow.tracking import MlflowClient
import logging

PRODUCTION_MODEL = "gradient_boosting"

def evaluate_and_decide(**context):

    #evaluate the new model against the current production model using multiple metrics and guardrails. Decides whether to register or skip.
    logger = logging.getLogger(__name__)
    ti = context['task_instance']

    # 1. Get New Model Metrics 
    new_run_id = ti.xcom_pull(task_ids='train_model', key='run_id')
    new_metrics = ti.xcom_pull(task_ids='train_model', key='metrics')

    if not new_run_id or not new_metrics:
        logger.error(" No run_id or metrics found in XCom for new model.")
        return "skip_registration"

    client = MlflowClient()
    model_name = "nyc-taxi-demand-forecaster" # Ensure this matches your registration name

    # Fetch current production model metrics 
    try:
        # Ask MLflow for whatever is currently tagged 'Production'
        prod_version = client.get_model_version_by_alias(model_name, "Production")
        prev_run = client.get_run(prod_version.run_id)
        prev_metrics = prev_run.data.metrics

        logger.info("\n--- METRICS COMPARISON (New vs Production) ---")
        logger.info(f"RMSE:  {prev_metrics.get('val_rmse', 999):.4f}  vs  {new_metrics['val_rmse']:.4f}")
        logger.info(f"MAE:   {prev_metrics.get('val_mae', 999):.4f}  vs  {new_metrics['val_mae']:.4f}")
        logger.info(f"SMAPE: {prev_metrics.get('val_smape', 999):.4f}  vs  {new_metrics['val_smape']:.4f}")

        # RMSE must be better, or at least within a 1% margin of error
        rmse_passed = new_metrics['val_rmse'] <= (prev_metrics.get('val_rmse', 999) * 1.01)

        # MAE and SMAPE must not degrade by more than 5%
        # This prevents a model from sacrificing general accuracy for outlier accuracy
        mae_passed = new_metrics['val_mae'] <= (prev_metrics.get('val_mae', 999) * 1.05)
        smape_passed = new_metrics['val_smape'] <= (prev_metrics.get('val_smape', 999) * 1.05)

        if rmse_passed and mae_passed and smape_passed:
            logger.info("New model passed all metrics Guardrails. Promoting to Production.")
            ti.xcom_push(key='approved_run_id', value=new_run_id)
            ti.xcom_push(key='approved_model', value=PRODUCTION_MODEL)
            ti.xcom_push(key='approved_metrics', value=new_metrics)
            return "register_model"
        else:
            logger.warning("New model failed to beat Production model.")
            logger.warning(f"Passed RMSE? {rmse_passed} | Passed MAE? {mae_passed} | Passed SMAPE? {smape_passed}")
            return "skip_registration"

    except Exception as e:
        # If the DAG runs for the very first time, there is no previous model to compare against. In this case, we should promote the new model by default.
        logger.info("No existing Production model found in MLflow. Promoting New Model as the first Production Model.")
        ti.xcom_push(key='approved_run_id', value=new_run_id)
        ti.xcom_push(key='approved_model', value=PRODUCTION_MODEL)
        ti.xcom_push(key='approved_metrics', value=new_metrics)
        return "register_model"