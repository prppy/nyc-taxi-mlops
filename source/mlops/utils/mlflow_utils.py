import mlflow


def start_experiment():
    mlflow.set_experiment("nyc_taxi_demand")


def start_run(run_name="training"):
    return mlflow.start_run(run_name=run_name)


def log_metrics(metrics: dict):
    for k, v in metrics.items():
        mlflow.log_metric(k, float(v))


def log_model_info(model_name: str):
    mlflow.log_param("model", model_name)