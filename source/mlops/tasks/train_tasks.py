import pandas as pd
import numpy as np
from sqlalchemy import create_engine
#from sklearn.linear_model import LinearRegression #for testing
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from dateutil.relativedelta import relativedelta
import mlflow
import mlflow.sklearn
import os
from mlops.feature_engineering import main as generate_features
import logging

logger = logging.getLogger(__name__)
PRODUCTION_MODEL = "gradient_boosting"
MLFLOW_EXPERIMENT = "nyc_taxi_demand_production"

def get_dates(**context):
    """Helper to calculate the window once for both tasks"""
    is_manual_run = context['dag_run'].external_trigger
    # Use context['ds'] for manual, or find Max DB date for scheduled
    if is_manual_run:
        base_date = pd.to_datetime("2024-01-01") #for testing with manual run on airflow ui
        #base_date = pd.to_datetime(context['ds'])
    else:
        engine = create_engine(os.getenv("DATABASE_URL"))
        max_date = pd.read_sql("SELECT MAX(hour_ts) FROM pickup_features", engine).iloc[0,0]
        base_date = pd.to_datetime(max_date) if max_date else pd.to_datetime(context['ds'])

    # End of the current month
    end_date = base_date + pd.offsets.MonthEnd(0)
    # Start of the 3-year window
    start_date = end_date - relativedelta(years=3) + pd.offsets.MonthBegin(1) 
    #start_date = end_date - relativedelta(months=1) + pd.offsets.MonthBegin(1) #for testing with 1 month window
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def run_feature_engineering(**context):
    #run feature engineering on latest data, creates pickup_features table

    start, end = get_dates(**context)
    logger.info(f"Running Feature Engineering for window: {start} to {end}")
    
    # We run it on the FULL 3 years to ensure lag features are accurate
    generate_features(start_date=start, end_date=end)

    logger.info("Feature engineering completed")


def train_production_model(**context):
    #Trains ONLY the best model and logs to MLflow.

    start_date, end_date = get_dates(**context)
    logger.info(f"Training Production Model for: {start_date} to {end_date}")

    # 1. Setup MLflow
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    # 2. Load Data
    engine = create_engine(os.getenv("DATABASE_URL"))
    query = f"SELECT * FROM pickup_features WHERE hour_ts >= '{start_date}' AND hour_ts <= '{end_date}'"
    df = pd.read_sql(query, engine)

    if df.empty:
        raise ValueError("CRITICAL: No data found for the specified window.")

    # This looks for columns that are entirely NULL
    killer_cols = [col for col in df.columns if df[col].isnull().sum() == len(df)]

    if killer_cols:
        logger.warning(f"Removing empty borough/feature columns: {killer_cols}")
        df = df.drop(columns=killer_cols)
    logger.info(f"Final training set: {len(df)} rows. Model fit starting...")

    # 3. Split Data (70/15/15)
    df = df.sort_values("hour_ts").reset_index(drop=True)
    n = len(df)
    train = df.iloc[:int(n * 0.7)]
    val = df.iloc[int(n * 0.7):int(n * 0.85)]
    test = df.iloc[int(n * 0.85):]

    exclude = ["hour_ts", "target_demand"]
    feature_cols = [c for c in df.columns if c not in exclude]

    X_train, y_train = train[feature_cols], train["target_demand"]
    X_val, y_val = val[feature_cols], val["target_demand"]
    X_test, y_test = test[feature_cols], test["target_demand"]

    # 4. Train specific Production Model (Gradient Boosting Regressor)
    #model = LinearRegression() #for testing
    model = GradientBoostingRegressor(n_estimators=50, max_depth=5, random_state=42)

    run_type = "manual" if context['dag_run'].external_trigger else "scheduled"
    
    with mlflow.start_run(run_name=f"prod_gbt_{pd.to_datetime(end_date).strftime('%Y-%m')}") as run:
        mlflow.set_tag("trigger_source", run_type)
        mlflow.set_tag("window_start", start_date)
        mlflow.set_tag("window_end", end_date)

        model.fit(X_train, y_train)
        
        val_pred = model.predict(X_val)
        test_pred = model.predict(X_test)
        
        val_rmse = np.sqrt(mean_squared_error(y_val, val_pred))
        test_rmse = np.sqrt(mean_squared_error(y_test, test_pred))
        val_mae = mean_absolute_error(y_val, val_pred)
        test_mae = mean_absolute_error(y_test, test_pred)
        # SMAPE (Symmetric Mean Absolute Percentage Error)
        val_smape = np.mean(
            2 * np.abs(val_pred - y_val) / (np.abs(y_val) + np.abs(val_pred) + 1e-8)
        )
        test_smape = np.mean(
            2 * np.abs(test_pred - y_test) / (np.abs(y_test) + np.abs(test_pred) + 1e-8)
        )

        mlflow.log_param("model_type", PRODUCTION_MODEL)
        mlflow.log_metrics({"val_rmse": val_rmse, "test_rmse": test_rmse, "val_mae": val_mae, "test_mae": test_mae, "val_smape": val_smape, "test_smape": test_smape})
        mlflow.sklearn.log_model(model, "model")

        run_id = run.info.run_id
        metrics = {"val_rmse": val_rmse, "test_rmse": test_rmse, "val_mae": val_mae, "test_mae": test_mae, "val_smape": val_smape, "test_smape": test_smape}

    # 5. Push to XCom for Evaluation Task
    context['ti'].xcom_push(key='run_id', value=run_id)
    context['ti'].xcom_push(key='metrics', value=metrics)
    logger.info(f"Training Complete. Run ID: {run_id}")