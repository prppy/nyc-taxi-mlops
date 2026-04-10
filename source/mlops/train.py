from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import random
import os
import joblib

np.random.seed(42)
random.seed(42)

from utils.db import load_features
from utils.split import rolling_split
from utils.evaluate import evaluate_all
from utils.mlflow_utils import start_experiment, start_run, log_metrics, log_model_info

from models.linear_reg import train_linear, predict_linear
from models.xgboost import train_xgb, predict_xgb
from models.lstm import train_lstm, create_sequences

import mlflow
import mlflow.sklearn
import mlflow.tensorflow

def get_target_month():
    today = datetime.today()
    target = today - relativedelta(months=1)
    return target.strftime("%Y-%m")


def get_feature_columns(df):
    exclude_cols = {"target_demand", "hour_ts", "year_month"}

    numeric_cols = df.select_dtypes(include=["number", "bool"]).columns.tolist()
    feature_cols = [c for c in numeric_cols if c not in exclude_cols]

    if not feature_cols:
        raise ValueError("No valid numeric feature columns found")

    return feature_cols


def main():

    print("\n" + "=" * 60)
    print("TRAINING PIPELINE")
    print("=" * 60)

    TEST_MONTH = get_target_month()
    print(f"Target month: {TEST_MONTH}")

    start_experiment()

    df = load_features()

    train_df, val_df, test_df = rolling_split(df, TEST_MONTH)

    features = get_feature_columns(df)

    X_train = train_df[features]
    y_train = train_df["target_demand"]

    X_val = val_df[features]
    y_val = val_df["target_demand"]

    results = {}
    trained_models = {}

    # LINEAR REGRESSION
    with start_run("linear_regression"):

        log_model_info("linear_regression")

        linear_model = train_linear(X_train, y_train)
        pred = predict_linear(linear_model, X_val)

        metrics = evaluate_all(y_val, pred)
        log_metrics(metrics)

        mlflow.sklearn.log_model(linear_model, "model")

        results["linear_regression"] = metrics
        trained_models["linear_regression"] = linear_model

        print("\nLinear Metrics:", metrics)

    # XGBOOST
    with start_run("xgboost"):

        log_model_info("xgboost")

        xgb_model = train_xgb(X_train, y_train)
        pred = predict_xgb(xgb_model, X_val)

        metrics = evaluate_all(y_val, pred)
        log_metrics(metrics)

        mlflow.sklearn.log_model(xgb_model, "model")

        results["xgboost"] = metrics
        trained_models["xgboost"] = xgb_model

        print("\nXGBoost Metrics:", metrics)

    # LSTM
    try:
        with start_run("lstm"):

            log_model_info("lstm")

            X_train_seq, y_train_seq = create_sequences(X_train.values, y_train.values)
            X_val_seq, y_val_seq = create_sequences(X_val.values, y_val.values)

            if len(X_train_seq) > 0 and len(X_val_seq) > 0:

                lstm_model = train_lstm(X_train, y_train)

                pred = lstm_model.predict(X_val_seq).flatten()
                y_val_seq = y_val_seq[:len(pred)]

                metrics = evaluate_all(y_val_seq, pred)
                log_metrics(metrics)

                mlflow.tensorflow.log_model(lstm_model, "model")

                results["lstm"] = metrics
                trained_models["lstm"] = lstm_model

                print("\nLSTM Metrics:", metrics)

            else:
                print("Skipping LSTM — not enough sequence data")

    except Exception as e:
        print(f"LSTM failed: {e}")


    # FINAL RESULTS
    print("\n" + "=" * 60)
    print("MODEL PERFORMANCE SUMMARY")
    print("=" * 60)

    for model, metrics in results.items():
        print(f"\n{model.upper()}")
        for k, v in metrics.items():
            print(f"{k}: {v:.4f}")


    # BEST MODEL SELECTION
    best_model = min(results, key=lambda x: results[x]["rmse"])
    best_rmse = results[best_model]["rmse"]

    print(f"\nBest model: {best_model} (RMSE: {best_rmse:.4f})")

    # SAVE BEST MODEL LOCALLY
    os.makedirs("models", exist_ok=True)

    best_model_obj = trained_models[best_model]

    save_path = f"models/{best_model}.pkl"
    joblib.dump(best_model_obj, save_path)

    print(f"Saved best model locally: {save_path}")


if __name__ == "__main__":
    main()