from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import random
import os
import joblib
import pandas as pd

np.random.seed(42)
random.seed(42)

from source.utils.db import load_features
from utils.split import rolling_split
from utils.evaluate import evaluate_all
from utils.mlflow_utils import start_experiment, start_run, log_metrics, log_model_info

from models.linear_reg import train_linear, predict_linear
from models.xgboost import train_xgb, predict_xgb
from models.lstm import train_lstm, create_sequences

import mlflow
import mlflow.sklearn
import mlflow.tensorflow


# ======================
# HELPERS
# ======================
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


# ======================
# MAIN
# ======================
def main():

    print("\n" + "=" * 60)
    print("TRAINING PIPELINE")
    print("=" * 60)

    TEST_MONTH = get_target_month()
    print(f"Target month: {TEST_MONTH}")

    # MLflow experiment
    mlflow.set_experiment("taxi_demand_forecasting")

    df = load_features()

    train_df, val_df, test_df = rolling_split(df, TEST_MONTH)

    #  use TRAIN ONLY to avoid leakage
    features = get_feature_columns(train_df)

    X_train = train_df[features]
    y_train = train_df["target_demand"]

    X_val = val_df[features]
    y_val = val_df["target_demand"]

    X_test = test_df[features]
    y_test = test_df["target_demand"]

    results = {}
    trained_models = {}

    # ======================
    # LINEAR REGRESSION
    # ======================
    with mlflow.start_run(run_name="linear_regression"):

        mlflow.log_params({
            "model": "linear_regression",
            "num_features": len(features),
            "train_size": len(X_train)
        })

        linear_model = train_linear(X_train, y_train)
        pred = predict_linear(linear_model, X_val)

        metrics = evaluate_all(y_val, pred)
        mlflow.log_metrics(metrics)

        mlflow.sklearn.log_model(linear_model, "model")

        results["linear_regression"] = metrics
        trained_models["linear_regression"] = linear_model

        print("\nLinear Metrics:", metrics)

    # ======================
    # XGBOOST
    # ======================
    with mlflow.start_run(run_name="xgboost"):

        mlflow.log_params({
            "model": "xgboost",
            "num_features": len(features),
            "train_size": len(X_train)
        })

        xgb_model = train_xgb(X_train, y_train)
        pred = predict_xgb(xgb_model, X_val)

        metrics = evaluate_all(y_val, pred)
        mlflow.log_metrics(metrics)

        mlflow.sklearn.log_model(xgb_model, "model")

        results["xgboost"] = metrics
        trained_models["xgboost"] = xgb_model

        print("\nXGBoost Metrics:", metrics)

    # ======================
    # LSTM
    # ======================
    try:
        with mlflow.start_run(run_name="lstm"):

            mlflow.log_params({
                "model": "lstm",
                "num_features": len(features)
            })

            X_train_seq, y_train_seq = create_sequences(X_train.values, y_train.values)
            X_val_seq, y_val_seq = create_sequences(X_val.values, y_val.values)

            if len(X_train_seq) > 0 and len(X_val_seq) > 0:

                # use sequence data
                lstm_model = train_lstm(X_train_seq, y_train_seq)

                pred = lstm_model.predict(X_val_seq).flatten()
                y_val_seq = y_val_seq[:len(pred)]

                metrics = evaluate_all(y_val_seq, pred)
                mlflow.log_metrics(metrics)

                mlflow.tensorflow.log_model(lstm_model, "model")

                results["lstm"] = metrics
                trained_models["lstm"] = lstm_model

                print("\nLSTM Metrics:", metrics)

            else:
                print("Skipping LSTM — not enough sequence data")

    except Exception as e:
        print(f"LSTM failed: {e}")

    # ======================
    # SUMMARY
    # ======================
    print("\n" + "=" * 60)
    print("MODEL PERFORMANCE SUMMARY")
    print("=" * 60)

    for model, metrics in results.items():
        print(f"\n{model.upper()}")
        for k, v in metrics.items():
            print(f"{k}: {v:.4f}")

    # ======================
    # BEST MODEL SELECTION
    # ======================
    best_model = min(results, key=lambda x: results[x]["rmse"])
    best_rmse = results[best_model]["rmse"]

    print(f"\nBest model: {best_model} (RMSE: {best_rmse:.4f})")

    # ======================
    # RETRAIN ON TRAIN + VAL
    # ======================
    full_train = pd.concat([train_df, val_df])

    X_full = full_train[features]
    y_full = full_train["target_demand"]

    if best_model == "linear_regression":
        final_model = train_linear(X_full, y_full)

    elif best_model == "xgboost":
        final_model = train_xgb(X_full, y_full)

    else:
        final_model = trained_models[best_model]

    # ======================
    # FINAL TEST EVALUATION
    # ======================
    final_pred = final_model.predict(X_test)
    test_metrics = evaluate_all(y_test, final_pred)

    print("\nFINAL TEST PERFORMANCE")
    print(test_metrics)

    mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})

    # ======================
    # SAVE MODEL
    # ======================
    os.makedirs("models", exist_ok=True)

    save_path = f"models/{best_model}.pkl"
    joblib.dump(final_model, save_path)

    print(f"\nSaved best model locally: {save_path}")


if __name__ == "__main__":
    main()