import pandas as pd
from sqlalchemy import create_engine

from dotenv import load_dotenv
import os

from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor

import mlflow
import mlflow.sklearn
import joblib
import numpy as np

## !! RUN OUTSIDE OF DOCKER. RUN IN LOCAL !!

def main():
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL2")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in .env")

    mlflow.set_experiment("nyc_taxi_training")

    print("\n=== LOADING DATA ===")

    engine = create_engine(DATABASE_URL)
    df = pd.read_sql("SELECT * FROM pickup_features", engine)
    print("Data shape:", df.shape)

    # Time-based split (70% train, 15% val, 15% test)
    print("\n=== SPLITTING DATA ===")
    df = df.sort_values("hour_ts").reset_index(drop=True)
    n = len(df)

    train_end = int(n * 0.7)
    val_end = int(n * 0.85)

    train = df.iloc[:train_end]
    val = df.iloc[train_end:val_end]
    test = df.iloc[val_end:]

    print("Train:", train.shape)
    print("Val:", val.shape)
    print("Test:", test.shape)

    # Features and target
    exclude = ["hour_ts", "target_demand"]
    feature_cols = [c for c in df.columns if c not in exclude]

    X_train = train[feature_cols]
    y_train = train["target_demand"]

    X_val = val[feature_cols]
    y_val = val["target_demand"]

    X_test = test[feature_cols]
    y_test = test["target_demand"]


    # Models
    models = {
        "linear_regression": LinearRegression(),
        "random_forest": RandomForestRegressor(n_estimators=50, max_depth=8, random_state=42),
        "gbt": GradientBoostingRegressor(n_estimators=50, max_depth=5)
    }

    # Model Training Loop
    results = {}

    for name, model in models.items():

        print(f"\n--- {name.upper()} ---")

        # Start MLflow run
        with mlflow.start_run(run_name=name):

            model.fit(X_train, y_train)

            val_pred = model.predict(X_val)
            test_pred = model.predict(X_test)

            # === METRICS ===

            # RMSE
            val_rmse = np.sqrt(mean_squared_error(y_val, val_pred))
            test_rmse = np.sqrt(mean_squared_error(y_test, test_pred))

            # MAE
            val_mae = mean_absolute_error(y_val, val_pred)
            test_mae = mean_absolute_error(y_test, test_pred)

            # SMAPE
            val_smape = np.mean(
                2 * np.abs(val_pred - y_val) / (np.abs(y_val) + np.abs(val_pred) + 1e-8)
            )
            test_smape = np.mean(
                2 * np.abs(test_pred - y_test) / (np.abs(y_test) + np.abs(test_pred) + 1e-8)
            )

            print("Val RMSE:", val_rmse)
            print("Test RMSE:", test_rmse)
            print("Val MAE:", val_mae)
            print("Test MAE:", test_mae)
            print("Val SMAPE:", val_smape)
            print("Test SMAPE:", test_smape)

            # Log to MLflow
            mlflow.log_param("model", name)

            mlflow.log_metric("val_rmse", val_rmse)
            mlflow.log_metric("test_rmse", test_rmse)

            mlflow.log_metric("val_mae", val_mae)
            mlflow.log_metric("test_mae", test_mae)

            mlflow.log_metric("val_smape", val_smape)
            mlflow.log_metric("test_smape", test_smape)

            mlflow.sklearn.log_model(model, "model")

            results[name] = {
                "val_rmse": val_rmse,
                "test_rmse": test_rmse,
                "val_mae": val_mae,
                "test_mae": test_mae,
                "val_smape": val_smape,
                "test_smape": test_smape,
                "model_obj": model
            }

    # Final Results
    print("\n=== FINAL COMPARISON ===")

    for k, v in results.items():
        print(
            k,
            "→ Val RMSE:", v["val_rmse"],
            "Test RMSE:", v["test_rmse"],
            "| Val MAE:", v["val_mae"],
            "Test MAE:", v["test_mae"],
            "| Val SMAPE:", v["val_smape"],
            "Test SMAPE:", v["test_smape"]
        )

    # Model Selection based on Val RMSE
    print("\n=== MODEL SELECTION ===")

    baseline_rmse = results["linear_regression"]["val_rmse"]

    best_model_name = "linear_regression"
    best_rmse = baseline_rmse

    for name, metrics in results.items():
        if metrics["val_rmse"] < best_rmse:
            best_model_name = name
            best_rmse = metrics["val_rmse"]

    print(f"Best model based on RMSE: {best_model_name}")

    with open("model_name.txt", "w") as f:
        f.write(best_model_name)
    print("Best model name saved to model_name.txt")

    # Acceptance Logic
    print("\n=== ACCEPTANCE CHECK ===")

    mlflow.set_tag("best_model", best_model_name)
    if best_model_name == "linear_regression":
        print("No better model found → Using baseline (Linear Regression)")
        final_model = results["linear_regression"]["model_obj"]
    else:
        print(f"Better model found → Using {best_model_name}")
        final_model = results[best_model_name]["model_obj"]

    # Save Final Model
    joblib.dump(final_model, "final_model.pkl")
    print("\nFinal model saved as final_model.pkl")

if __name__ == "__main__":
    main()