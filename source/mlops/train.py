import os
import re
import shutil
from dotenv import load_dotenv

import mlflow
import mlflow.spark

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


def build_spark():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Training")
        .master("local[1]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_db_config():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in .env")

    match = re.match(r"postgresql://(.*):(.*)@(.*):(.*)/(.*)", database_url)
    if not match:
        raise ValueError("Invalid DATABASE_URL format")

    user, password, host, port, db = match.groups()

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    jdbc_props = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
    return jdbc_url, jdbc_props


def load_features(spark, jdbc_url, jdbc_props, start_date: str, end_date: str):
    """
    Loads pickup_features filtered to a 3-year sliding window.

    start_date / end_date: 'YYYY-MM-DD' strings.
    The Spark JDBC subquery approach lets us push the date filter down to Postgres
    so we never materialise the full historical table in memory.
    """
    query = f"""
    (
        SELECT *
        FROM pickup_features
        WHERE hour_ts >= '{start_date}'
          AND hour_ts <= '{end_date}'
    ) AS training_window
    """
    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        column="pulocationid",
        lowerBound=1,
        upperBound=300,
        numPartitions=4,
        properties=jdbc_props,
    )
    return df


def get_time_cutoffs(df):
    ts_df = df.select(F.col("hour_ts").cast("long").alias("hour_ts_long"))

    q70, q85 = ts_df.approxQuantile("hour_ts_long", [0.70, 0.85], 0.001)

    spark = df.sparkSession

    train_cutoff = spark.range(1).select(
        F.from_unixtime(F.lit(int(q70))).cast("timestamp").alias("ts")
    ).collect()[0]["ts"]

    val_cutoff = spark.range(1).select(
        F.from_unixtime(F.lit(int(q85))).cast("timestamp").alias("ts")
    ).collect()[0]["ts"]

    total_rows = df.count()
    return total_rows, train_cutoff, val_cutoff


def prepare_splits(df, train_cutoff, val_cutoff):
    train = df.filter(F.col("hour_ts") <= F.lit(train_cutoff))
    val = df.filter((F.col("hour_ts") > F.lit(train_cutoff)) & (F.col("hour_ts") <= F.lit(val_cutoff)))
    test = df.filter(F.col("hour_ts") > F.lit(val_cutoff))
    return train, val, test


def get_feature_columns(df):
    exclude = {"hour_ts", "target_demand"}
    return [c for c in df.columns if c not in exclude]


def assemble_features(train, val, test, feature_cols):
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )

    train_vec = assembler.transform(train).select("features", F.col("target_demand").alias("label"))
    val_vec = assembler.transform(val).select("features", F.col("target_demand").alias("label"))
    test_vec = assembler.transform(test).select("features", F.col("target_demand").alias("label"))

    return train_vec, val_vec, test_vec, assembler


def smape_spark(pred_df):
    df = pred_df.withColumn(
        "smape_term",
        2 * F.abs(F.col("prediction") - F.col("label")) /
        (F.abs(F.col("label")) + F.abs(F.col("prediction")) + F.lit(1e-8))
    )
    return df.agg(F.avg("smape_term").alias("smape")).collect()[0]["smape"]


def evaluate_predictions(pred_df):
    rmse_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")

    rmse = rmse_eval.evaluate(pred_df)
    mae = mae_eval.evaluate(pred_df)
    smape = smape_spark(pred_df)

    return rmse, mae, smape


def main(start_date: str = None, end_date: str = None):
    """
    start_date / end_date: 'YYYY-MM-DD' strings defining the 3-year training window.
    Passed in from train_model_task in tasks.py.
    If not provided (e.g. running train.py standalone), defaults to all available data.
    """
    print("\n=== STARTING PYSPARK TRAINING ===")

    spark = build_spark()
    try:
        jdbc_url, jdbc_props = get_db_config()

        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5003")
        mlflow.set_tracking_uri(tracking_uri)
        print("MLflow tracking URI:", mlflow.get_tracking_uri())
        mlflow.set_experiment("nyc_taxi_training_spark")

        # Fall back to full table if no window is specified (standalone runs)
        if not start_date or not end_date:
            from pyspark.sql.functions import min as spark_min, max as spark_max
            bounds_df = spark.read.jdbc(
                url=jdbc_url,
                table="pickup_features",
                properties=jdbc_props,
            ).agg(
                spark_min("hour_ts").alias("min_ts"),
                spark_max("hour_ts").alias("max_ts"),
            ).collect()[0]
            start_date = str(bounds_df["min_ts"])[:10]
            end_date = str(bounds_df["max_ts"])[:10]
            print(f"No window provided — using full table: {start_date} to {end_date}")
        else:
            print(f"Training window: {start_date} to {end_date}")

        print("\n=== LOADING DATA ===")
        df = load_features(spark, jdbc_url, jdbc_props, start_date, end_date)

        print("\n=== FINDING SPLIT CUT-OFFS ===")
        total_rows, train_cutoff, val_cutoff = get_time_cutoffs(df)
        print("Total rows:", total_rows)
        print("Train cutoff:", train_cutoff)
        print("Val cutoff:", val_cutoff)

        print("\n=== SPLITTING DATA ===")
        train, val, test = prepare_splits(df, train_cutoff, val_cutoff)

        # print("Train rows:", train.count())
        # print("Val rows:", val.count())
        # print("Test rows:", test.count())

        feature_cols = get_feature_columns(df)
        print("Number of feature columns:", len(feature_cols))

        print("\n=== ASSEMBLING FEATURES ===")
        train_vec, val_vec, test_vec, _ = assemble_features(train, val, test, feature_cols)

        # train_vec.cache()
        # val_vec.cache()
        # test_vec.cache()

        # print("Vectorized train rows:", train_vec.count())
        # print("Vectorized val rows:", val_vec.count())
        # print("Vectorized test rows:", test_vec.count())

        models = {
            "linear_regression": LinearRegression(
                featuresCol="features",
                labelCol="label",
                predictionCol="prediction",
                maxIter=20,
                regParam=0.1,
                elasticNetParam=0.0,
            ),
            "random_forest": RandomForestRegressor(
                featuresCol="features",
                labelCol="label",
                predictionCol="prediction",
                numTrees=40,
                maxDepth=7,
                seed=42,
            ),
            "gbt": GBTRegressor(
                featuresCol="features",
                labelCol="label",
                predictionCol="prediction",
                maxIter=30,
                maxDepth=4,
                stepSize=0.1,
                seed=42,
            )
        }

        results = {}

        for name, estimator in models.items():
            print(f"\n--- {name.upper()} ---")

            with mlflow.start_run(run_name=name) as run:
                model = estimator.fit(train_vec)

                val_pred = model.transform(val_vec)
                test_pred = model.transform(test_vec)

                val_rmse, val_mae, val_smape = evaluate_predictions(val_pred)
                test_rmse, test_mae, test_smape = evaluate_predictions(test_pred)

                print("Val RMSE:", val_rmse)
                print("Test RMSE:", test_rmse)
                print("Val MAE:", val_mae)
                print("Test MAE:", test_mae)
                print("Val SMAPE:", val_smape)
                print("Test SMAPE:", test_smape)

                mlflow.log_param("model", name)
                mlflow.log_param("feature_count", len(feature_cols))

                mlflow.log_metric("val_rmse", val_rmse)
                mlflow.log_metric("test_rmse", test_rmse)
                mlflow.log_metric("val_mae", val_mae)
                mlflow.log_metric("test_mae", test_mae)
                mlflow.log_metric("val_smape", val_smape)
                mlflow.log_metric("test_smape", test_smape)

                results[name] = {
                    "run_id": run.info.run_id,
                    "val_rmse": val_rmse,
                    "test_rmse": test_rmse,
                    "val_mae": val_mae,
                    "test_mae": test_mae,
                    "val_smape": val_smape,
                    "test_smape": test_smape,
                }

                # optional cleanup references
                del val_pred, test_pred, model
            
        print("\n=== FINAL COMPARISON ===")
        for k, v in results.items():
            print(
                k,
                "→ Val RMSE:", v["val_rmse"],
                "Test RMSE:", v["test_rmse"],
                "| Val MAE:", v["val_mae"],
                "Test MAE:", v["test_mae"],
                "| Val SMAPE:", v["val_smape"],
                "Test SMAPE:", v["test_smape"],
            )

        print("\n=== MODEL SELECTION ===")

        sorted_models = sorted(results.items(), key=lambda x: x[1]["val_rmse"])
        best_model_name, best_metrics = sorted_models[0]
        second_model_name, second_metrics = sorted_models[1]

        rmse_gap = second_metrics["val_rmse"] - best_metrics["val_rmse"]

        if rmse_gap < 1.0:
            if second_metrics["val_smape"] < best_metrics["val_smape"]:
                best_model_name = second_model_name
                best_metrics = second_metrics

        print("Best model selected:", best_model_name)

        best_estimator = models[best_model_name]

        print("\n=== RETRAINING BEST MODEL FOR ARTIFACT SAVE ===")
        best_model = best_estimator.fit(train_vec)

        with mlflow.start_run(run_name=f"{best_model_name}_final") as final_run:
            mlflow.log_param("model", best_model_name)
            mlflow.log_param("feature_count", len(feature_cols))
            mlflow.log_param("selected_as_best", True)

            mlflow.log_metric("val_rmse", best_metrics["val_rmse"])
            mlflow.log_metric("test_rmse", best_metrics["test_rmse"])
            mlflow.log_metric("val_mae", best_metrics["val_mae"])
            mlflow.log_metric("test_mae", best_metrics["test_mae"])
            mlflow.log_metric("val_smape", best_metrics["val_smape"])
            mlflow.log_metric("test_smape", best_metrics["test_smape"])

            mlflow.spark.log_model(best_model, artifact_path="model")

        print("\n=== SAVING FINAL MODEL ===")
        model_path = "/opt/airflow/source/mlops/final_model_spark"

        # remove existing folder first to avoid overwrite/path issues
        if os.path.exists(model_path):
            shutil.rmtree(model_path)

        best_model.write().overwrite().save(model_path)
        print(f"Best Spark model saved to {model_path}")
        output = {
            "best_run_id": final_run.info.run_id,
            "best_model_name": best_model_name,
            "best_metrics": best_metrics,
        }
        return output
    finally:
        spark.stop()


if __name__ == "__main__":
    main()