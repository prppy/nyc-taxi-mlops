import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)
REGISTERED_MODEL_NAME = "nyc-taxi-demand-forecaster"


def get_spark():
    print("STEP 1: entering get_spark()", flush=True)

    spark = (
        SparkSession.builder
        .appName("NYC Taxi Backend Inference")
        .getOrCreate()
    )

    print("STEP 2: Spark session created", flush=True)
    spark.sparkContext.setLogLevel("WARN")
    print("STEP 3: Spark log level set", flush=True)
    return spark


def load_prod_model():
    print("STEP 4: entering load_prod_model()", flush=True)
    client = MlflowClient()

    try:
        prod_version = client.get_model_version_by_alias(
            REGISTERED_MODEL_NAME,
            "Production",
        )
        print(f"STEP 5: found production alias version={prod_version.version}", flush=True)
    except Exception as e:
        print(f"ERROR in get_model_version_by_alias: {e}", flush=True)
        raise ValueError(
            f"Could not find Production alias for model '{REGISTERED_MODEL_NAME}': {e}"
        )

    model_uri = "models:/{}/{}".format(
        REGISTERED_MODEL_NAME,
        prod_version.version,
    )
    print(f"STEP 6: model_uri={model_uri}", flush=True)

    try:
        model = mlflow.spark.load_model(model_uri)
        print("STEP 7: model loaded successfully", flush=True)
    except Exception as e:
        print(f"ERROR in mlflow.spark.load_model: {e}", flush=True)
        raise ValueError(
            f"Could not load production model from MLflow URI '{model_uri}': {e}"
        )

    return model


def run_model_inference(feature_df):
    print("STEP 8: entering run_model_inference()", flush=True)

    if feature_df.empty:
        print("STEP 9: feature_df is empty", flush=True)
        return []

    print(f"STEP 10: feature_df shape={feature_df.shape}", flush=True)

    spark = get_spark()
    model = load_prod_model()

    df = feature_df.copy()
    print(f"STEP 11: copied dataframe shape={df.shape}", flush=True)

    zone_ids = df["pulocationid"].tolist()
    sources = df["source"].tolist() if "source" in df.columns else ["nearby"] * len(df)

    if "source" in df.columns:
        df = df.drop(columns=["source"])
        print("STEP 12: dropped source column", flush=True)

    print("STEP 13: creating spark dataframe", flush=True)
    spark_df = spark.createDataFrame(df)

    print("STEP 14: running model.transform()", flush=True)
    pred_df = model.transform(spark_df)

    print("STEP 15: converting predictions to pandas", flush=True)
    pred_pdf = pred_df.select("pulocationid", "prediction").toPandas()

    print(f"STEP 16: got {len(pred_pdf)} prediction rows", flush=True)

    source_by_zone = {}
    for zone_id, source in zip(zone_ids, sources):
        source_by_zone[zone_id] = source

    results = []
    for _, row in pred_pdf.iterrows():
        zone_id = int(row["pulocationid"])
        predicted_demand = float(row["prediction"])
        score = demand_to_score(predicted_demand)

        results.append({
            "zoneId": zone_id,
            "predictedDemand": round(predicted_demand, 2),
            "score": score,
            "source": source_by_zone.get(zone_id, "nearby"),
        })

    results = sorted(results, key=lambda x: x["score"], reverse=True)
    print(f"STEP 17: returning {len(results)} results", flush=True)
    return results

def demand_to_score(predicted_demand):
    raw = predicted_demand / 130
    return min(1, max(0, round(raw, 4)))