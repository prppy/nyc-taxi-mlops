import mlflow
from mlflow.tracking import MlflowClient
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)
REGISTERED_MODEL_NAME = "nyc-taxi-demand-forecaster"

FEATURE_COLUMNS = [
    "pulocationid",
    "hour",
    "day_of_week",
    "month",
    "is_weekend",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "borough_manhattan",
    "borough_brooklyn",
    "borough_queens",
    "borough_bronx",
    "borough_staten island",
    "borough_ewr",
    "borough_nan",
    "service_zone_Yellow_Zone",
    "service_zone_Boro_Zone",
    "service_zone_Airports",
    "service_zone_EWR",
    "service_zone_nan",
    "temperature_mean",
    "precipitation_sum",
    "wind_speed_max",
    "is_rainy",
    "is_heavy_rain",
    "is_hot",
    "demand_lag_1",
    "demand_lag_2",
    "demand_lag_24",
    "rolling_mean_3h",
]

DEMAND_SCORE_P20 = 23.0
DEMAND_SCORE_P80 = 196.0


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

    model_uri = f"models:/{REGISTERED_MODEL_NAME}/{prod_version.version}"
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


def demand_to_score(predicted_demand: float, p10: float, p90: float) -> float:
    if p90 <= p10:
        return 0.0

    normalized = (predicted_demand - p10) / (p90 - p10)
    clipped = min(1.0, max(0.0, normalized))
    return round(clipped, 4)


def run_model_inference(feature_df):
    print("STEP 8: entering run_model_inference()", flush=True)

    if feature_df.empty:
        print("STEP 9: feature_df is empty", flush=True)
        return []

    print(f"STEP 10: feature_df shape={feature_df.shape}", flush=True)

    spark = get_spark()
    model = load_prod_model()

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="keep",
    )

    df = feature_df.copy()
    print(f"STEP 11: copied dataframe shape={df.shape}", flush=True)

    zone_ids = df["pulocationid"].tolist()
    sources = df["source"].tolist() if "source" in df.columns else ["nearby"] * len(df)

    if "source" in df.columns:
        df = df.drop(columns=["source"])
        print("STEP 12: dropped source column", flush=True)

    missing_cols = [col for col in FEATURE_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required feature columns for inference: {missing_cols}")

    print("STEP 13: creating spark dataframe", flush=True)
    spark_df = spark.createDataFrame(df)

    print("STEP 14: assembling features", flush=True)
    model_input = assembler.transform(spark_df)

    print("STEP 15: running model.transform()", flush=True)
    pred_df = model.transform(model_input)

    print("STEP 16: converting predictions to pandas", flush=True)
    pred_pdf = pred_df.select("pulocationid", "prediction").toPandas()

    print(f"STEP 17: got {len(pred_pdf)} prediction rows", flush=True)

    source_by_zone = dict(zip(zone_ids, sources))

    results = []
    for _, row in pred_pdf.iterrows():
        zone_id = int(row["pulocationid"])
        predicted_demand = float(row["prediction"])
        score = demand_to_score(
            predicted_demand,
            p10=DEMAND_SCORE_P20,
            p90=DEMAND_SCORE_P80,
        )

        results.append({
            "zoneId": zone_id,
            "predictedDemand": round(predicted_demand, 2),
            "score": score,
            "source": source_by_zone.get(zone_id, "nearby"),
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    print(f"STEP 18: returning {len(results)} results", flush=True)
    return results