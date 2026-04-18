import os
import re
import logging
from dotenv import load_dotenv

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, expr
from pyspark.sql.types import NumericType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import PipelineModel
from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel, GBTRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator

from airflow.utils.email import send_email

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

FEATURE_TABLE = "pickup_features"
REPORT_PATH = "source/mlops/monitor"
MODEL_PATH = "final_model_spark"

BASELINE_RMSE = 40.0 # actual best validation/test RMSE from MLflow
MODEL_DRIFT_THRESHOLD_RATIO = 0.30  # alert if live RMSE is 30% worse than baseline

# continuous / numeric features:
# drift = relative mean shift
NUMERIC_FEATURES = [
    "target_demand",
    "hour",
    "day_of_week",
    "is_weekend",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
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

# one-hot categorical features:
# drift = relative proportion shift
ONE_HOT_CATEGORICAL_FEATURES = [
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
]

EXCLUDED_FEATURES = [
    "pulocationid",
    "month",
]

ALL_MONITORED_FEATURES = NUMERIC_FEATURES + ONE_HOT_CATEGORICAL_FEATURES

CRITICAL_THRESHOLD = 0.80
HIGH_THRESHOLD = 0.55
MEDIUM_THRESHOLD = 0.30

MIN_HIGH_DRIFT_FEATURES_FOR_ALERT = 2
LABEL_DRIFT_THRESHOLD = 0.30


def get_spark():
    spark = (
        SparkSession.builder
        .appName("Drift Detector")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_db_config():
    load_dotenv(override=True)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in .env")

    match = re.match(r"postgresql://(.*):(.*)@(.*):(.*)/(.*)", database_url)
    if not match:
        raise ValueError("Invalid DATABASE_URL format")

    user, password, host, port, db = match.groups()

    db_url = f"jdbc:postgresql://{host}:{port}/{db}"
    db_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }

    return {
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "db": db,
        "db_url": db_url,
        "db_properties": db_properties,
    }


def parse_alert_emails():
    raw = os.getenv("ALERT_EMAILS", "").strip()
    if not raw:
        return []

    raw = raw.strip("[]")
    emails = [e.strip().strip("'").strip('"') for e in raw.split(",")]
    return [e for e in emails if e]


def load_table(spark, db_url, db_properties, table_name):
    logger.info(f"Loading table: {table_name}")
    return spark.read.jdbc(
        url=db_url,
        table=table_name,
        properties=db_properties,
    )


def get_numeric_columns(df):
    return {
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
    }


def safe_mean(df, feature):
    value = df.select(avg(col(feature)).alias("mean")).collect()[0]["mean"]
    return float(value) if value is not None else 0.0


def safe_std(df, feature):
    value = df.select(stddev(col(feature)).alias("std")).collect()[0]["std"]
    return float(value) if value is not None else 0.0


def safe_percentile(df, feature, p):
    try:
        value = df.select(expr(f"percentile_approx({feature}, {p}) as p")).collect()[0]["p"]
        return float(value) if value is not None else 0.0
    except Exception:
        return 0.0


def compute_relative_shift(reference_value, current_value):
    if reference_value == 0:
        if current_value == 0:
            return 0.0
        return 1.0

    raw_shift = abs(current_value - reference_value) / abs(reference_value)
    return min(float(raw_shift), 1.0)


def drift_label(score):
    if score >= CRITICAL_THRESHOLD:
        return "Critical"
    if score >= HIGH_THRESHOLD:
        return "High"
    if score >= MEDIUM_THRESHOLD:
        return "Medium"
    return "Healthy"


def get_feature_type(feature):
    if feature in NUMERIC_FEATURES:
        return "numeric_mean_shift"
    if feature in ONE_HOT_CATEGORICAL_FEATURES:
        return "categorical_proportion_shift"
    return "unknown"


def smape_spark(pred_df):
    df = pred_df.withColumn(
        "smape_term",
        2 * abs(col("prediction") - col("label")) /
        (abs(col("label")) + abs(col("prediction")) + expr("1e-8"))
    )
    return float(df.agg(avg("smape_term").alias("smape")).collect()[0]["smape"])


def evaluate_predictions(pred_df):
    rmse_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")

    rmse = float(rmse_eval.evaluate(pred_df))
    mae = float(mae_eval.evaluate(pred_df))
    smape = smape_spark(pred_df)

    return rmse, mae, smape


def load_saved_model(model_path=MODEL_PATH):
    if not os.path.exists(model_path):
        logger.warning(f"Model path not found: {model_path}")
        return None

    loaders = [
        PipelineModel.load,
        LinearRegressionModel.load,
        RandomForestRegressionModel.load,
        GBTRegressionModel.load,
    ]

    for loader in loaders:
        try:
            model = loader(model_path)
            logger.info(f"Loaded model from {model_path} using {loader.__qualname__}")
            return model
        except Exception:
            continue

    logger.warning(f"Could not load model from {model_path}")
    return None


def detect_feature_drift(reference_df, live_df):
    reference_numeric = get_numeric_columns(reference_df)
    live_numeric = get_numeric_columns(live_df)

    feature_stats = []
    missing_features = []
    excluded_features = list(EXCLUDED_FEATURES)

    for feature in ALL_MONITORED_FEATURES:
        if feature not in reference_numeric or feature not in live_numeric:
            missing_features.append(feature)
            continue

        reference_mean = safe_mean(reference_df, feature)
        current_mean = safe_mean(live_df, feature)
        drift_score = compute_relative_shift(reference_mean, current_mean)

        feature_stats.append({
            "feature": feature,
            "featureType": get_feature_type(feature),
            "referenceValue": reference_mean,
            "currentValue": current_mean,
            "driftScore": drift_score,
            "severity": drift_label(drift_score),
        })

    feature_stats = sorted(
        feature_stats,
        key=lambda row: row["driftScore"],
        reverse=True
    )

    high_drift_count = sum(1 for row in feature_stats if row["driftScore"] >= HIGH_THRESHOLD)
    critical_count = sum(1 for row in feature_stats if row["driftScore"] >= CRITICAL_THRESHOLD)
    avg_drift_score = (
        sum(row["driftScore"] for row in feature_stats) / len(feature_stats)
        if feature_stats else 0.0
    )

    return {
        "featureStats": feature_stats,
        "missingFeatures": missing_features,
        "excludedFeatures": excluded_features,
        "highDriftCount": high_drift_count,
        "criticalCount": critical_count,
        "avgDriftScore": avg_drift_score,
    }


def detect_label_drift(reference_df, live_df, label_col="target_demand"):
    if label_col not in reference_df.columns or label_col not in live_df.columns:
        return {
            "available": False,
            "labelCol": label_col,
            "reason": f"{label_col} missing from one or both tables"
        }

    reference_mean = safe_mean(reference_df, label_col)
    live_mean = safe_mean(live_df, label_col)

    reference_std = safe_std(reference_df, label_col)
    live_std = safe_std(live_df, label_col)

    reference_p50 = safe_percentile(reference_df, label_col, 0.5)
    live_p50 = safe_percentile(live_df, label_col, 0.5)

    reference_p90 = safe_percentile(reference_df, label_col, 0.9)
    live_p90 = safe_percentile(live_df, label_col, 0.9)

    drift_score = compute_relative_shift(reference_mean, live_mean)

    return {
        "available": True,
        "labelCol": label_col,
        "referenceMean": reference_mean,
        "liveMean": live_mean,
        "referenceStd": reference_std,
        "liveStd": live_std,
        "referenceP50": reference_p50,
        "liveP50": live_p50,
        "referenceP90": reference_p90,
        "liveP90": live_p90,
        "driftScore": drift_score,
        "severity": drift_label(drift_score),
        "shouldAlert": drift_score >= LABEL_DRIFT_THRESHOLD,
    }


def get_model_feature_columns(df):
    exclude = {"hour_ts", "target_demand"}
    return [c for c in df.columns if c not in exclude]


def detect_model_drift(live_df, model_path=MODEL_PATH, baseline_rmse=BASELINE_RMSE):
    if "target_demand" not in live_df.columns:
        return {
            "available": False,
            "reason": "target_demand missing from live table"
        }

    model = load_saved_model(model_path)
    if model is None:
        return {
            "available": False,
            "reason": f"Unable to load saved model from {model_path}"
        }

    feature_cols = get_model_feature_columns(live_df)
    missing_required = [c for c in feature_cols if c not in live_df.columns]
    if missing_required:
        return {
            "available": False,
            "reason": f"Missing required feature columns: {missing_required}"
        }

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )

    live_vec = assembler.transform(live_df).select(
        "features",
        col("target_demand").alias("label")
    )

    pred_df = model.transform(live_vec)

    rmse, mae, smape = evaluate_predictions(pred_df)
    rmse_ratio = ((rmse - baseline_rmse) / baseline_rmse) if baseline_rmse > 0 else 0.0

    return {
        "available": True,
        "baselineRmse": baseline_rmse,
        "liveRmse": rmse,
        "liveMae": mae,
        "liveSmape": smape,
        "rmseDegradationRatio": rmse_ratio,
        "severity": drift_label(min(max(rmse_ratio, 0.0), 1.0)),
        "shouldAlert": rmse_ratio >= MODEL_DRIFT_THRESHOLD_RATIO,
    }
    
def load_reference_and_current_data(spark, db_url, db_properties):
    latest_ts_query = f"""
    (
        SELECT MAX(hour_ts) AS max_hour_ts
        FROM {FEATURE_TABLE}
    ) AS latest_ts_subquery
    """

    latest_ts_df = spark.read.jdbc(
        url=db_url,
        table=latest_ts_query,
        properties=db_properties,
    )

    max_hour_ts = latest_ts_df.collect()[0]["max_hour_ts"]
    if max_hour_ts is None:
        raise ValueError("pickup_features is empty")

    latest_month_start = max_hour_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    reference_query = f"""
    (
        SELECT *
        FROM {FEATURE_TABLE}
        WHERE hour_ts < '{latest_month_start}'
    ) AS reference_subquery
    """

    live_query = f"""
    (
        SELECT *
        FROM {FEATURE_TABLE}
        WHERE hour_ts >= '{latest_month_start}'
    ) AS current_subquery
    """

    reference_df = spark.read.jdbc(
        url=db_url,
        table=reference_query,
        properties=db_properties,
    )

    live_df = spark.read.jdbc(
        url=db_url,
        table=live_query,
        properties=db_properties,
    )

    return reference_df, live_df, latest_month_start


def detect_drift():
    spark = get_spark()
    try:
        db_config = get_db_config()
        reference_df, live_df, latest_month = load_reference_and_current_data(
            spark,
            db_config["db_url"],
            db_config["db_properties"],
        )

        feature_report = detect_feature_drift(reference_df, live_df)
        label_report = detect_label_drift(reference_df, live_df, label_col="target_demand")
        model_report = detect_model_drift(live_df)

        return {
            "referenceTable": f"{FEATURE_TABLE} before {latest_month}",
            "liveTable": f"{FEATURE_TABLE} from {latest_month}",
            "featureStats": feature_report["featureStats"],
            "missingFeatures": feature_report["missingFeatures"],
            "excludedFeatures": feature_report["excludedFeatures"],
            "highDriftCount": feature_report["highDriftCount"],
            "criticalCount": feature_report["criticalCount"],
            "avgDriftScore": feature_report["avgDriftScore"],
            "labelDrift": label_report,
            "modelDrift": model_report,
        }
    finally:
        spark.stop()


def should_alert(report):
    feature_alert = (
        report["criticalCount"] > 0
        or report["highDriftCount"] >= MIN_HIGH_DRIFT_FEATURES_FOR_ALERT
    )

    label_alert = report.get("labelDrift", {}).get("shouldAlert", False)
    model_alert = report.get("modelDrift", {}).get("shouldAlert", False)

    return feature_alert or label_alert or model_alert


def get_overall_status(report) -> str:
    """
    Returns a single overall drift status label across all three drift types:
    feature drift, label drift, and model drift.

    Levels (in priority order): Critical → High → Medium → Low
    """
    feature_critical = report["criticalCount"] > 0
    label_severity = report.get("labelDrift", {}).get("severity", "Healthy")
    label_critical = label_severity == "Critical"

    if feature_critical or label_critical:
        return "Critical"

    feature_high = report["highDriftCount"] >= MIN_HIGH_DRIFT_FEATURES_FOR_ALERT
    label_alert = report.get("labelDrift", {}).get("shouldAlert", False)
    model_alert = report.get("modelDrift", {}).get("shouldAlert", False)

    if feature_high or label_alert or model_alert:
        return "High"

    avg_feature_drift = report.get("avgDriftScore", 0.0)
    label_drift_score = report.get("labelDrift", {}).get("driftScore", 0.0) or 0.0

    if avg_feature_drift >= MEDIUM_THRESHOLD or label_drift_score >= MEDIUM_THRESHOLD:
        return "Medium"

    return "Low"


def save_reports(report):
    os.makedirs(REPORT_PATH, exist_ok=True)

    pd.DataFrame(report["featureStats"]).to_csv(
        os.path.join(REPORT_PATH, "feature_drift_report.csv"),
        index=False
    )

    pd.DataFrame({"feature": report["missingFeatures"]}).to_csv(
        os.path.join(REPORT_PATH, "missing_feature_report.csv"),
        index=False
    )

    pd.DataFrame({"feature": report["excludedFeatures"]}).to_csv(
        os.path.join(REPORT_PATH, "excluded_feature_report.csv"),
        index=False
    )

    pd.DataFrame([report["labelDrift"]]).to_csv(
        os.path.join(REPORT_PATH, "label_drift_report.csv"),
        index=False
    )

    pd.DataFrame([report["modelDrift"]]).to_csv(
        os.path.join(REPORT_PATH, "model_drift_report.csv"),
        index=False
    )

    with open(os.path.join(REPORT_PATH, "drift_summary.txt"), "w") as f:
        f.write("=== DRIFT REPORT ===\n")
        f.write(f"Reference table: {report['referenceTable']}\n")
        f.write(f"Live table: {report['liveTable']}\n")
        f.write(f"Avg feature drift score: {report['avgDriftScore']:.4f}\n")
        f.write(f"High-drift features: {report['highDriftCount']}\n")
        f.write(f"Critical features: {report['criticalCount']}\n")
        f.write(f"Missing features: {len(report['missingFeatures'])}\n")
        f.write(f"Excluded features: {len(report['excludedFeatures'])}\n")
        f.write(f"Label drift available: {report['labelDrift'].get('available', False)}\n")
        f.write(f"Label drift alert: {report['labelDrift'].get('shouldAlert', False)}\n")
        f.write(f"Model drift available: {report['modelDrift'].get('available', False)}\n")
        f.write(f"Model drift alert: {report['modelDrift'].get('shouldAlert', False)}\n")
        f.write(f"Should alert: {should_alert(report)}\n")


def _build_flag_block(report):
    if not should_alert(report):
        return (
            '<div style="background:#e8f5e9;border-left:4px solid #43a047;'
            'padding:10px 14px;margin:12px 0;border-radius:4px">'
            '✅ No severe drift detected'
            '</div>'
        )

    return (
        '<div style="background:#fff8e1;border-left:4px solid #f9a825;'
        'padding:10px 14px;margin:12px 0;border-radius:4px">'
        f"<b>⚠ Drift alert triggered</b><br/>"
        f"High-drift features: {report['highDriftCount']} &nbsp;|&nbsp; "
        f"Critical features: {report['criticalCount']} &nbsp;|&nbsp; "
        f"Label drift alert: {report['labelDrift'].get('shouldAlert', False)} &nbsp;|&nbsp; "
        f"Model drift alert: {report['modelDrift'].get('shouldAlert', False)}"
        '</div>'
    )


def _build_feature_table_html(feature_stats, top_n=10):
    if not feature_stats:
        return "<p>No feature statistics available.</p>"

    top_rows = feature_stats[:top_n]

    rows_html = "".join(
        f"""
        <tr>
          <td>{row['feature']}</td>
          <td>{row['featureType']}</td>
          <td>{row['referenceValue']:.4f}</td>
          <td>{row['currentValue']:.4f}</td>
          <td>{row['driftScore']:.2f}</td>
          <td>{row['severity']}</td>
        </tr>
        """
        for row in top_rows
    )

    return f"""
    <table style="border-collapse:collapse;font-size:12px;width:100%">
      <thead>
        <tr style="background:#f0f4f8">
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Feature</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Type</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Reference</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Current</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Drift Score</th>
          <th style="padding:8px;border:1px solid #ddd;text-align:left">Severity</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
    """


def _build_label_drift_html(label_report):
    if not label_report.get("available", False):
        return f"<p>Label drift unavailable: {label_report.get('reason', 'Unknown reason')}</p>"

    return f"""
    <table style="border-collapse:collapse;font-size:12px;width:100%">
      <tr><td><b>Label column</b></td><td>{label_report['labelCol']}</td></tr>
      <tr><td><b>Reference mean</b></td><td>{label_report['referenceMean']:.4f}</td></tr>
      <tr><td><b>Live mean</b></td><td>{label_report['liveMean']:.4f}</td></tr>
      <tr><td><b>Reference std</b></td><td>{label_report['referenceStd']:.4f}</td></tr>
      <tr><td><b>Live std</b></td><td>{label_report['liveStd']:.4f}</td></tr>
      <tr><td><b>Reference (50th percentile)</b></td><td>{label_report['referenceP50']:.4f}</td></tr>
      <tr><td><b>Live (50th percentile)</b></td><td>{label_report['liveP50']:.4f}</td></tr>
      <tr><td><b>Reference (90th percentile)</b></td><td>{label_report['referenceP90']:.4f}</td></tr>
      <tr><td><b>Live (90th percentile)</b></td><td>{label_report['liveP90']:.4f}</td></tr>
      <tr><td><b>Drift score</b></td><td>{label_report['driftScore']:.4f}</td></tr>
      <tr><td><b>Severity</b></td><td>{label_report['severity']}</td></tr>
      <tr><td><b>Should alert</b></td><td>{label_report['shouldAlert']}</td></tr>
    </table>
    """


def _build_model_drift_html(model_report):
    if not model_report.get("available", False):
        return f"<p>Model drift unavailable: {model_report.get('reason', 'Unknown reason')}</p>"

    return f"""
    <table style="border-collapse:collapse;font-size:12px;width:100%">
      <tr><td><b>Baseline RMSE</b></td><td>{model_report['baselineRmse']:.4f}</td></tr>
      <tr><td><b>Live RMSE</b></td><td>{model_report['liveRmse']:.4f}</td></tr>
      <tr><td><b>Live MAE</b></td><td>{model_report['liveMae']:.4f}</td></tr>
      <tr><td><b>Live SMAPE</b></td><td>{model_report['liveSmape']:.4f}</td></tr>
      <tr><td><b>RMSE degradation ratio</b></td><td>{model_report['rmseDegradationRatio']:.4f}</td></tr>
      <tr><td><b>Severity</b></td><td>{model_report['severity']}</td></tr>
      <tr><td><b>Should alert</b></td><td>{model_report['shouldAlert']}</td></tr>
    </table>
    """


def _build_missing_features_html(missing_features):
    if not missing_features:
        return "<p>No missing drift features.</p>"

    items = "".join(f"<li>{feature}</li>" for feature in missing_features)
    return f"<ul style='margin-top:6px'>{items}</ul>"


def _build_excluded_features_html(excluded_features):
    if not excluded_features:
        return "<p>No excluded features.</p>"

    items = "".join(f"<li>{feature}</li>" for feature in excluded_features)
    return f"<ul style='margin-top:6px'>{items}</ul>"


def _build_email_body(report):
    feature_table = _build_feature_table_html(report["featureStats"], top_n=10)
    label_html = _build_label_drift_html(report["labelDrift"])
    model_html = _build_model_drift_html(report["modelDrift"])
    missing_html = _build_missing_features_html(report["missingFeatures"])
    excluded_html = _build_excluded_features_html(report["excludedFeatures"])

    return f"""
    <div style="font-family:Arial,sans-serif;font-size:13px;max-width:900px;color:#222">
      <h2 style="margin-bottom:4px">Drift Alert Report</h2>
      <p style="color:#666;margin-top:0">
        Avg feature drift score: <b>{report['avgDriftScore']:.2f}</b>
      </p>

      {_build_flag_block(report)}

      <hr style="border:none;border-top:1px solid #e0e0e0;margin:16px 0"/>

      <h3>Top Drifted Features</h3>
      {feature_table}

      <h3>Label Drift</h3>
      {label_html}

      <h3>Model Drift</h3>
      {model_html}

      <h3>Missing / unavailable drift fields</h3>
      {missing_html}

      <h3>Excluded fields</h3>
      {excluded_html}

      <p style="color:#999;font-size:11px;margin-top:24px">
        Generated by drift_detector.py
      </p>
    </div>
    """


def send_drift_alert(report):
    alert_emails = parse_alert_emails()
    if not alert_emails:
        logger.warning("ALERT_EMAILS not configured. Skipping drift alert email.")
        return

    subject = "[MLOps] Drift Alert"
    if should_alert(report):
        subject += " ⚠ Action Needed"
    else:
        subject += " ✔ Healthy"

    html_body = _build_email_body(report)

    attachment_paths = [
        os.path.join(REPORT_PATH, "feature_drift_report.csv"),
        os.path.join(REPORT_PATH, "missing_feature_report.csv"),
        os.path.join(REPORT_PATH, "excluded_feature_report.csv"),
        os.path.join(REPORT_PATH, "label_drift_report.csv"),
        os.path.join(REPORT_PATH, "model_drift_report.csv"),
    ]
    existing_files = [path for path in attachment_paths if os.path.exists(path)]

    send_email(
        to=alert_emails,
        subject=subject,
        html_content=html_body,
        files=existing_files
    )
    logger.info("Drift alert email sent.")


# if __name__ == "__main__":
#     report = detect_drift()
#     save_reports(report)

#     print("\n=== DRIFT RESPONSE ===")
#     print(f"Features returned: {len(report['featureStats'])}")
#     print(f"Missing features: {len(report['missingFeatures'])}")
#     print(f"Excluded features: {len(report['excludedFeatures'])}")
#     print(f"Avg feature drift score: {report['avgDriftScore']:.4f}")
#     print(f"High-drift features: {report['highDriftCount']}")
#     print(f"Critical features: {report['criticalCount']}")
#     print(f"Label drift: {report['labelDrift']}")
#     print(f"Model drift: {report['modelDrift']}")
#     print(f"Should alert: {should_alert(report)}")

#     for row in report["featureStats"][:10]:
#         print(row)

#     if should_alert(report):
#         send_drift_alert(report)