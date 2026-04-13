import os
import re
import logging
from dotenv import load_dotenv

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import NumericType

from airflow.utils.email import send_email

logger = logging.getLogger(__name__)


REFERENCE_TABLE = "pickup_features"
LIVE_TABLE = "live_features"
REPORT_PATH = "data/monitor/reports"

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


spark = (
    SparkSession.builder
    .appName("Drift Detector")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)


load_dotenv(override=True)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

match = re.match(r"postgresql://(.*):(.*)@(.*):(.*)/(.*)", DATABASE_URL)
if not match:
    raise ValueError("Invalid DATABASE_URL format")

user, password, host, port, db = match.groups()

DB_URL = f"jdbc:postgresql://{host}:{port}/{db}"
DB_PROPERTIES = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver",
}


def parse_alert_emails():
    raw = os.getenv("ALERT_EMAILS", "").strip()
    if not raw:
        return []

    raw = raw.strip("[]")
    emails = [e.strip().strip("'").strip('"') for e in raw.split(",")]
    return [e for e in emails if e]


def load_table(table_name):
    logger.info(f"Loading table: {table_name}")
    return spark.read.jdbc(
        url=DB_URL,
        table=table_name,
        properties=DB_PROPERTIES
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


def compute_relative_shift(reference_value, current_value):
    """
    Relative shift normalized by reference value, clamped to [0, 1].
    Used for both:
    - continuous numeric features (mean shift)
    - one-hot categorical features (proportion shift)
    """
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


def detect_drift(
    reference_table=REFERENCE_TABLE,
    live_table=LIVE_TABLE,
):
    reference_df = load_table(reference_table)
    live_df = load_table(live_table)

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


def should_alert(report):
    return (
        report["criticalCount"] > 0
        or report["highDriftCount"] >= MIN_HIGH_DRIFT_FEATURES_FOR_ALERT
    )


def save_reports(report):
    os.makedirs(REPORT_PATH, exist_ok=True)

    feature_stats_df = pd.DataFrame(report["featureStats"])
    feature_stats_df.to_csv(
        os.path.join(REPORT_PATH, "feature_drift_report.csv"),
        index=False
    )

    missing_df = pd.DataFrame({"feature": report["missingFeatures"]})
    missing_df.to_csv(
        os.path.join(REPORT_PATH, "missing_feature_report.csv"),
        index=False
    )

    excluded_df = pd.DataFrame({"feature": report["excludedFeatures"]})
    excluded_df.to_csv(
        os.path.join(REPORT_PATH, "excluded_feature_report.csv"),
        index=False
    )

    with open(os.path.join(REPORT_PATH, "drift_summary.txt"), "w") as f:
        f.write("=== DRIFT REPORT ===\n")
        f.write(f"Reference table: {REFERENCE_TABLE}\n")
        f.write(f"Live table: {LIVE_TABLE}\n")
        f.write(f"Avg drift score: {report['avgDriftScore']:.4f}\n")
        f.write(f"High-drift features: {report['highDriftCount']}\n")
        f.write(f"Critical features: {report['criticalCount']}\n")
        f.write(f"Missing features: {len(report['missingFeatures'])}\n")
        f.write(f"Excluded features: {len(report['excludedFeatures'])}\n")
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
        f"Critical features: {report['criticalCount']}"
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
    missing_html = _build_missing_features_html(report["missingFeatures"])
    excluded_html = _build_excluded_features_html(report["excludedFeatures"])

    return f"""
    <div style="font-family:Arial,sans-serif;font-size:13px;max-width:900px;color:#222">
      <h2 style="margin-bottom:4px">Data Drift Alert Report</h2>
      <p style="color:#666;margin-top:0">
        Avg drift score: <b>{report['avgDriftScore']:.2f}</b>
      </p>

      {_build_flag_block(report)}

      <hr style="border:none;border-top:1px solid #e0e0e0;margin:16px 0"/>

      <h3>Top Drifted Features</h3>
      {feature_table}

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

    subject = "[MLOps] Data Drift Alert"
    if should_alert(report):
        subject += " ⚠ Action Needed"
    else:
        subject += " ✔ Healthy"

    html_body = _build_email_body(report)

    attachment_paths = [
        os.path.join(REPORT_PATH, "feature_drift_report.csv"),
        os.path.join(REPORT_PATH, "missing_feature_report.csv"),
        os.path.join(REPORT_PATH, "excluded_feature_report.csv"),
    ]
    existing_files = [path for path in attachment_paths if os.path.exists(path)]

    send_email(
        to=alert_emails,
        subject=subject,
        html_content=html_body,
        files=existing_files
    )
    logger.info("Drift alert email sent.")


if __name__ == "__main__":
    report = detect_drift()
    save_reports(report)

    print("\n=== DATA DRIFT RESPONSE ===")
    print(f"Features returned: {len(report['featureStats'])}")
    print(f"Missing features: {len(report['missingFeatures'])}")
    print(f"Excluded features: {len(report['excludedFeatures'])}")
    print(f"Avg drift score: {report['avgDriftScore']:.4f}")
    print(f"High-drift features: {report['highDriftCount']}")
    print(f"Critical features: {report['criticalCount']}")
    print(f"Should alert: {should_alert(report)}")

    for row in report["featureStats"][:10]:
        print(row)

    if should_alert(report):
        send_drift_alert(report)