from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from source.devops.backend.model import run_model_inference
from source.devops.backend.weather import apply_lag_features, apply_weather_features, get_forecasted_weather, get_lag_features
from source.devops.backend.zones import build_zone_feature_rows, get_nearby_zone_ids, get_zone_metadata
from sqlalchemy import text
from source.utils.db import get_engine
import logging

load_dotenv()

app = Flask(__name__)
CORS(app)

logger = logging.getLogger(__name__)

@app.route("/")
def home():
    return {"message": "Backend is running"}


@app.route("/api/predict")
def get_prediction():
    """
    1. Calculate nearby zone ids based on selected zone ids
    2. Extract forecasted weather data for all zone ids
    3. Builds dataframe with engineered features
    4. Runs inference using the trained production model
    """
    logger.info("Received /api/predict request")

    timestamp_param = request.args.get("timestamp")
    raw_zone_ids = request.args.get("zone_ids", "")
    logger.info("Raw request params | timestamp=%s | zone_ids=%s", timestamp_param, raw_zone_ids)

    if not timestamp_param:
        logger.warning("Missing timestamp")
        return jsonify({"error": "timestamp is required"}), 400

    try:
        ts = pd.Timestamp(timestamp_param).to_pydatetime()
        logger.info("Parsed timestamp successfully | ts=%s", ts)
    except Exception:
        logger.exception("Failed to parse timestamp: %s", timestamp_param)
        return jsonify({"error": "timestamp must be valid ISO format"}), 400

    try:
        zone_ids = [int(z.strip()) for z in raw_zone_ids.split(",") if z.strip()]
        logger.info("Parsed zone ids successfully | zone_ids=%s", zone_ids)
    except ValueError:
        logger.exception("Failed to parse zone_ids: %s", raw_zone_ids)
        return jsonify({"error": "zone_ids must be comma-separated integers"}), 400

    if not zone_ids:
        logger.warning("No zone_ids provided after parsing")
        return jsonify({"error": "at least one zone_id is required"}), 400

    nearby_zone_ids = get_nearby_zone_ids(zone_ids)
    logger.info("Computed nearby zones | selected=%s | nearby=%s", zone_ids, nearby_zone_ids)

    included_zone_ids = list(dict.fromkeys(zone_ids + nearby_zone_ids))
    logger.info("Included zone ids for inference | included=%s", included_zone_ids)

    weather_by_zone = get_forecasted_weather(included_zone_ids, ts)
    logger.info("Fetched forecasted weather | zone_count=%s", len(weather_by_zone))

    feature_df = build_prediction_dataframe(
        included_zone_ids,
        zone_ids,
        ts,
        weather_by_zone,
    )
    logger.info(
        "Built prediction dataframe | rows=%s | cols=%s | columns=%s",
        len(feature_df),
        len(feature_df.columns),
        list(feature_df.columns),
    )

    predictions = run_model_inference(feature_df)
    logger.info("Inference completed | prediction_count=%s", len(predictions))

    return jsonify({
        "includedZoneIds": included_zone_ids,
        "predictions": predictions,
        "weatherByZoneId": weather_by_zone,
    })
    

@app.route("/api/drift")
def get_drift():
    """
    Returns drift results for a given data month (or latest if not specified).

    Query params:
      ?month=2025-01   (YYYY-MM format, optional)

    Response shape matches DataDriftResponse expected by the frontend:
    {
      featureStats: [{ feature, currentMean, trainingMean, driftScore, severity, featureType }],
      summary: {
        dataMonth, avgDriftScore, highDriftCount, criticalCount, overallStatus,
        trainingTriggered, labelDrift: {...}, modelDrift: {...}
      }
    }
    """
    month_param = request.args.get("month")  # e.g. "2025-01"
    engine = get_engine()
    with engine.connect() as conn:
        if month_param:
            summary_row = conn.execute(text("""
                SELECT * FROM drift_run_summary
                WHERE data_month = :m
            """), {"m": f"{month_param}-01"}).mappings().first()
        else:
            summary_row = conn.execute(text("""
                SELECT * FROM drift_run_summary
                ORDER BY data_month DESC
                LIMIT 1
            """)).mappings().first()

        if not summary_row:
            return jsonify({"featureStats": [], "summary": None}), 200

        feature_rows = conn.execute(text("""
            SELECT
                feature,
                feature_type    AS "featureType",
                reference_value AS "trainingMean",
                current_value   AS "currentMean",
                drift_score     AS "driftScore",
                severity
            FROM drift_feature_stats
            WHERE data_month = :m
            ORDER BY drift_score DESC
        """), {"m": summary_row["data_month"]}).mappings().all()

    return jsonify({
        "featureStats": [dict(r) for r in feature_rows],
        "summary": {
            "dataMonth": str(summary_row["data_month"]),
            "avgDriftScore": summary_row["avg_feature_drift"],
            "highDriftCount": summary_row["high_drift_count"],
            "criticalCount": summary_row["critical_count"],
            "overallStatus": summary_row["overall_status"],
            "trainingTriggered": summary_row["training_triggered"],
            "labelDrift": {
                "driftScore": summary_row["label_drift_score"],
                "severity": summary_row["label_severity"],
                "shouldAlert": summary_row["label_should_alert"],
            },
            "modelDrift": {
                "rmseDegradationRatio": summary_row["model_rmse_ratio"],
                "severity": summary_row["model_severity"],
                "shouldAlert": summary_row["model_should_alert"],
            },
        },
    })


@app.route("/api/drift/history")
def get_drift_history():
    """
    Returns drift summary for all available months, sorted chronologically.
    Useful for the frontend to show a timeline of drift over time.
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                data_month,
                overall_status,
                avg_feature_drift,
                high_drift_count,
                critical_count,
                training_triggered
            FROM drift_run_summary
            ORDER BY data_month ASC
        """)).mappings().all()

    return jsonify([{
        "dataMonth": str(r["data_month"]),
        "overallStatus": r["overall_status"],
        "avgDriftScore": r["avg_feature_drift"],
        "highDriftCount": r["high_drift_count"],
        "criticalCount": r["critical_count"],
        "trainingTriggered": r["training_triggered"],
    } for r in rows])


def build_prediction_dataframe(included_zone_ids, selected_zone_ids, timestamp, weather_by_zone):
    logger.info("Building prediction dataframe")

    zone_meta = get_zone_metadata(included_zone_ids)
    logger.info("Fetched zone metadata | count=%s", len(zone_meta))

    rows = build_zone_feature_rows(
        included_zone_ids,
        selected_zone_ids,
        timestamp,
        zone_meta,
    )
    logger.info("Built base feature rows | row_count=%s", len(rows))

    rows = apply_weather_features(rows, weather_by_zone)
    logger.info("Applied weather features")

    lag_map = get_lag_features(included_zone_ids, timestamp)
    logger.info("Fetched lag features | zone_count=%s", len(lag_map))

    rows = apply_lag_features(rows, lag_map)
    logger.info("Applied lag features")

    df = pd.DataFrame(rows)

    drop_cols = ["borough", "service_zone"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    logger.info("Final dataframe ready | shape=%s", df.shape)
    return df


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
