from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from sqlalchemy import text
from source.utils.db import get_engine

load_dotenv()

app = Flask(__name__)
CORS(app)


@app.route("/")
def home():
    return {"message": "Backend is running"}


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
