from functools import lru_cache
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

load_dotenv()

WATERMARK_SECRET_KEY = os.getenv("WATERMARK_SECRET_KEY")

@lru_cache(maxsize=1)
def get_engine():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set")

    return create_engine(
        database_url,
        poolclass=NullPool,
        pool_pre_ping=True,
        future=True,
    )

def setup_tables():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_trips_pickup (
                hour_ts             TIMESTAMP,
                pulocationid        INTEGER,
                demand              BIGINT,
                avg_trip_distance   DOUBLE PRECISION,
                avg_total_amount    DOUBLE PRECISION,
                row_fingerprint     TEXT,
                PRIMARY KEY (hour_ts, pulocationid)
            );

            ALTER TABLE fact_trips_pickup ADD COLUMN IF NOT EXISTS row_fingerprint TEXT;

            CREATE TABLE IF NOT EXISTS dim_weather (
                date                DATE,
                temperature_mean    DOUBLE PRECISION,
                precipitation_sum   DOUBLE PRECISION,
                wind_speed_max      DOUBLE PRECISION,
                borough             TEXT
            );

            CREATE TABLE IF NOT EXISTS dim_zone (
                location_id         INTEGER PRIMARY KEY,
                borough             TEXT,
                zone                TEXT,
                service_zone        TEXT
            );

            CREATE TABLE IF NOT EXISTS drift_run_summary (
                id                  SERIAL PRIMARY KEY,
                data_month          DATE NOT NULL UNIQUE,
                execution_date      DATE NOT NULL,
                avg_feature_drift   DOUBLE PRECISION,
                high_drift_count    INTEGER,
                critical_count      INTEGER,
                label_drift_score   DOUBLE PRECISION,
                label_severity      TEXT,
                label_should_alert  BOOLEAN,
                model_rmse_ratio    DOUBLE PRECISION,
                model_severity      TEXT,
                model_should_alert  BOOLEAN,
                overall_status      TEXT,
                training_triggered  BOOLEAN DEFAULT FALSE,
                created_at          TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS drift_feature_stats (
                id              SERIAL PRIMARY KEY,
                run_id          INTEGER REFERENCES drift_run_summary(id) ON DELETE CASCADE,
                data_month      DATE NOT NULL,
                feature         TEXT NOT NULL,
                feature_type    TEXT,
                reference_value DOUBLE PRECISION,
                current_value   DOUBLE PRECISION,
                drift_score     DOUBLE PRECISION,
                severity        TEXT
            );
        """))
    print("Tables created successfully")
