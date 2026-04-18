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
            
            CREATE TABLE IF NOT EXISTS pickup_features (
                pulocationid                 INTEGER,
                hour_ts                      TIMESTAMP,
                target_demand                BIGINT,
                hour                         INTEGER,
                day_of_week                  INTEGER,
                month                        INTEGER,
                is_weekend                   INTEGER,
                hour_sin                     DOUBLE PRECISION,
                hour_cos                     DOUBLE PRECISION,
                dow_sin                      DOUBLE PRECISION,
                dow_cos                      DOUBLE PRECISION,
                temperature_mean             DOUBLE PRECISION,
                precipitation_sum            DOUBLE PRECISION,
                wind_speed_max               DOUBLE PRECISION,
                is_rainy                     INTEGER,
                is_heavy_rain                INTEGER,
                is_hot                       INTEGER,
                demand_lag_1                 BIGINT,
                demand_lag_2                 BIGINT,
                demand_lag_24                BIGINT,
                rolling_mean_3h              DOUBLE PRECISION,
                borough_manhattan            INTEGER,
                borough_brooklyn             INTEGER,
                borough_queens               INTEGER,
                borough_bronx                INTEGER,
                "borough_staten island"      INTEGER,
                borough_ewr                  INTEGER,
                borough_nan                  INTEGER,
                "service_zone_Yellow_Zone"   INTEGER,
                "service_zone_Boro_Zone"     INTEGER,
                "service_zone_Airports"      INTEGER,
                "service_zone_EWR"           INTEGER,
                "service_zone_nan"           INTEGER,
                PRIMARY KEY (hour_ts, pulocationid)
            );

            ALTER TABLE pickup_features
                ADD COLUMN IF NOT EXISTS pulocationid INTEGER,
                ADD COLUMN IF NOT EXISTS hour_ts TIMESTAMP,
                ADD COLUMN IF NOT EXISTS target_demand BIGINT,
                ADD COLUMN IF NOT EXISTS hour INTEGER,
                ADD COLUMN IF NOT EXISTS day_of_week INTEGER,
                ADD COLUMN IF NOT EXISTS month INTEGER,
                ADD COLUMN IF NOT EXISTS is_weekend INTEGER,
                ADD COLUMN IF NOT EXISTS hour_sin DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS hour_cos DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS dow_sin DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS dow_cos DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS temperature_mean DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS precipitation_sum DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS wind_speed_max DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS is_rainy INTEGER,
                ADD COLUMN IF NOT EXISTS is_heavy_rain INTEGER,
                ADD COLUMN IF NOT EXISTS is_hot INTEGER,
                ADD COLUMN IF NOT EXISTS demand_lag_1 BIGINT,
                ADD COLUMN IF NOT EXISTS demand_lag_2 BIGINT,
                ADD COLUMN IF NOT EXISTS demand_lag_24 BIGINT,
                ADD COLUMN IF NOT EXISTS rolling_mean_3h DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS borough_manhattan INTEGER,
                ADD COLUMN IF NOT EXISTS borough_brooklyn INTEGER,
                ADD COLUMN IF NOT EXISTS borough_queens INTEGER,
                ADD COLUMN IF NOT EXISTS borough_bronx INTEGER,
                ADD COLUMN IF NOT EXISTS "borough_staten island" INTEGER,
                ADD COLUMN IF NOT EXISTS borough_ewr INTEGER,
                ADD COLUMN IF NOT EXISTS borough_nan INTEGER,
                ADD COLUMN IF NOT EXISTS "service_zone_Yellow_Zone" INTEGER,
                ADD COLUMN IF NOT EXISTS "service_zone_Boro_Zone" INTEGER,
                ADD COLUMN IF NOT EXISTS "service_zone_Airports" INTEGER,
                ADD COLUMN IF NOT EXISTS "service_zone_EWR" INTEGER,
                ADD COLUMN IF NOT EXISTS "service_zone_nan" INTEGER;

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
        
        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'pickup_features_pkey'
                      AND conrelid = 'pickup_features'::regclass
                ) THEN
                    ALTER TABLE pickup_features
                    ADD CONSTRAINT pickup_features_pkey
                    PRIMARY KEY (hour_ts, pulocationid);
                END IF;
            END $$;
        """))
    print("Tables created successfully")
