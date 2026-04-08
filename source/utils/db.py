import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
WATERMARK_SECRET_KEY = os.getenv("WATERMARK_SECRET_KEY")

engine = create_engine(DATABASE_URL)

def setup_tables():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_trips (
                pulocationid                INTEGER,
                dolocationid                INTEGER,
                year                        INTEGER,
                month                       INTEGER,
                day_of_week                 INTEGER,
                hour                        INTEGER,
                num_trips                   BIGINT,

                avg_fare_amount             DOUBLE PRECISION,
                total_fare_amount           DOUBLE PRECISION,
                avg_total_amount            DOUBLE PRECISION,
                avg_trip_distance           DOUBLE PRECISION,
                avg_trip_time               DOUBLE PRECISION,
                avg_tolls_amount            DOUBLE PRECISION,
                total_tolls_amount          DOUBLE PRECISION,
                avg_tip_amount              DOUBLE PRECISION,
                total_tip_amount            DOUBLE PRECISION,
                avg_airport_fee             DOUBLE PRECISION,
                total_airport_fee           DOUBLE PRECISION,
                avg_congestion_surcharge    DOUBLE PRECISION,
                total_congestion_surcharge  DOUBLE PRECISION,
                avg_cbd_congestion_fee      DOUBLE PRECISION,
                total_cbd_congestion_fee    DOUBLE PRECISION,
                avg_extra                   DOUBLE PRECISION,
                total_extra                 DOUBLE PRECISION,
                avg_total_fee               DOUBLE PRECISION,

                row_fingerprint             TEXT,
                
                PRIMARY KEY (
                    pulocationid,
                    dolocationid,
                    year,
                    month,
                    day_of_week,
                    hour
                )
            );

            ALTER TABLE fact_trips ADD COLUMN IF NOT EXISTS row_fingerprint TEXT;

            CREATE TABLE IF NOT EXISTS dim_weather (
                borough                 TEXT,
                year                    INTEGER,
                month                   INTEGER,
                day_of_week             INTEGER,
                avg_temperature_mean    DOUBLE PRECISION,
                avg_precipitation_sum   DOUBLE PRECISION,
                avg_wind_speed_max      DOUBLE PRECISION,
                rainy_days_count        INTEGER,
                num_days                INTEGER,
                
                PRIMARY KEY (
                    borough,
                    year,
                    month,
                    day_of_week
                )
            );

            CREATE TABLE IF NOT EXISTS dim_zone (
                location_id   INTEGER PRIMARY KEY,
                borough       TEXT,
                zone          TEXT,
                service_zone  TEXT
            );
        """))
    print("Tables created successfully")

def load_enriched_trip_data():
    import pandas as pd
    
    query = """
        SELECT
            f.pulocationid,
            f.dolocationid,
            f.year,
            f.month,
            f.day_of_week,
            f.hour,
            f.num_trips,
            f.avg_fare_amount,
            f.total_fare_amount,
            f.avg_total_amount,
            f.avg_trip_distance,
            f.avg_trip_time,
            f.avg_tolls_amount,
            f.total_tolls_amount,
            f.avg_tip_amount,
            f.total_tip_amount,
            f.avg_airport_fee,
            f.total_airport_fee,
            f.avg_congestion_surcharge,
            f.total_congestion_surcharge,
            f.avg_cbd_congestion_fee,
            f.total_cbd_congestion_fee,
            f.avg_extra,
            f.total_extra,
            f.avg_total_fee,
            f.row_fingerprint,

            pu.borough       AS pu_borough,
            pu.zone          AS pu_zone,
            pu.service_zone  AS pu_service_zone,

            dz.borough       AS do_borough,
            dz.zone          AS do_zone,
            dz.service_zone  AS do_service_zone,

            w.avg_temperature_mean,
            w.avg_precipitation_sum,
            w.avg_wind_speed_max,
            w.rainy_days_count,
            w.num_days

        FROM fact_trips f
        LEFT JOIN dim_zone pu
            ON f.pulocationid = pu.location_id
        LEFT JOIN dim_zone dz
            ON f.dolocationid = dz.location_id
        LEFT JOIN dim_weather w
            ON pu.borough = w.borough
        AND f.year = w.year
        AND f.month = w.month
        AND f.day_of_week = w.day_of_week
    """
    return pd.read_sql(query, engine)