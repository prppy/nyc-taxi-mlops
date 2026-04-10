import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
WATERMARK_SECRET_KEY = os.getenv("WATERMARK_SECRET_KEY")

engine = create_engine(DATABASE_URL)

def setup_tables():
    with engine.begin() as conn:  # auto-commits on exit
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_trips (
                pickup_datetime         TIMESTAMP,
                dropoff_datetime        TIMESTAMP,
                pulocationid            INTEGER,
                dolocationid            INTEGER,
                trip_distance           DOUBLE PRECISION,
                fare_amount             DOUBLE PRECISION,
                total_amount            DOUBLE PRECISION,
                taxi_type               TEXT,
                vendor_id               BIGINT,
                hvfhs_license_num       TEXT,
                dispatching_base_num    TEXT,
                originating_base_num    TEXT,
                passenger_count         INTEGER,
                ratecode_id             TEXT,
                request_datetime        TIMESTAMP,
                on_scene_datetime       TIMESTAMP,
                store_and_fwd_flag      BOOLEAN,
                payment_type            TEXT,
                trip_time               BIGINT,
                shared_request_flag     BOOLEAN,
                extra                   DOUBLE PRECISION,
                mta_tax                 DOUBLE PRECISION,
                improvement_surcharge   DOUBLE PRECISION,
                wav_request_flag        BOOLEAN,
                wav_match_flag          BOOLEAN,
                tip_amount              DOUBLE PRECISION,
                tolls_amount            DOUBLE PRECISION,
                driver_pay              DOUBLE PRECISION,
                congestion_surcharge    DOUBLE PRECISION,
                airport_fee             DOUBLE PRECISION,
                cbd_congestion_fee      DOUBLE PRECISION,
                bcf                     DOUBLE PRECISION,
                sales_tax               DOUBLE PRECISION,
                access_a_ride_flag      BOOLEAN,
                shared_match_flag       BOOLEAN,
                row_fingerprint         TEXT
            );
                          
            ALTER TABLE fact_trips ADD COLUMN IF NOT EXISTS row_fingerprint TEXT;

            CREATE TABLE IF NOT EXISTS dim_weather (
                date                DATE,
                temperature_mean    DOUBLE PRECISION,
                precipitation_sum   DOUBLE PRECISION,
                wind_speed_max      DOUBLE PRECISION,
                borough             TEXT
            );
                          
            CREATE TABLE IF NOT EXISTS dim_zone (
                location_id   INTEGER PRIMARY KEY,
                borough       TEXT,
                zone          TEXT,
                service_zone  TEXT
            );
        """))
    print("Tables created successfully")
    
def load_features():
    import pandas as pd
    query = """
    SELECT *
    FROM pickup_features
    """

    df = pd.read_sql(query, engine)

    print(f"Loaded from Postgres: {df.shape}")
    return df