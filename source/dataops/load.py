import os
import pandas as pd
from sqlalchemy import text

import logging
from utils.config import PROCESSED_PATH
from utils.db import engine
from utils.monitoring import monitor

logger = logging.getLogger(__name__)

@monitor
def load_data(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    logger.info(f"Starting load for {year}-{month:02d}")

    with engine.begin() as conn:
        # -------------------------
        # load fact_trips
        # -------------------------
        fact_path = os.path.join(PROCESSED_PATH, "fact_trips", f"{year}-{month:02d}")
        if not os.path.exists(fact_path):
            logger.warning(f"No processed fact_trips data found at {fact_path}")
        else:
            fact_df = pd.read_parquet(fact_path)
            logger.info(f"Loaded fact_trips with {len(fact_df)} rows")

            # keep only columns that exist in the new fact schema
            fact_cols = [
                "pulocationid",
                "dolocationid",
                "year",
                "month",
                "day_of_week",
                "hour",
                "num_trips",
                "avg_fare_amount",
                "total_fare_amount",
                "avg_total_amount",
                "avg_trip_distance",
                "avg_trip_time",
                "avg_tolls_amount",
                "total_tolls_amount",
                "avg_tip_amount",
                "total_tip_amount",
                "avg_airport_fee",
                "total_airport_fee",
                "avg_congestion_surcharge",
                "total_congestion_surcharge",
                "avg_cbd_congestion_fee",
                "total_cbd_congestion_fee",
                "avg_extra",
                "total_extra",
                "avg_total_fee",
                "row_fingerprint",
            ]
            fact_df = fact_df[[c for c in fact_cols if c in fact_df.columns]]

            # rerun-safe load for this month
            conn.execute(
                text("""
                    DELETE FROM fact_trips
                    WHERE year = :year AND month = :month
                """),
                {"year": year, "month": month}
            )

            fact_df.to_sql(
                "fact_trips",
                conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
            logger.info("fact_trips loaded to Postgres successfully")

        # -------------------------
        # load dim_weather
        # -------------------------
        weather_path = os.path.join(PROCESSED_PATH, "dim_weather", f"{year}-{month:02d}")
        if not os.path.exists(weather_path):
            logger.warning(f"No processed dim_weather data found at {weather_path}")
        else:
            weather_df = pd.read_parquet(weather_path)
            logger.info(f"Loaded dim_weather with {len(weather_df)} rows")

            weather_cols = [
                "borough",
                "year",
                "month",
                "day_of_week",
                "avg_temperature_mean",
                "avg_precipitation_sum",
                "avg_wind_speed_max",
                "rainy_days_count",
                "num_days",
            ]
            weather_df = weather_df[[c for c in weather_cols if c in weather_df.columns]]

            # rerun-safe load for this month
            conn.execute(
                text("""
                    DELETE FROM dim_weather
                    WHERE year = :year AND month = :month
                """),
                {"year": year, "month": month}
            )

            weather_df.to_sql(
                "dim_weather",
                conn,
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi",
            )
            logger.info("dim_weather loaded to Postgres successfully")

        # -------------------------
        # load dim_zone
        # -------------------------
        zone_path = os.path.join(PROCESSED_PATH, "dim_zone.csv")
        if not os.path.exists(zone_path):
            logger.warning(f"No processed dim_zone data found at {zone_path}")
        else:
            zone_df = pd.read_csv(zone_path)
            logger.info(f"Loaded dim_zone with {len(zone_df)} rows")

            zone_cols = ["location_id", "borough", "zone", "service_zone"]
            zone_df = zone_df[[c for c in zone_cols if c in zone_df.columns]]

            conn.execute(text("DELETE FROM dim_zone"))

            zone_df.to_sql(
                "dim_zone",
                conn,
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi",
            )
            logger.info("dim_zone replaced in Postgres successfully")

    logger.info(f"Load completed for {year}-{month:02d}")