import os
import pandas as pd
from sqlalchemy import text
from utils.config import PROCESSED_PATH, get_month_year
from utils.db import get_engine
from utils.monitoring import monitor
import logging

logger = logging.getLogger(__name__)

@monitor
def load_data(**context):
    execution_date = context["execution_date"]
    year, month = get_month_year(execution_date)

    logger.info(f"Starting load for {year}-{month:02d}")
    
    engine = get_engine()
    with engine.begin() as conn:
        # load fact_trips_pickup
        pickup_path = os.path.join(PROCESSED_PATH, "fact_trips_pickup", f"{year}-{month:02d}")
        if not os.path.exists(pickup_path):
            logger.warning(f"No fact_trips_pickup data found at {pickup_path}")
        else:
            pickup_df = pd.read_parquet(pickup_path)
            logger.info(f"Loaded fact_trips_pickup with {len(pickup_df):,} rows")

            pickup_df["hour_ts"] = pd.to_datetime(pickup_df["hour_ts"])

            min_ts = pickup_df["hour_ts"].min()
            max_ts = pickup_df["hour_ts"].max()

            logger.info(f"pickup_df hour_ts min={min_ts}, max={max_ts}")

            conn.execute(
                text("""
                    DELETE FROM fact_trips_pickup
                    WHERE hour_ts >= :min_ts
                      AND hour_ts <= :max_ts
                """),
                {"min_ts": min_ts, "max_ts": max_ts}
            )

            pickup_df.to_sql(
                "fact_trips_pickup",
                conn,
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi"
            )

            logger.info("fact_trips_pickup overwritten for this month successfully")

        # load dim_weather
        weather_path = os.path.join(PROCESSED_PATH, "dim_weather", f"{year}-{month:02d}")
        if not os.path.exists(weather_path):
            logger.warning(f"No processed dim_weather data found at {weather_path}")
        else:
            weather_df = pd.read_parquet(weather_path)
            logger.info(f"Loaded dim_weather with {len(weather_df):,} rows")

            weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.date

            conn.execute(
                text("""
                    DELETE FROM dim_weather
                    WHERE EXTRACT(YEAR FROM date) = :year
                      AND EXTRACT(MONTH FROM date) = :month
                """),
                {"year": year, "month": month}
            )

            weather_df.to_sql(
                "dim_weather",
                conn,
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi"
            )

            logger.info("dim_weather overwritten for this month successfully")

        # load dim_zone
        zone_path = os.path.join(PROCESSED_PATH, "dim_zone.csv")
        if not os.path.exists(zone_path):
            logger.warning(f"No dim_zone data found at {zone_path}")
        else:
            zone_df = pd.read_csv(zone_path)
            logger.info(f"Loaded dim_zone with {len(zone_df):,} rows")

            conn.execute(text("DELETE FROM dim_zone"))

            zone_df.to_sql(
                "dim_zone",
                conn,
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi"
            )

            logger.info("dim_zone replaced successfully")

    logger.info(f"Load completed for {year}-{month:02d}")