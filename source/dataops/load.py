import os
import pandas as pd
from utils.config import PROCESSED_PATH
from utils.db import engine
from utils.monitoring import monitor
import logging

logger = logging.getLogger(__name__)

@monitor
def load_data(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    logger.info(f"Starting load for {year}-{month:02d}")

    # load fact_trips_pickup  (zone-hour aggregated by pickup zone)
    pickup_path = os.path.join(PROCESSED_PATH, "fact_trips_pickup", f"{year}-{month:02d}")
    if not os.path.exists(pickup_path):
        logger.warning(f"No fact_trips_pickup data found at {pickup_path}")
    else:
        pickup_df = pd.read_parquet(pickup_path)
        logger.info(f"Loaded fact_trips_pickup with {len(pickup_df):,} rows")
        pickup_df.to_sql(
            "fact_trips_pickup",
            engine,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi"
        )
        logger.info("fact_trips_pickup appended to Postgres successfully")

    '''
    # load fact_trips_pair
    pair_path = os.path.join(PROCESSED_PATH, "fact_trips_pair", f"{year}-{month:02d}")
    if not os.path.exists(pair_path):
        logger.warning(f"No fact_trips_pair data found at {pair_path}")
    else:
        pair_df = pd.read_parquet(pair_path)
        logger.info(f"Loaded fact_trips_pair with {len(pair_df):,} rows")
        pair_df.to_sql(
            "fact_trips_pair",
            engine,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi"
        )
        logger.info("fact_trips_pair appended to Postgres successfully")
    '''

    # load dim_weather
    weather_path = os.path.join(PROCESSED_PATH, "dim_weather", f"{year}-{month:02d}")
    if not os.path.exists(weather_path):
        logger.warning(f"No processed dim_weather data found at {weather_path}")
    else:
        weather_df = pd.read_parquet(weather_path)
        logger.info(f"Loaded dim_weather with {len(weather_df)} rows")

        weather_df.to_sql(
            "dim_weather",
            engine,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi"
        )
        logger.info("dim_weather appended to Postgres database successfully")

    logger.info(f"Load completed for {year}-{month:02d}")