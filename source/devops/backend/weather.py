import pandas as pd
import requests
from sqlalchemy import text

from source.utils.config import BOROUGH_COORDS
from source.utils.db import get_engine

import logging
logger = logging.getLogger(__name__)

def get_forecasted_weather(zone_ids, ts):
    logger.info("Getting forecasted weather | zone_ids=%s | ts=%s", zone_ids, ts)
    engine = get_engine()

    zone_query = text("""
        SELECT location_id, borough
        FROM dim_zone
        WHERE location_id = ANY(:zone_ids)
    """)

    with engine.connect() as conn:
        zone_df = pd.read_sql(zone_query, conn, params={"zone_ids": zone_ids})

    logger.info("Loaded zone borough mappings | rows=%s", len(zone_df))

    if zone_df.empty:
        logger.warning("No zone metadata found for weather lookup")
        return {}

    borough_by_zone = {}
    for _, row in zone_df.iterrows():
        borough_by_zone[int(row["location_id"])] = str(row["borough"]).strip() if pd.notna(row["borough"]) else None

    unique_boroughs = set()
    for borough in borough_by_zone.values():
        if borough and borough in BOROUGH_COORDS:
            unique_boroughs.add(borough)

    weather_by_borough = {}
    logger.info("Unique boroughs to fetch weather for | boroughs=%s", list(unique_boroughs))
    for borough in unique_boroughs:
        logger.info("Fetching weather for borough=%s", borough)
        weather_by_borough[borough] = fetch_open_meteo_forecast_for_borough(borough, ts)

    weather_by_zone = {}
    for zone_id in zone_ids:
        borough = borough_by_zone.get(zone_id)

        if not borough or borough not in weather_by_borough:
            weather_by_zone[zone_id] = {
                "borough": borough,
                "temperature_mean": None,
                "precipitation_sum": None,
                "wind_speed_max": None,
            }
            continue

        borough_weather = weather_by_borough[borough]
        weather_by_zone[zone_id] = {
            "borough": borough,
            "temperature_mean": borough_weather["temperature_mean"],
            "precipitation_sum": borough_weather["precipitation_sum"],
            "wind_speed_max": borough_weather["wind_speed_max"],
        }

    return weather_by_zone


def fetch_open_meteo_forecast_for_borough(borough, ts):
    logger.info("Fetching Open-Meteo forecast | borough=%s | ts=%s", borough, ts)
    
    coords = BOROUGH_COORDS.get(borough)
    if not coords:
        logger.warning("Missing coordinates for borough=%s", borough)
        return {
            "temperature_mean": None,
            "precipitation_sum": None,
            "wind_speed_max": None,
        }

    lat, lon = coords

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "start_date": ts.date().isoformat(),
        "end_date": ts.date().isoformat(),
        "timezone": "UTC",
    }

    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params=params,
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()

    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    temperatures = hourly.get("temperature_2m", [])
    precipitations = hourly.get("precipitation", [])
    wind_speeds = hourly.get("wind_speed_10m", [])

    target_hour = ts.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00")

    idx = None
    for i, t in enumerate(times):
        if t == target_hour:
            idx = i
            break

    if idx is None:
        logger.warning("No matching forecast hour found | borough=%s | target_hour=%s", borough, target_hour)
        return {
            "temperature_mean": None,
            "precipitation_sum": None,
            "wind_speed_max": None,
        }

    return {
        "temperature_mean": temperatures[idx] if idx < len(temperatures) else None,
        "precipitation_sum": precipitations[idx] if idx < len(precipitations) else None,
        "wind_speed_max": wind_speeds[idx] if idx < len(wind_speeds) else None,
    }
    
def apply_weather_features(rows, weather_by_zone):
    enriched_rows = []

    for row in rows:
        zone_id = row["pulocationid"]
        weather = weather_by_zone.get(zone_id, {})

        temperature_mean = weather.get("temperature_mean")
        precipitation_sum = weather.get("precipitation_sum")
        wind_speed_max = weather.get("wind_speed_max")

        if temperature_mean is None:
            temperature_mean = 0.0
        if precipitation_sum is None:
            precipitation_sum = 0.0
        if wind_speed_max is None:
            wind_speed_max = 0.0

        new_row = dict(row)
        new_row["temperature_mean"] = float(temperature_mean)
        new_row["precipitation_sum"] = float(precipitation_sum)
        new_row["wind_speed_max"] = float(wind_speed_max)

        new_row["is_rainy"] = 1 if precipitation_sum > 0 else 0
        new_row["is_heavy_rain"] = 1 if precipitation_sum >= 10 else 0
        new_row["is_hot"] = 1 if temperature_mean > 25 else 0

        enriched_rows.append(new_row)

    return enriched_rows

def get_lag_features(included_zone_ids, timestamp):
    engine = get_engine()

    query = text("""
        SELECT pulocationid, hour_ts, demand
        FROM fact_trips_pickup
        WHERE pulocationid = ANY(:zone_ids)
          AND hour_ts >= :min_ts
          AND hour_ts <= :max_ts
    """)

    min_ts = timestamp - pd.Timedelta(hours=24)
    max_ts = timestamp - pd.Timedelta(hours=1)

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "zone_ids": included_zone_ids,
                "min_ts": min_ts,
                "max_ts": max_ts,
            },
            parse_dates=["hour_ts"],
        )

    lag_map = {}

    for zone_id in included_zone_ids:
        zone_df = df[df["pulocationid"] == zone_id].copy()

        lag_1_ts = timestamp - pd.Timedelta(hours=1)
        lag_2_ts = timestamp - pd.Timedelta(hours=2)
        lag_24_ts = timestamp - pd.Timedelta(hours=24)
        roll_start_ts = timestamp - pd.Timedelta(hours=3)

        lag_1 = zone_df.loc[zone_df["hour_ts"] == lag_1_ts, "demand"]
        lag_2 = zone_df.loc[zone_df["hour_ts"] == lag_2_ts, "demand"]
        lag_24 = zone_df.loc[zone_df["hour_ts"] == lag_24_ts, "demand"]

        rolling_df = zone_df[
            (zone_df["hour_ts"] >= roll_start_ts) &
            (zone_df["hour_ts"] <= lag_1_ts)
        ]

        lag_map[zone_id] = {
            "demand_lag_1": float(lag_1.iloc[0]) if not lag_1.empty else 0.0,
            "demand_lag_2": float(lag_2.iloc[0]) if not lag_2.empty else 0.0,
            "demand_lag_24": float(lag_24.iloc[0]) if not lag_24.empty else 0.0,
            "rolling_mean_3h": float(rolling_df["demand"].mean()) if not rolling_df.empty else 0.0,
        }
    logger.info("Lag query returned %s rows", len(df))
    return lag_map


def apply_lag_features(rows, lag_map):
    enriched_rows = []

    for row in rows:
        zone_id = row["pulocationid"]
        lag_features = lag_map.get(zone_id, {})

        new_row = dict(row)
        new_row["demand_lag_1"] = float(lag_features.get("demand_lag_1", 0.0))
        new_row["demand_lag_2"] = float(lag_features.get("demand_lag_2", 0.0))
        new_row["demand_lag_24"] = float(lag_features.get("demand_lag_24", 0.0))
        new_row["rolling_mean_3h"] = float(lag_features.get("rolling_mean_3h", 0.0))

        enriched_rows.append(new_row)

    return enriched_rows