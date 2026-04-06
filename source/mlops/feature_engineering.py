import os
import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)

BASE_PATH = "data/processed"
INPUT_PATH = os.path.join(BASE_PATH, "eda_joined.parquet")
OUTPUT_PATH = os.path.join(BASE_PATH, "fe_cleaned.parquet")
OUTPUT_PICKUP_PATH = os.path.join(BASE_PATH, "feature_engineered_pickup.parquet")
OUTPUT_PAIR_PATH   = os.path.join(BASE_PATH, "feature_engineered_pickup_dropoff_pair.parquet")

def load_data(input_path: str):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"File not found: {input_path}")
    df = pd.read_parquet(input_path)
    print(f"Loaded data from {input_path}")
    print(f"Shape: {df.shape}")
    return df

def ensure_datetime_columns(df):
    df = df.copy()
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    return df

def engineer_time_features(df):
    df = df.copy()
    df["hour"] = df["pickup_datetime"].dt.hour
    df["day_of_week"] = df["pickup_datetime"].dt.dayofweek
    df["month"] = df["pickup_datetime"].dt.month
    df["day"] = df["pickup_datetime"].dt.day
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_peak_hour"] = df["hour"].isin([17, 18, 19]).astype(int)

    # for cyclical encoding of hour and day of week
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    return df

def engineer_trip_features(df):
    df = df.copy()
    df["trip_duration_min"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds() / 60
    df["speed_mph"] = df["trip_distance"] / (df["trip_duration_min"] / 60)
    df["speed_mph"] = df["speed_mph"].replace([np.inf, -np.inf], np.nan) # to handle if trip_duration_min is 0
    return df

def engineer_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "precipitation_sum" in df.columns:
        df["is_raining"] = (df["precipitation_sum"] > 0).astype(int)
    if "temperature_mean" in df.columns:
        df["extreme_weather_flag"] = (
            (df["temperature_mean"] < 0) |
            (df["temperature_mean"] > 30) |
            (df["precipitation_sum"] > 10)
        ).astype(int)
    return df

def engineer_interaction_features(df):
    df = df.copy()
    if "is_raining" in df.columns and "is_peak_hour" in df.columns:
        df["rain_peak_interaction"] = df["is_raining"] * df["is_peak_hour"]
    return df

# will add a clean_data function later on after EDA is completed 
'''
def clean_trip_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    initial_rows = len(df)
    df = df[
        (df["trip_distance"] > 0) &
        (df["trip_distance"] < 50) &
        (df["trip_duration_min"] > 0) &
        (df["trip_duration_min"] < 300) &
        (df["speed_mph"] > 0) &
        (df["speed_mph"] < 100)
    ]
    removed_rows = initial_rows - len(df)

    print("\n=== CLEANING SUMMARY ===")
    print(f"Rows before cleaning: {initial_rows:,}")
    print(f"Rows after cleaning:  {len(df):,}")
    print(f"Rows removed:         {removed_rows:,}")
    print(f"Percentage removed:   {(removed_rows / initial_rows) * 100:.2f}%")
    return df
'''

# version 1: by pickup zone only
def aggregate_pickup_features(df):
    df = df.copy()
    # create hour-level timestamp
    df["hour_ts"] = df["pickup_datetime"].dt.floor("h")

    # aggregating numerical features by zone + hour
    agg_dict = {
        "trip_distance": "mean",
        "fare_amount": "mean",
        "temperature_mean": "mean",
        "precipitation_sum": "mean",
        "hour": "first",
        "day_of_week": "first",
        "month": "first",
        "hour_sin": "first",
        "hour_cos": "first",
        "dow_sin": "first",
        "dow_cos": "first",
        "is_weekend": "first",
        "is_peak_hour": "first",
        "borough": "first"
    }
    if "extreme_weather_flag" in df.columns:
        agg_dict["extreme_weather_flag"] = "max"
    if "is_raining" in df.columns:
        agg_dict["is_raining"] = "max"
    if "rain_peak_interaction" in df.columns:
        agg_dict["rain_peak_interaction"] = "max"

    # group by pickup + hour
    pickup_hour_df = df.groupby(["pulocationid", "hour_ts"]).agg(agg_dict).reset_index()

    # create target variable demand 
    demand_counts = df.groupby(["pulocationid", "hour_ts"]).size().reset_index(name="demand")
    pickup_hour_df = pickup_hour_df.merge(demand_counts, on=["pulocationid", "hour_ts"], how="left")

    pickup_hour_df = pickup_hour_df.rename(columns={
        "pulocationid": "pickup_zone_id",
        "trip_distance": "avg_trip_distance",
        "fare_amount": "avg_fare",
        "temperature_mean": "temperature",
        "precipitation_sum": "rainfall"
    })

    print("\n=== ZONE-HOUR AGGREGATION (PICKUP) SUMMARY ===")
    print(f"Shape: {pickup_hour_df.shape}")
    return pickup_hour_df

# version 2: by pickup-dropoff pair 
def aggregate_pair_features(df):
    df = df.copy()
    df["hour_ts"] = df["pickup_datetime"].dt.floor("h")
    df["pickup_dropoff_pair"] = (
        df["pulocationid"].astype(str) + "_" + df["dolocationid"].astype(str)
    )
 
    agg_dict = {
        "trip_distance": "mean",
        "fare_amount": "mean",
        "temperature_mean": "mean",
        "precipitation_sum": "mean",
        "hour": "first",
        "day_of_week": "first",
        "month": "first",
        "hour_sin": "first",
        "hour_cos": "first",
        "dow_sin": "first",
        "dow_cos": "first",
        "is_weekend": "first",
        "is_peak_hour": "first",
        "borough": "first",
        "pulocationid": "first",
        "dolocationid": "first",
    }
    if "extreme_weather_flag" in df.columns:
        agg_dict["extreme_weather_flag"] = "max"
    if "is_raining" in df.columns:
        agg_dict["is_raining"] = "max"
    if "rain_peak_interaction" in df.columns:
        agg_dict["rain_peak_interaction"] = "max"
 
    pair_hour_df = df.groupby(["pickup_dropoff_pair", "hour_ts"]).agg(agg_dict).reset_index()
 
    demand_counts = df.groupby(["pickup_dropoff_pair", "hour_ts"]).size().reset_index(name="demand")
    pair_hour_df = pair_hour_df.merge(demand_counts, on=["pickup_dropoff_pair", "hour_ts"], how="left")
 
    pair_hour_df = pair_hour_df.rename(columns={
        "pulocationid": "pickup_zone_id",
        "dolocationid": "dropoff_zone_id",
        "trip_distance": "avg_trip_distance",
        "fare_amount": "avg_fare",
        "temperature_mean": "temperature",
        "precipitation_sum": "rainfall",
    })
 
    print("\n=== ZONE-HOUR AGGREGATION (PICKUP-DROPOFF PAIR) AGGREGATION ===")
    print(f"Shape: {pair_hour_df.shape}")
    print(f"Unique pairs: {pair_hour_df['pickup_dropoff_pair'].nunique():,}")
    return pair_hour_df

def engineer_lag_features(df, group_col):
    df = df.copy()
    df = df.sort_values([group_col, "hour_ts"])
    df["demand_lag_1h"]  = df.groupby(group_col)["demand"].shift(1)
    df["demand_lag_24h"] = df.groupby(group_col)["demand"].shift(24)
    df["demand_lag_168h"] = df.groupby(group_col)["demand"].shift(168)
    df["rolling_mean_7d"] = df.groupby(group_col)["demand"].transform(
        lambda x: x.shift(1).rolling(24 * 7, min_periods=24).mean()
    )
    return df

def handle_missing_lag_values(df):
    df = df.copy()
    initial_rows = len(df)
    df = df.dropna(subset=["demand_lag_1h", "demand_lag_24h", "demand_lag_168h", "rolling_mean_7d"])
    removed_rows = initial_rows - len(df)
    print("\n=== LAG FEATURE SUMMARY ===")
    print(f"Rows before dropping lag nulls: {initial_rows:,}")
    print(f"Rows after dropping lag nulls: {len(df):,}")
    print(f"Rows removed: {removed_rows:,}")
    return df

def print_summary_statistics(df, label):
    print(f"\n=== FEATURE ENGINEERING SUMMARY ({label}) ===")
    print(f"Final shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    numeric_cols = ["demand", "avg_trip_distance", "avg_fare", "temperature",
                    "rainfall", "demand_lag_1h", "demand_lag_24h", "rolling_mean_7d"]
    existing = [c for c in numeric_cols if c in df.columns]
    print(f"\nNumeric summary:")
    print(df[existing].describe())

def save_data(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"Saved to: {output_path}")


def main():
    # 1. load data
    df = load_data(INPUT_PATH)

    # 2. ensure datetime types
    df = ensure_datetime_columns(df)

    # 3. feature engineering
    df = engineer_time_features(df)
    df = engineer_trip_features(df)
    df = engineer_weather_features(df)
    df = engineer_interaction_features(df)

    # 4. clean anomalies based on EDA findings
    # df = clean_trip_data(df)

    # handle version 1: pickup only
    pickup_df = aggregate_pickup_features(df)
    pickup_df = engineer_lag_features(pickup_df, group_col="pickup_zone_id")
    pickup_df = handle_missing_lag_values(pickup_df)
    print_summary_statistics(pickup_df, "PICKUP")
    save_data(pickup_df, OUTPUT_PICKUP_PATH)

    # handle version 2: pickup-dropoff pair
    pair_df = aggregate_pair_features(df)
    pair_df = engineer_lag_features(pair_df, group_col="pickup_dropoff_pair")
    pair_df = handle_missing_lag_values(pair_df)
    print_summary_statistics(pair_df, "PICKUP-DROPOFF PAIR")
    save_data(pair_df, OUTPUT_PAIR_PATH)


if __name__ == "__main__":
    main()