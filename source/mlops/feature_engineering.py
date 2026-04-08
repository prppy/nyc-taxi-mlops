import os
import numpy as np
import pandas as pd

BASE_PATH = "data/processed"
EDA_JOINED_PATH = os.path.join(BASE_PATH, "eda", "joined")

INPUT_PICKUP_PATH = os.path.join(EDA_JOINED_PATH, "pickup.parquet")
# INPUT_PAIR_PATH = os.path.join(EDA_JOINED_PATH, "pair.parquet")

OUTPUT_PATH = os.path.join(BASE_PATH, "feature_engineered")
os.makedirs(OUTPUT_PATH, exist_ok=True)

OUTPUT_PICKUP_PATH = os.path.join(OUTPUT_PATH, "pickup_features.parquet")
# OUTPUT_PAIR_PATH = os.path.join(OUTPUT_PATH, "pair_features.parquet")


def load_joined_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise ValueError(f"Path does not exist: {path}")

    df = pd.read_parquet(path)
    print(f"Loaded: {path}")
    print(f"Shape : {df.shape[0]:,} rows x {df.shape[1]:,} cols")
    return df


def prepare_base_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["hour_ts"] = pd.to_datetime(df["hour_ts"], errors="coerce")
    df = df.dropna(subset=["hour_ts"]).copy()

    # temporal fields
    df["hour"] = df["hour_ts"].dt.hour
    df["day_of_week"] = (df["hour_ts"].dt.dayofweek + 1) % 7   # 0=Sun, 1=Mon, ..., 6=Sat
    df["month"] = df["hour_ts"].dt.month
    df["day"] = df["hour_ts"].dt.day
    df["weekofyear"] = df["hour_ts"].dt.isocalendar().week.astype(int)
    df["is_weekend"] = df["day_of_week"].isin([0, 6]).astype(int)

    # Cyclical encoding
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    df["dayofweek_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dayofweek_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    # Time buckets
    df["is_peak_hour"] = df["hour"].isin([17, 18, 19]).astype(int)
    df["is_night"] = df["hour"].isin([0, 1, 2, 3, 4, 5]).astype(int)

    return df


def add_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "precipitation_sum" in df.columns:
        df["precipitation_sum"] = pd.to_numeric(df["precipitation_sum"], errors="coerce").fillna(0)
        df["is_rainy"] = (df["precipitation_sum"] > 0).astype(int)
        df["is_heavy_rain"] = (df["precipitation_sum"] >= 10).astype(int)
    else:
        df["is_rainy"] = 0
        df["is_heavy_rain"] = 0

    if "temperature_mean" in df.columns:
        df["temperature_mean"] = pd.to_numeric(df["temperature_mean"], errors="coerce")

        # fill by timestamp first — all boroughs share same temp at a given hour
        df["temperature_mean"] = df.groupby("hour_ts")["temperature_mean"].transform(
            lambda x: x.fillna(x.median())
        )

        # fallback for any remaining NaNs
        df["temperature_mean"] = df["temperature_mean"].fillna(df["temperature_mean"].median())

        df["is_cold"] = (df["temperature_mean"] < 5).astype(int)
        df["is_hot"] = (df["temperature_mean"] > 25).astype(int)
    else:
        df["temperature_mean"] = np.nan
        df["is_cold"] = 0
        df["is_hot"] = 0

    df["extreme_weather_flag"] = (
        df["is_cold"] | df["is_hot"] | df["is_heavy_rain"]
    ).astype(int)

    return df


def add_grouped_lag_features(
    df: pd.DataFrame,
    group_cols: list[str],
    target_col: str = "demand"
) -> pd.DataFrame:
    df = df.copy()
    df["hour_ts"] = pd.to_datetime(df["hour_ts"])
    df = df.sort_values(group_cols + ["hour_ts"]).reset_index(drop=True)

    grouped = df.groupby(group_cols, dropna=False)[target_col]

    # lag features
    for lag in [1, 2, 24, 168]:
        df[f"{target_col}_lag_{lag}h"] = grouped.shift(lag)

    # rolling mean features using past data only
    shifted_col = f"_{target_col}_shifted"
    df[shifted_col] = grouped.shift(1)

    for window in [3, 24]:
        df[f"{target_col}_rolling_mean_{window}h"] = (
            df.groupby(group_cols, dropna=False)[shifted_col]
              .transform(lambda s: s.rolling(window=window, min_periods=1).mean())
        )

    df = df.drop(columns=[shifted_col])

    return df


def encode_location_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "borough" in df.columns:
        borough_dummies = pd.get_dummies(df["borough"], prefix="borough")
        df = pd.concat([df, borough_dummies], axis=1)

    if "service_zone" in df.columns:
        service_zone_dummies = pd.get_dummies(df["service_zone"], prefix="service_zone")
        df = pd.concat([df, service_zone_dummies], axis=1)

    return df


def final_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # keep current row target
    df["target_demand"] = df["demand"]

    # replace inf values from pct_change with NaN
    df = df.replace([np.inf, -np.inf], np.nan)

    # sort for readability
    sort_cols = [c for c in ["pulocationid", "dolocationid", "hour_ts"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    return df


def engineer_features(df: pd.DataFrame, version: str = "pickup") -> pd.DataFrame:
    df = df.copy()
    
    # drop lineage / audit-only columns before feature creation
    cols_to_drop = ["row_fingerprint"]
    existing_cols_to_drop = [c for c in cols_to_drop if c in df.columns]
    if existing_cols_to_drop:
        df = df.drop(columns=existing_cols_to_drop)
        
    df = prepare_base_features(df)
    df = add_weather_features(df)

    if version == "pickup":
        group_cols = ["pulocationid"]
    elif version == "pair":
        group_cols = ["pulocationid", "dolocationid"]
    else:
        raise ValueError("version must be either 'pickup' or 'pair'")

    df = add_grouped_lag_features(df, group_cols=group_cols, target_col="demand")
    df = encode_location_features(df)
    df = final_cleaning(df)

    return df


def print_summary(df: pd.DataFrame, label: str) -> None:
    print(f"\n=== FEATURE ENGINEERING SUMMARY ({label}) ===")
    print(f"Shape   : {df.shape[0]:,} rows x {df.shape[1]:,} cols")
    print(f"Columns : {list(df.columns)}")

    key_cols = [
        "target_demand",
        "demand_lag_1h",
        "demand_lag_24h",
        "demand_rolling_mean_24h",
        "avg_trip_distance",
        "avg_total_amount",
        "temperature_mean",
        "precipitation_sum",
    ]
    existing = [c for c in key_cols if c in df.columns]

    if existing:
        print("\nNumeric summary:")
        print(df[existing].describe())


def main():
    print("\n" + "=" * 60)
    print("FEATURE ENGINEERING - VERSION 1: PICKUP ZONE")
    print("=" * 60)

    pickup_df = load_joined_data(INPUT_PICKUP_PATH)
    pickup_features = engineer_features(pickup_df, version="pickup")
    print_summary(pickup_features, "PICKUP")

    pickup_features.to_parquet(OUTPUT_PICKUP_PATH, index=False)
    print(f"\nSaved: {OUTPUT_PICKUP_PATH}")

    """
    print("\n" + "=" * 60)
    print("FEATURE ENGINEERING - VERSION 2: PICKUP-DROPOFF PAIR")
    print("=" * 60)

    pair_df = load_joined_data(INPUT_PAIR_PATH)
    pair_features = engineer_features(pair_df, version="pair")
    print_summary(pair_features, "PICKUP-DROPOFF PAIR")

    pair_features.to_parquet(OUTPUT_PAIR_PATH, index=False)
    print(f"\nSaved: {OUTPUT_PAIR_PATH}")
    """

    print("\nFeature engineering completed.")

if __name__ == "__main__":
    main()