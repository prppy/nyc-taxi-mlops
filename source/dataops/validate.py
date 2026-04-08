import os
import calendar
import hashlib
import pandas as pd
import pyarrow.parquet as pq
from utils.config import (
    RAW_PATH,
    PROCESSED_PATH,
    DATASETS,
    BOROUGH_COORDS,
    get_raw_file_path,
    get_month_year
)
from utils.monitoring import monitor

# ---------- expected schemas ----------
YELLOW_REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
}

FHVHV_REQUIRED_COLUMNS = {
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
}

WEATHER_REQUIRED_COLUMNS = {
    "date",
    "temperature_mean",
    "precipitation_sum",
    "wind_speed_max",
    "borough",
}

LOOKUP_REQUIRED_COLUMNS = {
    "LocationID",
    "Borough",
    "Zone",
    "service_zone",
}

PROCESSED_PICKUP_FACT_REQUIRED_COLUMNS = {
    "hour_ts",
    "pulocationid",
    "demand",
    "avg_trip_distance",
    "avg_total_amount",
    "row_fingerprint",
}

PROCESSED_PAIR_FACT_REQUIRED_COLUMNS = {
    "hour_ts",
    "pickup_dropoff_pair",
    "pulocationid",
    "dolocationid",
    "demand",
    "avg_trip_distance",
    "avg_total_amount",
    "row_fingerprint",
}

PROCESSED_WEATHER_REQUIRED_COLUMNS = WEATHER_REQUIRED_COLUMNS
PROCESSED_ZONE_REQUIRED_COLUMNS = {"location_id", "borough", "zone", "service_zone"}


def _fail(errors):
    if errors:
        raise ValueError("Validation failed:\n- " + "\n- ".join(errors))


def _warn(msgs):
    for msg in msgs:
        print(f"WARNING: {msg}")


def _month_days(year, month):
    return calendar.monthrange(year, month)[1]


def _current_lookup_path(year, month):
    return os.path.join(RAW_PATH, "lookup", f"taxi_zone_lookup_{year}-{month:02d}.csv")


def _previous_lookup_path(year, month):
    if month == 1:
        prev_year, prev_month = year - 1, 12
    else:
        prev_year, prev_month = year, month - 1
    path = _current_lookup_path(prev_year, prev_month)
    return path if os.path.exists(path) else None


def _file_md5(path):
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def _raw_taxi_checks(path, taxi_type, errors, warnings):
    try:
        pf = pq.ParquetFile(path)
    except Exception as e:
        errors.append(f"{taxi_type}: failed to open parquet metadata ({e})")
        return

    num_rows = pf.metadata.num_rows
    if num_rows == 0:
        errors.append(f"{taxi_type}: file is empty ({path})")
        return

    columns = set(pf.schema.names)

    required = YELLOW_REQUIRED_COLUMNS if taxi_type == "yellow" else FHVHV_REQUIRED_COLUMNS
    missing = required - columns
    if missing:
        errors.append(f"{taxi_type}: missing required columns {sorted(missing)}")
        return

    print(f"Raw {taxi_type} parquet metadata OK: rows={num_rows:,}, columns={len(columns)}")


def _raw_weather_checks(df, year, month, errors, warnings):
    if df.empty:
        errors.append("weather: file is empty")
        return

    missing = WEATHER_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"weather: missing required columns {sorted(missing)}")
        return

    expected_boroughs = set(BOROUGH_COORDS.keys())
    actual_boroughs = set(df["borough"].dropna().unique())
    missing_boroughs = expected_boroughs - actual_boroughs
    if missing_boroughs:
        errors.append(f"weather: missing boroughs {sorted(missing_boroughs)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isna().any():
        errors.append("weather: some dates could not be parsed")

    expected_days = _month_days(year, month)

    dupes = df.duplicated(subset=["date", "borough"]).sum()
    if dupes > 0:
        errors.append(f"weather: found {dupes} duplicate (date, borough) rows")

    for borough in sorted(expected_boroughs):
        borough_df = df[df["borough"] == borough]
        actual_days = borough_df["date"].dt.date.nunique()
        if actual_days != expected_days:
            errors.append(
                f"weather: borough {borough} has {actual_days} days, expected {expected_days}"
            )

    if (df["precipitation_sum"] < 0).fillna(False).any():
        errors.append("weather: precipitation_sum has negative values")

    if (df["wind_speed_max"] < 0).fillna(False).any():
        errors.append("weather: wind_speed_max has negative values")


def _lookup_checks(df, path, errors, warnings):
    if df.empty:
        errors.append(f"lookup: file is empty ({path})")
        return

    missing = LOOKUP_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"lookup: missing required columns {sorted(missing)}")
        return

    if df["LocationID"].isna().any():
        errors.append("lookup: LocationID contains nulls")

    dupes = df.duplicated(subset=["LocationID"]).sum()
    if dupes > 0:
        errors.append(f"lookup: found {dupes} duplicate LocationID rows")


def _check_lookup_changed(curr_path, prev_path, warnings):
    if not prev_path:
        warnings.append("lookup: no previous lookup file available for comparison")
        return

    curr_hash = _file_md5(curr_path)
    prev_hash = _file_md5(prev_path)

    if curr_hash == prev_hash:
        print("Lookup file unchanged from previous month")
    else:
        warnings.append(f"lookup: file changed compared with previous month ({prev_path})")

        curr_df = pd.read_csv(curr_path).sort_values("LocationID").reset_index(drop=True)
        prev_df = pd.read_csv(prev_path).sort_values("LocationID").reset_index(drop=True)

        curr_ids = set(curr_df["LocationID"].dropna().astype(int))
        prev_ids = set(prev_df["LocationID"].dropna().astype(int))

        added = sorted(curr_ids - prev_ids)
        removed = sorted(prev_ids - curr_ids)

        if added:
            warnings.append(f"lookup: added LocationIDs {added[:10]}{'...' if len(added) > 10 else ''}")
        if removed:
            warnings.append(f"lookup: removed LocationIDs {removed[:10]}{'...' if len(removed) > 10 else ''}")


def _processed_pickup_fact_checks(df, errors, warnings):
    if df.empty:
        errors.append("processed fact_trips_pickup: empty output")
        return

    missing = PROCESSED_PICKUP_FACT_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"processed fact_trips_pickup: missing columns {sorted(missing)}")
        return

    critical_cols = ["hour_ts", "pulocationid", "demand", "row_fingerprint"]
    nulls = df[critical_cols].isna().sum()
    for col, count in nulls.items():
        if count > 0:
            errors.append(f"processed fact_trips_pickup: {count} nulls in critical column {col}")

    df["hour_ts"] = pd.to_datetime(df["hour_ts"], errors="coerce")
    if df["hour_ts"].isna().any():
        errors.append("processed fact_trips_pickup: some hour_ts values could not be parsed")

    if (pd.to_numeric(df["demand"], errors="coerce") <= 0).fillna(False).any():
        errors.append("processed fact_trips_pickup: demand contains non-positive values")

    if (pd.to_numeric(df["avg_trip_distance"], errors="coerce") < 0).fillna(False).any():
        errors.append("processed fact_trips_pickup: avg_trip_distance has negative values")

    if (pd.to_numeric(df["avg_total_amount"], errors="coerce") < 0).fillna(False).any():
        warnings.append("processed fact_trips_pickup: avg_total_amount has negative values")

    dupes = df.duplicated(subset=["hour_ts", "pulocationid"]).sum()
    if dupes > 0:
        errors.append(f"processed fact_trips_pickup: found {dupes} duplicate (hour_ts, pulocationid) rows")

    if df["row_fingerprint"].isna().any():
        errors.append("processed fact_trips_pickup: row_fingerprint contains nulls")


'''
def _processed_pair_fact_checks(df, errors, warnings):
    if df.empty:
        errors.append("processed fact_trips_pair: empty output")
        return

    missing = PROCESSED_PAIR_FACT_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"processed fact_trips_pair: missing columns {sorted(missing)}")
        return

    critical_cols = ["hour_ts", "pickup_dropoff_pair", "pulocationid", "dolocationid", "demand", "row_fingerprint"]
    nulls = df[critical_cols].isna().sum()
    for col, count in nulls.items():
        if count > 0:
            errors.append(f"processed fact_trips_pair: {count} nulls in critical column {col}")

    df["hour_ts"] = pd.to_datetime(df["hour_ts"], errors="coerce")
    if df["hour_ts"].isna().any():
        errors.append("processed fact_trips_pair: some hour_ts values could not be parsed")

    if (pd.to_numeric(df["demand"], errors="coerce") <= 0).fillna(False).any():
        errors.append("processed fact_trips_pair: demand contains non-positive values")

    if (pd.to_numeric(df["avg_trip_distance"], errors="coerce") < 0).fillna(False).any():
        errors.append("processed fact_trips_pair: avg_trip_distance has negative values")

    if (pd.to_numeric(df["avg_total_amount"], errors="coerce") < 0).fillna(False).any():
        warnings.append("processed fact_trips_pair: avg_total_amount has negative values")

    dupes = df.duplicated(subset=["hour_ts", "pickup_dropoff_pair", "pulocationid", "dolocationid"]).sum()
    if dupes > 0:
        errors.append(f"processed fact_trips_pair: found {dupes} duplicate pair-hour rows")

    expected_pair = (
        pd.to_numeric(df["pulocationid"], errors="coerce").astype("Int64").astype(str)
        + "_"
        + pd.to_numeric(df["dolocationid"], errors="coerce").astype("Int64").astype(str)
    )
    mismatches = (df["pickup_dropoff_pair"].astype(str) != expected_pair).sum()
    if mismatches > 0:
        errors.append(
            f"processed fact_trips_pair: {mismatches} rows have inconsistent pickup_dropoff_pair values"
        )

    if df["row_fingerprint"].isna().any():
        errors.append("processed fact_trips_pair: row_fingerprint contains nulls")
'''


def _processed_weather_checks(df, year, month, errors, warnings):
    if df.empty:
        errors.append("processed dim_weather: empty output")
        return

    missing = PROCESSED_WEATHER_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"processed dim_weather: missing columns {sorted(missing)}")
        return

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    dupes = df.duplicated(subset=["date", "borough"]).sum()
    if dupes > 0:
        errors.append(f"processed dim_weather: found {dupes} duplicate (date, borough) rows")


def _processed_zone_checks(df, errors, warnings):
    if df.empty:
        errors.append("processed dim_zone: empty output")
        return

    missing = PROCESSED_ZONE_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"processed dim_zone: missing columns {sorted(missing)}")
        return

    if df["location_id"].isna().any():
        errors.append("processed dim_zone: location_id contains nulls")

    dupes = df.duplicated(subset=["location_id"]).sum()
    if dupes > 0:
        errors.append(f"processed dim_zone: found {dupes} duplicate location_id rows")


@monitor
def validate_raw(**context):
    execution_date = context["execution_date"]
    year, month = get_month_year(execution_date)
    # year = execution_date.year
    # month = execution_date.month

    errors = []
    warnings = []
    taxi_dfs = {}

    # taxi checks
    for taxi_type in DATASETS.keys():
        path = get_raw_file_path(taxi_type, year, month)
        if not os.path.exists(path):
            errors.append(f"{taxi_type}: raw taxi file missing ({path})")
            continue

        _raw_taxi_checks(path, taxi_type, errors, warnings)

    # weather checks
    weather_path = os.path.join(RAW_PATH, "weather", f"{year}-{month:02d}.csv")
    if not os.path.exists(weather_path):
        errors.append(f"weather: raw file missing ({weather_path})")
    else:
        try:
            weather_df = pd.read_csv(weather_path)
            print(f"Loaded raw weather rows: {len(weather_df):,}")
            _raw_weather_checks(weather_df, year, month, errors, warnings)
        except Exception as e:
            errors.append(f"weather: failed to read csv ({e})")

    # lookup checks
    lookup_path = _current_lookup_path(year, month)
    lookup_df = None
    if not os.path.exists(lookup_path):
        errors.append(f"lookup: raw file missing ({lookup_path})")
    else:
        try:
            lookup_df = pd.read_csv(lookup_path)
            print(f"Loaded raw lookup rows: {len(lookup_df):,}")
            _lookup_checks(lookup_df, lookup_path, errors, warnings)
            _check_lookup_changed(lookup_path, _previous_lookup_path(year, month), warnings)
        except Exception as e:
            errors.append(f"lookup: failed to read csv ({e})")

    # if taxi_dfs and lookup_df is not None and not lookup_df.empty:
    #     _taxi_lookup_crosscheck(taxi_dfs, lookup_df, errors, warnings)

    _warn(warnings)
    _fail(errors)
    print(f"Raw validation passed for {year}-{month:02d}")


@monitor
def validate_processed(**context):
    execution_date = context["execution_date"]
    year, month = get_month_year(execution_date)
    # year = execution_date.year
    # month = execution_date.month

    errors = []
    warnings = []

    pickup_df = None
    pair_df = None
    weather_df = None
    zone_df = None

    # version 1: pickup-only fact
    pickup_path = os.path.join(PROCESSED_PATH, "fact_trips_pickup", f"{year}-{month:02d}")
    if not os.path.exists(pickup_path):
        errors.append(f"processed fact_trips_pickup missing: {pickup_path}")
    else:
        try:
            pickup_df = pd.read_parquet(pickup_path)
            print(f"Loaded processed fact_trips_pickup rows: {len(pickup_df):,}")
            _processed_pickup_fact_checks(pickup_df, errors, warnings)
        except Exception as e:
            errors.append(f"processed fact_trips_pickup unreadable: {e}")

    '''
    # version 2: pickup-dropoff-pair fact
    pair_path = os.path.join(PROCESSED_PATH, "fact_trips_pair", f"{year}-{month:02d}")
    if not os.path.exists(pair_path):
        errors.append(f"processed fact_trips_pair missing: {pair_path}")
    else:
        try:
            pair_df = pd.read_parquet(pair_path)
            print(f"Loaded processed fact_trips_pair rows: {len(pair_df):,}")
            _processed_pair_fact_checks(pair_df, errors, warnings)
        except Exception as e:
            errors.append(f"processed fact_trips_pair unreadable: {e}")
    '''

    # weather
    weather_path = os.path.join(PROCESSED_PATH, "dim_weather", f"{year}-{month:02d}")
    if not os.path.exists(weather_path):
        errors.append(f"processed dim_weather missing: {weather_path}")
    else:
        try:
            weather_df = pd.read_parquet(weather_path)
            print(f"Loaded processed dim_weather rows: {len(weather_df):,}")
            _processed_weather_checks(weather_df, year, month, errors, warnings)
        except Exception as e:
            errors.append(f"processed dim_weather unreadable: {e}")

    # zone
    zone_path = os.path.join(PROCESSED_PATH, "dim_zone.csv")
    if not os.path.exists(zone_path):
        errors.append(f"processed dim_zone missing: {zone_path}")
    else:
        try:
            zone_df = pd.read_csv(zone_path)
            print(f"Loaded processed dim_zone rows: {len(zone_df):,}")
            _processed_zone_checks(zone_df, errors, warnings)
        except Exception as e:
            errors.append(f"processed dim_zone unreadable: {e}")

    # cross-check processed pickup fact IDs against processed dim_zone
    if pickup_df is not None and zone_df is not None and not pickup_df.empty and not zone_df.empty:
        valid_zone_ids = set(pd.to_numeric(zone_df["location_id"], errors="coerce").dropna().astype(int).unique())
        pickup_zone_ids = set(
            pd.to_numeric(pickup_df["pulocationid"], errors="coerce").dropna().astype(int).unique()
        )
        missing_pickup_zone_ids = sorted(pickup_zone_ids - valid_zone_ids)
        if missing_pickup_zone_ids:
            errors.append(
                f"processed cross-check: fact_trips_pickup contains zone ids not present in dim_zone: "
                f"{missing_pickup_zone_ids[:20]}{'...' if len(missing_pickup_zone_ids) > 20 else ''}"
            )

    '''
    # cross-check processed pair fact IDs against processed dim_zone
    if pair_df is not None and zone_df is not None and not pair_df.empty and not zone_df.empty:
        valid_zone_ids = set(pd.to_numeric(zone_df["location_id"], errors="coerce").dropna().astype(int).unique())
        pair_zone_ids = set(
            pd.concat([
                pd.to_numeric(pair_df["pulocationid"], errors="coerce"),
                pd.to_numeric(pair_df["dolocationid"], errors="coerce"),
            ]).dropna().astype(int).unique()
        )
        missing_pair_zone_ids = sorted(pair_zone_ids - valid_zone_ids)
        if missing_pair_zone_ids:
            errors.append(
                f"processed cross-check: fact_trips_pair contains zone ids not present in dim_zone: "
                f"{missing_pair_zone_ids[:20]}{'...' if len(missing_pair_zone_ids) > 20 else ''}"
            )
    '''

    _warn(warnings)
    _fail(errors)
    print(f"Processed validation passed for {year}-{month:02d}")