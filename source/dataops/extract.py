from datetime import datetime
import os
import pandas as pd
import requests
from utils.config import (
    NYCTAXI_URL,
    BOROUGH_COORDS,
    DATASETS,
    RAW_PATH,
    RAW_WEATHER_PATH,
    get_raw_file_path
)
from utils.monitoring import monitor

@monitor
def download_file(url, output_path):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded: {output_path}")
        else:
            print(f"Failed ({response.status_code}): {url}")
            raise Exception(f"HTTP {response.status_code}: {url}")

    except Exception as e:
        print(f"Error downloading {url}: {e}")
        raise

@monitor
def extract_taxi(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    print(f"Extracting taxi data for {year}-{month:02d}")

    for dataset_name, dataset_prefix in DATASETS.items():
        file_name = f"{dataset_prefix}_{year}-{month:02d}.parquet"
        url = f"{NYCTAXI_URL}/trip-data/{file_name}"
        output_path = get_raw_file_path(dataset_name, year, month)

        # skip if already exists (idempotency)
        if os.path.exists(output_path):
            print(f"Skipping (exists): {output_path}")
            continue

        print(f"Downloading {url}")
        download_file(url, output_path)
        
@monitor
def extract_weather(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    print(f"Extracting weather data for {year}-{month:02d}")

    os.makedirs(RAW_WEATHER_PATH, exist_ok=True)
    file_name = f"{year}-{month:02d}.csv"
    output_path = os.path.join(RAW_WEATHER_PATH, file_name)

    # skip if already exists (idempotency)
    if os.path.exists(output_path):
        print(f"Skipping (exists): {output_path}")
        return

    # safe date range
    start_date = f"{year}-{month:02d}-01"

    if month == 12:
        end_date = f"{year}-12-31"
    else:
        next_month = datetime(year, month + 1, 1)
        end_date = (next_month - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    all_dfs = []
    for borough, (lat, lon) in BOROUGH_COORDS.items():
        print(f"Downloading weather for {borough} {year}-{month:02d}")

        url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date={start_date}"
            f"&end_date={end_date}"
            f"&daily=temperature_2m_mean,precipitation_sum,windspeed_10m_max"
            f"&timezone=America/New_York"
        )

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()

            daily = data.get("daily", {})
            if not daily or "time" not in daily:
                print(f"No daily weather data found for {borough}")
                continue

            df = pd.DataFrame({
                "date": daily["time"],
                "temperature_mean": daily["temperature_2m_mean"],
                "precipitation_sum": daily["precipitation_sum"],
                "wind_speed_max": daily["windspeed_10m_max"],
            })

            df["date"] = pd.to_datetime(df["date"])
            df["borough"] = borough
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            df["day_of_week"] = df["date"].dt.day_name()
            df["is_rainy"] = (df["precipitation_sum"].fillna(0) > 0).astype(int)

            # aggregate to borough-month-day_of_week
            grouped = (
                df.groupby(["borough", "year", "month", "day_of_week"], as_index=False)
                    .agg(
                        avg_temperature_mean=("temperature_mean", "mean"),
                        avg_precipitation_sum=("precipitation_sum", "mean"),
                        avg_wind_speed_max=("wind_speed_max", "mean"),
                        rainy_days_count=("is_rainy", "sum"),
                        num_days=("date", "count"),
                    )
            )
            all_dfs.append(grouped)

        except Exception as e:
            print(f"Error processing weather for {borough}: {e}")

    # combine all boroughs
    if not all_dfs:
        print("No weather data collected")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # enforce weekday ordering
    weekday_order = [
        "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday", "Sunday"
    ]
    combined_df["day_of_week"] = pd.Categorical(
        combined_df["day_of_week"],
        categories=weekday_order,
        ordered=True
    )

    combined_df = combined_df.sort_values(
        by=["borough", "year", "month", "day_of_week"]
    ).reset_index(drop=True)

    combined_df.to_csv(output_path, index=False)
    print(f"Saved aggregated weather file: {output_path}")
    
@monitor
def extract_lookup(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month
    
    url = f"{NYCTAXI_URL}/misc/taxi_zone_lookup.csv"
    output_path = os.path.join(RAW_PATH, "lookup", f"taxi_zone_lookup_{year}-{month:02d}.csv")

    # skip if already exists (idempotency)
    if os.path.exists(output_path):
        print(f"Skipping (exists): {output_path}")
        return

    print(f"Downloading {url}")
    download_file(url, output_path)