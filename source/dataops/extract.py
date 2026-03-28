from datetime import datetime
import os
import pandas as pd
import requests
from utils.config import (
    NYCTAXI_URL,
    BOROUGH_COORDS,
    DATASETS,
    RAW_PATH,
    get_raw_file_path
)

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

    except Exception as e:
        print(f"Error downloading {url}: {e}")

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
        
def extract_weather(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    print(f"Extracting weather data for {year}-{month:02d}")

    output_dir = os.path.join(RAW_PATH, "weather")
    os.makedirs(output_dir, exist_ok=True)

    file_name = f"{year}-{month:02d}.csv"
    output_path = os.path.join(output_dir, file_name)

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
            response = requests.get(url)

            if response.status_code != 200:
                print(f"Failed: {borough} {year}-{month:02d}")
                continue

            data = response.json()
            df = pd.DataFrame({
                "date": data["daily"]["time"],
                "temperature_mean": data["daily"]["temperature_2m_mean"],
                "precipitation_sum": data["daily"]["precipitation_sum"],
                "wind_speed_max": data["daily"]["windspeed_10m_max"]
            })

            df["borough"] = borough
            all_dfs.append(df)

        except Exception as e:
            print(f"Error processing weather for {borough}: {e}")

    # combine all boroughs
    if not all_dfs:
        print("No weather data collected")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.to_csv(output_path, index=False)

    print(f"Saved combined weather file: {output_path}")

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
