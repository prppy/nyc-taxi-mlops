# paths
RAW_PATH = "data/raw/"
PROCESSED_PATH = "data/processed/"

# urls
NYCTAXI_URL = "https://d37ci6vzurychx.cloudfront.net"

DATASETS = {
    "yellow": "yellow_tripdata",
    "fhvhv": "fhvhv_tripdata"
}

TAXI_TYPES = list(DATASETS.keys())
BOROUGH_COORDS = {
    "Bronx": (40.8448, -73.8648),
    "Brooklyn": (40.6782, -73.9442),
    "Manhattan": (40.7831, -73.9712),
    "Queens": (40.7282, -73.7949),
    "Staten Island": (40.5795, -74.1502),
    "EWR": (40.6895, -74.1745)
}

# dates
START_YEAR, START_MONTH = 2023, 3
TEST_YEAR = 2025

# table names
FACT_TABLE = "fact_demand"

DIM_TABLES = {
    "zone": "dim_zone",
    "weather": "dim_weather"
}

ZONE_LOOKUP_FILE = f"{RAW_PATH}taxi_zone_lookup.csv"

# unwanted location ids
EXCLUDED_LOCATION_IDS = [264, 265]

# airflow dag configs
DAG_ID = "taxi_data_pipeline"
SCHEDULE_INTERVAL = "0 0 L * *" # "@monthly"   # or "@daily"
RETRY_COUNT = 0

# logging
LOG_LEVEL = "INFO"

# helpers
def get_raw_file_path(dataset, year, month):
    return f"{RAW_PATH}taxi/{dataset}_tripdata_{year}-{month:02d}.parquet"

def get_processed_fact_path():
    return f"{PROCESSED_PATH}fact_demand/"

def get_month_year(execution_date):
    from dateutil.relativedelta import relativedelta
    target_date = execution_date - relativedelta(months=2)

    year = target_date.year
    month = target_date.month

    return (year, month)