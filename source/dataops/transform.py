import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when
from utils.config import PROCESSED_PATH, get_raw_file_path
from utils.monitoring import monitor

STANDARD_TRIP_FACT_COLUMNS = [
    "pickup_datetime", "dropoff_datetime", "pulocationid", "dolocationid", "trip_distance", "fare_amount", "total_amount",
    "taxi_type", "vendor_id", "hvfhs_license_num", "dispatching_base_num", "originating_base_num", "passenger_count", "ratecode_id",
    "request_datetime", "on_scene_datetime", "store_and_fwd_flag", "payment_type", "trip_time", "shared_request_flag", "extra",
    "mta_tax", "improvement_surcharge", "wav_request_flag", "wav_match_flag", "tip_amount", "tolls_amount", "driver_pay",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "bcf", "sales_tax", "access_a_ride_flag", "shared_match_flag"
]

@monitor
def transform_fact(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month
    
    # skip if already processed
    fact_path = os.path.join(PROCESSED_PATH, "fact_trips", f"{year}-{month:02d}")
    if os.path.exists(fact_path):
        # check if folder has actual parquet files
        files = os.listdir(fact_path)
        parquet_files = [
            f for f in os.listdir(fact_path)
            if f.startswith("part-") and f.endswith(".parquet")
        ]

        if parquet_files:
            print(f"Data already exists for {year}-{month:02d}, skipping transform")
            return
    
    spark = (
        SparkSession.builder
        .appName("Taxi ETL Transform")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )

    print(f"Transforming fact table for {year}-{month:02d}")
    
    yellow_path = get_raw_file_path("yellow", year, month)
    fhvhv_path = get_raw_file_path("fhvhv", year, month)

    dfs = []

    # read yellow data
    try:
        yellow = spark.read.parquet(yellow_path)
        yellow = yellow.withColumn("taxi_type", lit("yellow"))
        dfs.append(yellow)
        print(f"Loaded yellow: {yellow_path}")
    except Exception as e:
        print(f"Yellow missing: {e}")

    # read fhvhv data
    try:
        fhvhv = spark.read.parquet(fhvhv_path)
        fhvhv = fhvhv.withColumn("taxi_type", lit("fhvhv"))
        dfs.append(fhvhv)
        print(f"Loaded fhvhv: {fhvhv_path}")
    except Exception as e:
        print(f"FHVHV missing: {e}")

    if not dfs:
        print("No data found for this month → skipping transform")
        return
    
    # standardise each dataset before union
    standardised_dfs = []
    for df in dfs:
        taxi_type = df.schema["taxi_type"]

        rename_map = {
            "VendorID": "vendor_id",
            "RatecodeID": "ratecode_id",
            "PULocationID": "pulocationid",
            "DOLocationID": "dolocationid",
            "trip_miles": "trip_distance",
            "base_passenger_fare": "fare_amount",
            "tips": "tip_amount",
            "tolls": "tolls_amount"
        }

        for old, new in rename_map.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)

        # unify datetime columns
        df = df.withColumn(
                "pickup_datetime",
                coalesce(col("pickup_datetime"), col("tpep_pickup_datetime"))
            )

        df = df.withColumn(
                "dropoff_datetime",
                coalesce(col("dropoff_datetime"), col("tpep_dropoff_datetime"))
            )
        
        # calculating fhvhv total_amount
        if taxi_type == "fhvhv":
            df = df.withColumn(
                "total_amount",
                coalesce(col("fare_amount"), lit(0)) +
                coalesce(col("tolls_amount"), lit(0)) +
                coalesce(col("congestion_surcharge"), lit(0)) +
                coalesce(col("airport_fee"), lit(0)) +
                coalesce(col("cbd_congestion_fee"), lit(0))
            )

        for col_name in STANDARD_TRIP_FACT_COLUMNS:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None))

        df = df.select(STANDARD_TRIP_FACT_COLUMNS)
        standardised_dfs.append(df)

    # union datasets
    combined = standardised_dfs[0]
    for df in standardised_dfs[1:]:
        combined = combined.unionByName(df)

    ## data cleaning
    # critical fields
    combined = combined.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("pulocationid").isNotNull() &
        col("dolocationid").isNotNull()
    )

    # invalid trips
    combined = combined.filter(
        col("pickup_datetime") < col("dropoff_datetime")
    )

    # convert flags to boolean
    FLAG_COLUMNS = [
        "shared_request_flag", "shared_match_flag", "wav_request_flag", "wav_match_flag", "access_a_ride_flag", "store_and_fwd_flag"
    ]

    for c in FLAG_COLUMNS:
        combined = combined.withColumn(
            c,
            when(col(c) == "Y", True)
            .when(col(c) == "N", False)
            .otherwise(None)
        )

    # Write to trip fact table
    combined = combined.withColumn("year", lit(year))
    combined = combined.withColumn("month", lit(month))
    combined = combined.repartition(8)

    # append or overwrite?
    combined.write.mode("overwrite").parquet(fact_path)

    print("Fact table appended successfully")
    spark.stop()

@monitor 
def transform_dim_zone(**context):
    pass

@monitor
def transform_dim_weather(**context):
    pass