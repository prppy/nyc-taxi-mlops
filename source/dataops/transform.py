import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, TimestampType, BooleanType, DateType)
from utils.config import PROCESSED_PATH, get_raw_file_path
from utils.monitoring import monitor
from utils.watermark import apply_cryptographic_watermark

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
        .config("spark.jars", "/opt/spark/jars/openlineage-spark-1.8.0.jar")
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", "http://marquez-api:5000")
        .config("spark.openlineage.namespace", "taxi_etl_project")
        .getOrCreate()
    )

    print(f"Transforming fact table for {year}-{month:02d}")
    
    yellow_path = get_raw_file_path("yellow", year, month)
    fhvhv_path = get_raw_file_path("fhvhv", year, month)

    dfs = []


    # read yellow data
    try:
        yellow = spark.read.parquet(yellow_path) 
        yellow = yellow.sample(fraction=0.001, seed=42) # sample 0.1%
        yellow = yellow.withColumn("taxi_type", lit("yellow"))
        dfs.append(("yellow", yellow))
        print(f"Loaded yellow: {yellow_path}")
    except Exception as e:
        print(f"Yellow missing: {e}")

    # read fhvhv data
    try:
        fhvhv = spark.read.parquet(fhvhv_path)
        fhvhv = fhvhv.sample(fraction=0.001, seed=42) # sample 0.1%
        fhvhv = fhvhv.withColumn("taxi_type", lit("fhvhv"))
        dfs.append(("fhvhv", fhvhv))
        print(f"Loaded fhvhv: {fhvhv_path}")
    except Exception as e:
        print(f"FHVHV missing: {e}")


    if not dfs:
        print("No data found for this month → skipping transform")
        return
    

    # standardise each dataset before union
    standardised_dfs = []
    for taxi_type, df in dfs:

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
                

        # parquet file had naming issues..
        if "congestion_surchage" in df.columns:
            df = df.withColumnRenamed("congestion_surchage", "congestion_surcharge")

        # fill all missing standard columns with None FIRST
        for col_name in STANDARD_TRIP_FACT_COLUMNS:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None))
        # unify datetime columns
        if taxi_type == "yellow":
            if "tpep_pickup_datetime" in df.columns:
                df = df.withColumn("pickup_datetime", col("tpep_pickup_datetime"))
            if "tpep_dropoff_datetime" in df.columns:
                df = df.withColumn("dropoff_datetime", col("tpep_dropoff_datetime"))

        elif taxi_type == "fhvhv":
           pass
        
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
    
    # typecasting
    combined = combined \
    .withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType())) \
    .withColumn("dropoff_datetime", col("dropoff_datetime").cast(TimestampType())) \
    .withColumn("pulocationid", col("pulocationid").cast(IntegerType())) \
    .withColumn("dolocationid", col("dolocationid").cast(IntegerType())) \
    .withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
    .withColumn("fare_amount", col("fare_amount").cast(DoubleType())) \
    .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
    .withColumn("taxi_type", col("taxi_type").cast(StringType())) \
    .withColumn("vendor_id", col("vendor_id").cast(LongType())) \
    .withColumn("hvfhs_license_num", col("hvfhs_license_num").cast(StringType())) \
    .withColumn("dispatching_base_num", col("dispatching_base_num").cast(StringType())) \
    .withColumn("originating_base_num", col("originating_base_num").cast(StringType())) \
    .withColumn("ratecode_id", col("ratecode_id").cast(StringType())) \
    .withColumn("payment_type", col("payment_type").cast(StringType())) \
    .withColumn("request_datetime", col("request_datetime").cast(TimestampType())) \
    .withColumn("on_scene_datetime", col("on_scene_datetime").cast(TimestampType())) \
    .withColumn("trip_time", col("trip_time").cast(LongType())) \
    .withColumn("extra", col("extra").cast(DoubleType())) \
    .withColumn("mta_tax", col("mta_tax").cast(DoubleType())) \
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType())) \
    .withColumn("tip_amount", col("tip_amount").cast(DoubleType())) \
    .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType())) \
    .withColumn("driver_pay", col("driver_pay").cast(DoubleType())) \
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType())) \
    .withColumn("airport_fee", col("airport_fee").cast(DoubleType())) \
    .withColumn("cbd_congestion_fee", col("cbd_congestion_fee").cast(DoubleType())) \
    .withColumn("bcf", col("bcf").cast(DoubleType())) \
    .withColumn("sales_tax", col("sales_tax").cast(DoubleType())) \
    .withColumn("shared_request_flag", col("shared_request_flag").cast(BooleanType())) \
    .withColumn("shared_match_flag", col("shared_match_flag").cast(BooleanType())) \
    .withColumn("wav_request_flag", col("wav_request_flag").cast(BooleanType())) \
    .withColumn("wav_match_flag", col("wav_match_flag").cast(BooleanType())) \
    .withColumn("access_a_ride_flag", col("access_a_ride_flag").cast(BooleanType())) \
    .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast(BooleanType()))

    combined = apply_cryptographic_watermark(combined)

    # write to trip fact table
    # combined = combined.withColumn("year", lit(year))
    # combined = combined.withColumn("month", lit(month))
    # TODO: temporary repartition
    combined = combined.repartition(1) # only one connection

    # append or overwrite?
    combined.write.mode("overwrite").parquet(fact_path)

    print("Fact table appended successfully")
    spark.stop()

@monitor 
def transform_dim_zone(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    spark = (
        SparkSession.builder
        .appName("Zone Dim Transform")
        .config("spark.jars", "/opt/spark/jars/openlineage-spark-1.8.0.jar")
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", "http://marquez-api:5000")
        .config("spark.openlineage.namespace", "taxi_etl_project")
        .getOrCreate()
    )
    
    # Path to the lookup file downloaded in extract_lookup
    input_path = os.path.join("data/raw/lookup", f"taxi_zone_lookup_{year}-{month:02d}.csv")
    output_path = os.path.join(PROCESSED_PATH, "dim_zone", f"{year}-{month:02d}")

    if not os.path.exists(input_path):
        print(f"Lookup file missing: {input_path}")
        return

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Standardize column names for SQL
    df = df.withColumnRenamed("LocationID", "location_id") \
           .withColumnRenamed("Borough", "borough") \
           .withColumnRenamed("Zone", "zone") \
           .withColumnRenamed("service_zone", "service_zone")

    # Final selection and cleaning
    df = df.select("location_id", "borough", "zone", "service_zone").dropDuplicates(["location_id"])

    df.repartition(1).write.mode("overwrite").parquet(output_path)
    print(f"Dimension Zone table written to {output_path}")
    spark.stop()

@monitor
def transform_dim_weather(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    spark = (
        SparkSession.builder
        .appName("Weather Transform")
        .config("spark.jars", "/opt/spark/jars/openlineage-spark-1.8.0.jar")
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", "http://marquez-api:5000")
        .config("spark.openlineage.namespace", "taxi_etl_project")
        .getOrCreate()
    )

    print(f"Transforming weather for {year}-{month:02d}")

    input_path = f"data/raw/weather/{year}-{month:02d}.csv"

    if not os.path.exists(input_path):
        print(f"No weather data found for {year}-{month:02d}")
        return

    df = spark.read.csv(input_path, header=True)

    # typecasting
    df = df \
        .withColumn("date", col("date").cast(DateType())) \
        .withColumn("temperature_mean", col("temperature_mean").cast(DoubleType())) \
        .withColumn("precipitation_sum", col("precipitation_sum").cast(DoubleType())) \
        .withColumn("wind_speed_max", col("wind_speed_max").cast(DoubleType())) \
        .withColumn("borough", col("borough"))
    
    df = df.dropDuplicates(["date", "borough"])

    # write to dim_weather table
    output_path = os.path.join(PROCESSED_PATH, "dim_weather", f"{year}-{month:02d}")
    df.write.mode("overwrite").parquet(output_path)

    print("dim_weather written successfully")
    spark.stop()