import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, year, month, hour, dayofweek,
    unix_timestamp, count, avg, sum as spark_sum, coalesce
)
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, TimestampType, BooleanType, DateType)
import shutil
from utils.config import PROCESSED_PATH, EXCLUDED_LOCATION_IDS, get_raw_file_path
from utils.monitoring import monitor
from utils.watermark import apply_cryptographic_watermark

STANDARD_TRIP_FACT_COLUMNS = [
    "pickup_datetime", "dropoff_datetime", "pulocationid", "dolocationid",
    "trip_distance", "trip_time", "fare_amount", "tolls_amount", "tip_amount", 
    "airport_fee", "congestion_surcharge", "cbd_congestion_fee", "extra", 
    "total_amount"
]

@monitor
def transform_fact(**context):
    execution_date = context["execution_date"]
    year_val = execution_date.year
    month_val = execution_date.month

    fact_path = os.path.join(PROCESSED_PATH, "fact_trips", f"{year_val}-{month_val:02d}")

    # skip if already exists (idempotency)
    if os.path.exists(fact_path):
        parquet_files = [
            f for f in os.listdir(fact_path)
            if f.startswith("part-") and f.endswith(".parquet")
        ]
        if parquet_files:
            print(f"Data already exists for {year_val}-{month_val:02d}, skipping transform")
            return

    spark = (
        SparkSession.builder
        .appName("Taxi Fact Transform")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.extraClassPath", "/opt/airflow/jars/openlineage-spark-1.8.0.jar")
        .config("spark.executor.extraClassPath", "/opt/airflow/jars/openlineage-spark-1.8.0.jar")
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", "http://marquez-api:5000")
        .config("spark.openlineage.namespace", "taxi_etl_project")
        .getOrCreate()
    )

    print(f"Transforming aggregated pair-demand table for {year_val}-{month_val:02d}")

    yellow_path = get_raw_file_path("yellow", year_val, month_val)
    fhvhv_path = get_raw_file_path("fhvhv", year_val, month_val)

    dfs = []

    try:
        yellow = spark.read.parquet(yellow_path)
        yellow = yellow.withColumn("taxi_type", lit("yellow"))
        dfs.append(("yellow", yellow))
        print(f"Loaded yellow: {yellow_path}")
    except Exception as e:
        print(f"Yellow missing: {e}")

    try:
        fhvhv = spark.read.parquet(fhvhv_path)
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
            # renaming yellow columns
            "tpep_pickup_datetime" : "pickup_datetime",
            "tpep_dropoff_datetime" : "dropoff_datetime",
            "Airport_fee" : "airport_fee",
            
            # renaming fhvhv columns
            "trip_miles": "trip_distance",
            "base_passenger_fare": "fare_amount",
            "tolls": "tolls_amount",
            "tips": "tip_amount",
            
            # standardisation
            "PULocationID" : "pulocationid",
            "DOLocationID" : "dolocationid",
            
            # naming issue
            "congestion_surchage" : "congestion_surcharge"
        }
        
        for old, new in rename_map.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
                
        for c in STANDARD_TRIP_FACT_COLUMNS:
            if c not in df.columns:
                df = df.withColumn(c, lit(None))
                
        if taxi_type == "yellow":
            df = df.withColumn(
                "trip_time",
                unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")
            )
            df = df.withColumn(
                "extra", 
                coalesce(col("extra"), lit(0.0)) + 
                coalesce(col("mta_tax"), lit(0.0)) + 
                coalesce(col("improvement_surcharge"), lit(0.0))
            )
        else:
            df = df.withColumn(
                "extra", 
                coalesce(col("bcf"), lit(0.0)) + 
                coalesce(col("sales_tax"), lit(0.0))
            )
            
            df = df.withColumn(
                "total_amount", 
                coalesce(col("fare_amount"), lit(0.0)) +
                coalesce(col("tolls_amount"), lit(0.0)) +
                coalesce(col("congestion_surcharge"), lit(0.0)) +
                coalesce(col("cbd_congestion_fee"), lit(0.0)) + 
                coalesce(col("extra"), lit(0.0)) +
                coalesce(col("airport_fee"), lit(0.0)) + 
                coalesce(col("tip_amount"), lit(0.0))
            )

        df = df.select(*STANDARD_TRIP_FACT_COLUMNS)
        df = df.withColumn("year", year(col("pickup_datetime"))) \
            .withColumn("month", month(col("pickup_datetime")))
        standardised_dfs.append(df)
        
    # union datasets
    combined = standardised_dfs[0]
    for df in standardised_dfs[1:]:
        combined = combined.unionByName(df)
        
    # data cleaning
    combined = combined.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("pulocationid").isNotNull() &
        col("dolocationid").isNotNull() &
        (~col("pulocationid").isin(EXCLUDED_LOCATION_IDS)) &
        (~col("dolocationid").isin(EXCLUDED_LOCATION_IDS))
    )
    
    combined = combined.filter(col("pickup_datetime") < col("dropoff_datetime"))
    
    combined = combined \
        .withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType())) \
        .withColumn("dropoff_datetime", col("dropoff_datetime").cast(TimestampType())) \
        .withColumn("pulocationid", col("pulocationid").cast(IntegerType())) \
        .withColumn("dolocationid", col("dolocationid").cast(IntegerType())) \
        .withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
        .withColumn("trip_time", col("trip_time").cast(IntegerType())) \
        .withColumn("fare_amount", col("fare_amount").cast(DoubleType())) \
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        
    combined = combined.filter(
        col("trip_distance").isNull() | (col("trip_distance") >= 0)
    ).filter(
        col("fare_amount").isNull() | (col("fare_amount") >= 0)
    ).filter(
        col("total_amount").isNull() | (col("total_amount") >= 0)
    ).filter(
        col("trip_time").isNull() | (col("trip_time") > 0)
    )
    
    combined = combined \
        .withColumn("year", year(col("pickup_datetime"))) \
        .withColumn("month", month(col("pickup_datetime"))) \
        .withColumn("hour", hour(col("pickup_datetime"))) \
        .withColumn("day_of_week", dayofweek(col("pickup_datetime")))
    
    aggregated = combined.groupBy(
        "pulocationid",
        "dolocationid",
        "year",
        "month",
        "day_of_week",
        "hour"
    ).agg(
        count("*").alias("num_trips"),

        avg("fare_amount").alias("avg_fare_amount"),
        spark_sum("fare_amount").alias("total_fare_amount"),

        avg("trip_distance").alias("avg_trip_distance"),
        # spark_sum("trip_distance").alias("total_trip_distance"),

        avg("trip_time").alias("avg_trip_time"),
        # spark_sum("trip_time").alias("total_trip_time"),

        avg("tolls_amount").alias("avg_tolls_amount"),
        spark_sum("tolls_amount").alias("total_tolls_amount"),

        avg("tip_amount").alias("avg_tip_amount"),
        spark_sum("tip_amount").alias("total_tip_amount"),

        avg("airport_fee").alias("avg_airport_fee"),
        spark_sum("airport_fee").alias("total_airport_fee"),

        avg("congestion_surcharge").alias("avg_congestion_surcharge"),
        spark_sum("congestion_surcharge").alias("total_congestion_surcharge"),

        avg("cbd_congestion_fee").alias("avg_cbd_congestion_fee"),
        spark_sum("cbd_congestion_fee").alias("total_cbd_congestion_fee"),

        avg("extra").alias("avg_extra"),
        spark_sum("extra").alias("total_extra"),
        
        avg("total_amount").alias("avg_total_amount"),
        # spark_sum("total_amount").alias("total_total_amount"),
    )
    
    aggregated = apply_cryptographic_watermark(aggregated)
    aggregated.write.mode("overwrite").parquet(fact_path)

    print("Aggregated pair-demand table saved successfully")
    spark.stop()
  
@monitor
def transform_dim_zone(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    spark = (
        SparkSession.builder
        .appName("Zone Dim Transform")
        .config("spark.driver.extraClassPath", "/opt/airflow/jars/openlineage-spark-1.8.0.jar")
        .config("spark.executor.extraClassPath", "/opt/airflow/jars/openlineage-spark-1.8.0.jar")
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", "http://marquez-api:5000")
        .config("spark.openlineage.namespace", "taxi_etl_project")
        .getOrCreate()
    )

    # Path to the lookup file downloaded in extract_lookup
    input_path = os.path.join("data/raw/lookup", f"taxi_zone_lookup_{year}-{month:02d}.csv")
    final_output_path = os.path.join("data/processed", "dim_zone.csv")
    temp_output_dir = os.path.join("data/processed", "_dim_zone_tmp")

    if not os.path.exists(input_path):
        print(f"Lookup file missing: {input_path}")
        spark.stop()
        return

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    # Standardize column names for SQL
    df = (
        df.withColumnRenamed("LocationID", "location_id")
          .withColumnRenamed("Borough", "borough")
          .withColumnRenamed("Zone", "zone")
          .withColumnRenamed("service_zone", "service_zone")
          .select("location_id", "borough", "zone", "service_zone")
          .dropDuplicates(["location_id"])
    )

    # Clean up old outputs
    if os.path.exists(temp_output_dir):
        shutil.rmtree(temp_output_dir)
    if os.path.exists(final_output_path):
        os.remove(final_output_path)

    # Write to temporary folder first
    (
        df.repartition(1)
          .write
          .mode("overwrite")
          .option("header", True)
          .csv(temp_output_dir)
    )

    # Find the actual Spark part file and rename it
    part_file = None
    for file_name in os.listdir(temp_output_dir):
        if file_name.startswith("part-") and file_name.endswith(".csv"):
            part_file = os.path.join(temp_output_dir, file_name)
            break

    if part_file is None:
        spark.stop()
        raise FileNotFoundError(f"No Spark part file found in {temp_output_dir}")

    os.rename(part_file, final_output_path)

    # Remove temp folder and leftover Spark metadata files
    shutil.rmtree(temp_output_dir)

    print(f"Dimension Zone table written to {final_output_path}")
    spark.stop()

@monitor
def transform_dim_weather(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    spark = (
        SparkSession.builder
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.extraClassPath", "/opt/airflow/jars/openlineage-spark-1.8.0.jar")
        .config("spark.executor.extraClassPath", "/opt/airflow/jars/openlineage-spark-1.8.0.jar")
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
    
    # convert day_of_week string to integer
    df = df.withColumn(
        "day_of_week",
        when(col("day_of_week") == "Monday", 2)
        .when(col("day_of_week") == "Tuesday", 3)
        .when(col("day_of_week") == "Wednesday", 4)
        .when(col("day_of_week") == "Thursday", 5)
        .when(col("day_of_week") == "Friday", 6)
        .when(col("day_of_week") == "Saturday", 7)
        .when(col("day_of_week") == "Sunday", 1)
        .otherwise(None)
    )

    # typecasting
    df = df \
        .withColumn("borough", col("borough")) \
        .withColumn("year", col("year").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("day_of_week", col("day_of_week").cast(IntegerType())) \
        .withColumn("avg_temperature_mean", col("avg_temperature_mean").cast(DoubleType())) \
        .withColumn("avg_precipitation_sum", col("avg_precipitation_sum").cast(DoubleType())) \
        .withColumn("avg_wind_speed_max", col("avg_wind_speed_max").cast(DoubleType())) \
        .withColumn("rainy_days_count", col("rainy_days_count").cast(IntegerType())) \
        .withColumn("num_days", col("num_days").cast(IntegerType()))
    
    df = df.dropDuplicates(["borough", "year", "month", "day_of_week"])

    # write to dim_weather table
    output_path = os.path.join(PROCESSED_PATH, "dim_weather", f"{year}-{month:02d}")
    df.write.mode("overwrite").parquet(output_path)

    print("dim_weather written successfully")
    spark.stop()