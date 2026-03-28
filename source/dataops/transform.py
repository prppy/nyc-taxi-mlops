import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit
from utils.config import PROCESSED_PATH, get_raw_file_path
from utils.monitoring import monitor

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

    # combine datasets
    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    # unify pickup datetime
    pickup_cols = []

    if "pickup_datetime" in combined.columns:
        pickup_cols.append(col("pickup_datetime"))

    if "tpep_pickup_datetime" in combined.columns:
        pickup_cols.append(col("tpep_pickup_datetime"))

    if "lpep_pickup_datetime" in combined.columns:
        pickup_cols.append(col("lpep_pickup_datetime"))

    if pickup_cols:
        combined = combined.withColumn(
            "pickup_datetime", coalesce(*pickup_cols)
        )

    # unify dropoff datetime 
    dropoff_cols = []

    if "dropoff_datetime" in combined.columns:
        dropoff_cols.append(col("dropoff_datetime"))

    if "tpep_dropoff_datetime" in combined.columns:
        dropoff_cols.append(col("tpep_dropoff_datetime"))

    if "lpep_dropoff_datetime" in combined.columns:
        dropoff_cols.append(col("lpep_dropoff_datetime"))

    if dropoff_cols:
        combined = combined.withColumn(
            "dropoff_datetime", coalesce(*dropoff_cols)
        )

    # drop old redundant columns
    for c in [
        "tpep_pickup_datetime",
        "lpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "lpep_dropoff_datetime",
    ]:
        if c in combined.columns:
            combined = combined.drop(c)

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