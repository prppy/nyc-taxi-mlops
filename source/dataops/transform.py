import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from utils.config import PROCESSED_PATH, get_raw_file_path

def transform_fact(**context):
    execution_date = context["execution_date"]

    year = execution_date.year
    month = execution_date.month
    
    spark = SparkSession.builder \
        .appName("Taxi ETL Transform") \
        .getOrCreate()

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

    combined = combined.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                       .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")

    # append or overwrite?
    fact_path = os.path.join(PROCESSED_PATH, "fact_trips", f"{year}-{month:02d}")
    combined.write.mode("overwrite").parquet(fact_path)

    print("Fact table appended successfully")
    spark.stop()
    
def transform_dim_zone(**context):
    pass

def transform_dim_weather(**context):
    pass