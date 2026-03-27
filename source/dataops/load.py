import os
from pyspark.sql import SparkSession
from utils.config import PROCESSED_PATH
from utils.db import engine

def load_data(**context):
    spark = SparkSession.builder \
        .appName("Taxi ETL Load") \
        .getOrCreate()

    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month

    print(f"Loading data for {year}-{month:02d}")

    month_path = os.path.join(PROCESSED_PATH, "fact_trips", f"{year}-{month:02d}")

    if not os.path.exists(month_path):
        print(f"No processed data found at {month_path}")
        return

    df = spark.read.parquet(month_path)

    print(f"Loaded Spark DF with {df.count()} rows")

    pdf = df.toPandas()
    pdf.to_sql(
        "fact_trips",
        engine,
        if_exists="append",
        index=False,
        chunksize=5000
    )

    print("Data appended to Supabase successfully")
    spark.stop()