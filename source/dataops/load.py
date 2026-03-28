import os
from pyspark.sql import SparkSession
from utils.config import PROCESSED_PATH
from utils.db import engine
from utils.monitoring import monitor
import logging

logger = logging.getLogger(__name__)

@monitor
def load_data(**context):
    execution_date = context["execution_date"]
    year = execution_date.year
    month = execution_date.month
    
    # spark = SparkSession.builder \
    #     .appName("Taxi ETL Load") \
    #     .config(
    #         "spark.jars.packages",
    #         "org.postgresql:postgresql:42.7.3"
    #     ) \
    #     .getOrCreate()
    spark = SparkSession.builder \
        .appName("Taxi ETL Load") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .getOrCreate()

    print(f"Loading data for {year}-{month:02d}")

    month_path = os.path.join(PROCESSED_PATH, "fact_trips", f"{year}-{month:02d}")

    if not os.path.exists(month_path):
        print(f"No processed data found at {month_path}")
        return

    df = spark.read.parquet(month_path)

    print(f"Loaded Spark DF with {df.count()} rows")

    jdbc_url = f"jdbc:postgresql://{engine.url.host}:{engine.url.port}/{engine.url.database}"
    
    # logging
    logger.info(f"Connecting to JDBC URL: {jdbc_url}")
    logger.info(f"Host: {engine.url.host}, Port: {engine.url.port}, DB: {engine.url.database}, User: {engine.url.username}")

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "fact_trips") \
        .option("user", engine.url.username) \
        .option("password", engine.url.password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print("Data appended to Supabase successfully")
    spark.stop()
    