# import os
# from pyspark.sql import SparkSession
# from utils.config import PROCESSED_PATH
# from utils.db import engine
# from utils.monitoring import monitor
# import logging

# logger = logging.getLogger(__name__)

# @monitor
# def load_data(**context):
#     execution_date = context["execution_date"]
#     year = execution_date.year
#     month = execution_date.month
    
#     # spark = SparkSession.builder \
#     #     .appName("Taxi ETL Load") \
#     #     .config(
#     #         "spark.jars.packages",
#     #         "org.postgresql:postgresql:42.7.3"
#     #     ) \
#     #     .getOrCreate()
#     spark = SparkSession.builder \
#         .appName("Taxi ETL Load") \
#         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
#         .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
#         .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
#         .getOrCreate()

#     logger.info(f"Starting load for {year}-{month:02d}")

#     month_path = os.path.join(PROCESSED_PATH, "fact_trips", f"{year}-{month:02d}")

#     if not os.path.exists(month_path):
#         logger.warning(f"No processed fact_trips data found at {month_path}")
#         return

#     df = spark.read.parquet(month_path)

#     row_count = df.count()
#     logger.info(f"Loaded fact_trips Spark DF with {row_count} rows")

#     jdbc_url = f"jdbc:postgresql://{engine.url.host}:{engine.url.port}/{engine.url.database}"
    
#     # logging
#     logger.info(f"Connecting to JDBC URL: {jdbc_url}")
#     logger.info(f"Host: {engine.url.host}, Port: {engine.url.port}, DB: {engine.url.database}, User: {engine.url.username}")

#     df.printSchema()
#     df.write \
#         .format("jdbc") \
#         .option("url", jdbc_url) \
#         .option("dbtable", "fact_trips") \
#         .option("user", engine.url.username) \
#         .option("password", engine.url.password) \
#         .option("driver", "org.postgresql.Driver") \
#         .option("batchsize", 10000) \
#         .option("numPartitions", 4) \
#         .mode("append") \
#         .save()
    
#     logger.info("fact_trips appended to Supabase successfully")

#     # load weather data in dim_weather
#     weather_path = os.path.join(PROCESSED_PATH, "dim_weather", f"{year}-{month:02d}")

#     if not os.path.exists(weather_path):
#         logger.warning(f"No processed dim_weather data found at {weather_path}")
#     else:
#         weather_df = spark.read.parquet(weather_path)

#         weather_count = weather_df.count()
#         logger.info(f"Loaded dim_weather Spark DF with {weather_count} rows")

#         weather_df.write \
#             .format("jdbc") \
#             .option("url", jdbc_url) \
#             .option("dbtable", "dim_weather") \
#             .option("user", engine.url.username) \
#             .option("password", engine.url.password) \
#             .option("driver", "org.postgresql.Driver") \
#             .mode("append") \
#             .save()

#         logger.info("dim_weather appended to Supabase successfully")

#     logger.info(f"Load completed for {year}-{month:02d}")
#     spark.stop()

# TODO: temporary lighter pandas version 

import os
import pandas as pd
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

    logger.info(f"Starting load for {year}-{month:02d}")

    # load fact_trips
    month_path = os.path.join(PROCESSED_PATH, "fact_trips", f"{year}-{month:02d}")
    if not os.path.exists(month_path):
        logger.warning(f"No processed fact_trips data found at {month_path}")
        return

    df = pd.read_parquet(month_path)
    logger.info(f"Loaded fact_trips with {len(df)} rows")

    df.to_sql(
        "fact_trips",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi"
    )
    logger.info("fact_trips appended to Supabase successfully")

    # load dim_weather
    weather_path = os.path.join(PROCESSED_PATH, "dim_weather", f"{year}-{month:02d}")
    if not os.path.exists(weather_path):
        logger.warning(f"No processed dim_weather data found at {weather_path}")
    else:
        weather_df = pd.read_parquet(weather_path)
        logger.info(f"Loaded dim_weather with {len(weather_df)} rows")

        weather_df.to_sql(
            "dim_weather",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi"
        )
        logger.info("dim_weather appended to Supabase successfully")

    logger.info(f"Load completed for {year}-{month:02d}")