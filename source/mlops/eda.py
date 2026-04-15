import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, to_date, avg

# Spark Setup 
def get_spark():
    return (
        SparkSession.builder
        .appName("NYC Taxi EDA")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )


# DB Config
def get_db_config():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found")

    match = re.match(r"postgresql://(.*):(.*)@(.*):(.*)/(.*)", database_url)
    if not match:
        raise ValueError("DATABASE_URL is not in expected format")

    user, password, host, port, db = match.groups()

    db_url = f"jdbc:postgresql://{host}:{port}/{db}"
    db_properties = {

        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }

    return db_url, db_properties

# Loading of Tables
def load_fact(spark, db_url, db_properties):
    print("\nLoading fact_trips_pickup...")

    query = """
    (SELECT pulocationid, hour_ts, demand
        FROM fact_trips_pickup
        WHERE pulocationid IS NOT NULL
        ORDER BY random()
        LIMIT 1000000
    ) AS subquery
    """

    return spark.read.jdbc(
        url=db_url,
        table=query,
        properties=db_properties,
    )

def load_zone(spark, db_url, db_properties):
    print("Loading dim_zone...")
    return (
        spark.read.jdbc(
            url=db_url,
            table="dim_zone",
            properties=db_properties,
        )
        .dropDuplicates(["location_id"])
    )

def load_weather(spark, db_url, db_properties):
    print("Loading dim_weather...")
    return spark.read.jdbc(
        url=db_url,
        table="dim_weather",
        properties=db_properties,
    )


# Feature Engineering (for EDA segment)
def add_time_features(df):
    return df \
        .withColumn("hour_ts", col("hour_ts").cast("timestamp")) \
        .withColumn("hour", hour("hour_ts")) \
        .withColumn("day_of_week", (dayofweek("hour_ts") + 5) % 7) \
        .withColumn("month", month("hour_ts")) \
        .withColumn("pickup_date", to_date("hour_ts"))


# Joining of Tables
def join_data(fact, zone, weather):

    print("\nJoining zone...")
    df = fact.join(
        zone.select("location_id", "borough"),
        fact.pulocationid == zone.location_id,
        "left"
    ).drop("location_id")

    print("Joining weather...")
    weather = weather \
        .withColumn("weather_date", to_date(col("date"))) \
        .withColumnRenamed("borough", "weather_borough")

    df = df.join(
        weather,
        (df.pickup_date == weather.weather_date) &
        (df.borough == weather.weather_borough),
        "left"
    ).drop("weather_date", "weather_borough")

    print("Join completed")
    return df

# EDA
def run_eda(df):

    print("\n=== Running EDA ===")

    print("\nSchema:")
    df.printSchema()

    print("\nSample rows:")
    df.show(5)

    print("\nAvg demand by hour:")
    df.groupBy("hour") \
        .agg(avg("demand").alias("avg_demand")) \
        .orderBy("hour") \
        .show()

    print("\nAvg demand by day of week:")
    df.groupBy("day_of_week") \
        .agg(avg("demand").alias("avg_demand")) \
        .orderBy("day_of_week") \
        .show()

    print("\nAvg demand by borough:")
    df.groupBy("borough") \
        .agg(avg("demand").alias("avg_demand")) \
        .orderBy("avg_demand") \
        .show()

# MAIN
def main():
    print("\n=== STARTING EDA PIPELINE ===")
    
    spark = get_spark()
    try:
        db_url, db_properties = get_db_config()

        fact = load_fact(spark, db_url, db_properties).select(
            "pulocationid", "hour_ts", "demand"
        )
        zone = load_zone(spark, db_url, db_properties)
        weather = load_weather(spark, db_url, db_properties)

        fact = add_time_features(fact)

        df = join_data(fact, zone, weather)
        df = df.filter(
            (col("borough").isNotNull())
            & (~col("borough").isin("N/A", "Unknown"))
        )

        run_eda(df)

        print("\nEDA completed successfully.")
    finally:
        spark.stop()

    print("\nEDA completed successfully.")

if __name__ == "__main__":
    main()