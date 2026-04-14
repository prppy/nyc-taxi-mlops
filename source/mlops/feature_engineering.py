import os
import re
from dotenv import load_dotenv

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, hour, dayofweek, month, to_date,
    sin, cos, lag, avg, lower, trim
)

# CONFIG
DEV_MODE = True  # Set to False for full run, True for quick dev iterations 
SAMPLE_FRACTION = 1.0 # Use 1.0 for full data, <1.0 for sampling during dev
WRITE_MODE = "overwrite"  # "overwrite" for now, "append" later for DAG

# SPARK SETUP
spark = SparkSession.builder \
    .appName("Feature Engineering") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# DB CONFIG
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env")

match = re.match(r"postgresql://(.*):(.*)@(.*):(.*)/(.*)", DATABASE_URL)
if not match:
    raise ValueError("Invalid DATABASE_URL format")

user, password, host, port, db = match.groups()
DB_URL = f"jdbc:postgresql://{host}:{port}/{db}"
DB_PROPERTIES = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

# LOAD DATA
def load_fact():
    if DEV_MODE and SAMPLE_FRACTION < 1.0:
        percent = SAMPLE_FRACTION * 100
        query = f"""
        (
            SELECT *
            FROM fact_trips_pickup
            TABLESAMPLE BERNOULLI ({percent})
            WHERE pulocationid IS NOT NULL
        ) AS subquery
        """
        return spark.read.jdbc(
            url=DB_URL,
            table=query,
            properties=DB_PROPERTIES
        )

    return spark.read.jdbc(
        url=DB_URL,
        table=query,
        #table="fact_trips_pickup",
        column="pulocationid",
        lowerBound=1,
        upperBound=300,
        numPartitions=8,
        properties=DB_PROPERTIES
    )
    
def load_zone():
    return spark.read.jdbc(
        url=DB_URL,
        table="dim_zone",
        properties=DB_PROPERTIES
    ).dropDuplicates(["location_id"])

def load_weather():
    return spark.read.jdbc(
        url=DB_URL,
        table="dim_weather",
        properties=DB_PROPERTIES
    )

# JOIN
def join_all(fact, zone, weather):

    df = fact.join(
        zone.select("location_id", "borough", "zone", "service_zone"),
        fact.pulocationid == zone.location_id,
        "left"
    ).drop("location_id")

    df = df.withColumn("borough", lower(trim(col("borough"))))

    weather = weather \
        .withColumn("weather_date", to_date(col("date"))) \
        .withColumn("weather_borough", lower(trim(col("borough"))))

    weather = weather.select(
        "weather_date",
        "weather_borough",
        "temperature_mean",
        "precipitation_sum"
    )

    df = df.join(
        weather,
        (df.hour_ts.cast("date") == weather.weather_date) &
        (df.borough == weather.weather_borough),
        "left"
    ).drop("weather_date", "weather_borough")

    return df

# TIME FEATURES
def add_time_features(df):

    df = df.withColumn("hour_ts", col("hour_ts").cast("timestamp"))

    df = df.withColumn("hour", hour("hour_ts"))
    df = df.withColumn("day_of_week", (dayofweek("hour_ts") + 5) % 7)
    df = df.withColumn("month", month("hour_ts"))

    df = df.withColumn("is_weekend", col("day_of_week").isin([5, 6]).cast("int"))

    # cyclical encoding
    df = df.withColumn("hour_sin", sin(2 * 3.14159 * col("hour") / 24))
    df = df.withColumn("hour_cos", cos(2 * 3.14159 * col("hour") / 24))
    df = df.withColumn("dow_sin", sin(2 * 3.14159 * col("day_of_week") / 7))
    df = df.withColumn("dow_cos", cos(2 * 3.14159 * col("day_of_week") / 7))

    return df

# WEATHER FEATURES
def add_weather_features(df):

    df = df.withColumn("temperature_mean", col("temperature_mean").cast("double"))
    df = df.withColumn("precipitation_sum", col("precipitation_sum").cast("double"))

    df = df.fillna({
        "temperature_mean": 0,
        "precipitation_sum": 0
    })

    df = df.withColumn("is_rainy", (col("precipitation_sum") > 0).cast("int"))
    df = df.withColumn("is_heavy_rain", (col("precipitation_sum") >= 10).cast("int"))
    df = df.withColumn("is_hot", (col("temperature_mean") > 25).cast("int"))

    return df

# LAG FEATURES (FIXED ORDERING)
def add_lag_features(df):

    # ensure correct ordering
    df = df.repartition(32, "pulocationid").sortWithinPartitions("hour_ts")

    window_spec = Window.partitionBy("pulocationid").orderBy("hour_ts")

    for lag_val in [1, 2, 24]:
        df = df.withColumn(
            f"demand_lag_{lag_val}",
            lag("demand", lag_val).over(window_spec)
        )

    df = df.withColumn(
        "rolling_mean_3h",
        avg("demand").over(window_spec.rowsBetween(-3, -1))
    )

    return df

# ENCODING
def encode_location_features(df):

    boroughs = ["manhattan", "brooklyn", "queens", "bronx", "staten island", "ewr"]

    for b in boroughs:
        df = df.withColumn(f"borough_{b}", (col("borough") == b).cast("int"))

    df = df.withColumn("borough_nan", col("borough").isNull().cast("int"))

    service_zones = ["Yellow Zone", "Boro Zone", "Airports", "EWR"]

    for s in service_zones:
        clean = s.replace(" ", "_")
        df = df.withColumn(
            f"service_zone_{clean}",
            (col("service_zone") == s).cast("int")
        )

    df = df.withColumn("service_zone_nan", col("service_zone").isNull().cast("int"))

    return df.drop("borough", "service_zone")


# FINAL CLEAN
def final_clean(df):

    df = df.withColumn("target_demand", col("demand"))

    df = df.dropna(subset=[
        "demand_lag_1",
        "demand_lag_2",
        "demand_lag_24",
        "rolling_mean_3h"
    ])

    return df


# MAIN PIPELINE
def main(start_date, end_date):
    print("\n=== FEATURE ENGINEERING START ===")

    fact = load_fact(start_date, end_date)
    zone = load_zone()
    weather = load_weather()

    df = join_all(fact, zone, weather)
    df = add_time_features(df)
    df = add_weather_features(df)
    df = add_lag_features(df)
    df = encode_location_features(df)

    df = final_clean(df)

    # select final columns
    df = df.select(
        "pulocationid",
        "hour_ts",
        "target_demand",
        "hour",
        "day_of_week",
        "month",
        "is_weekend",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
        "temperature_mean",
        "precipitation_sum",
        "is_rainy",
        "is_heavy_rain",
        "is_hot",
        "demand_lag_1",
        "demand_lag_2",
        "demand_lag_24",
        "rolling_mean_3h",
        *[c for c in df.columns if c.startswith("borough_")],
        *[c for c in df.columns if c.startswith("service_zone_")]
    )

    # WRITE TO POSTGRES
    df.coalesce(8).write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "pickup_features") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 5000) \
        .mode(WRITE_MODE) \
        .save()

    print("\nFeature engineering completed and saved to Postgres.")


if __name__ == "__main__":
    main("2023-01-01", "2025-12-31")