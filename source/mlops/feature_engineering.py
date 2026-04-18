import os
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, hour, dayofweek, month, to_date,
    sin, cos, lag, avg, lower, trim
)

# CONFIG
DEV_MODE = False  # Set to False for full run, True for quick dev iterations 
SAMPLE_FRACTION = 1.0 # Use 1.0 for full data, <1.0 for sampling during dev
WRITE_MODE = "append"  # "overwrite" for now, "append" later for DAG

# SPARK SETUP
def get_spark():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Feature Engineering")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# DB CONFIG
def get_db_config():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in .env")

    match = re.match(r"postgresql://(.*):(.*)@(.*):(.*)/(.*)", database_url)
    if not match:
        raise ValueError("Invalid DATABASE_URL format")

    user, password, host, port, db = match.groups()

    db_url = f"jdbc:postgresql://{host}:{port}/{db}"
    db_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }

    return {
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "db": db,
        "db_url": db_url,
        "db_properties": db_properties,
    }

# LOAD DATA
def load_fact(spark, db_url, db_properties, start_date, end_date):
    query = f"""
    (
        SELECT *
        FROM fact_trips_pickup
        WHERE hour_ts >= '{start_date}'
        AND hour_ts <= '{end_date}'
        AND pulocationid IS NOT NULL
    ) AS subquery
    """
    if DEV_MODE and SAMPLE_FRACTION < 1.0:
        percent = SAMPLE_FRACTION * 100
        query = f"""
        (
            SELECT *
            FROM fact_trips_pickup
            TABLESAMPLE BERNOULLI ({percent})
            WHERE hour_ts >= '{start_date}'
            AND hour_ts <= '{end_date}'
            AND pulocationid IS NOT NULL
        ) AS subquery
        """
        return spark.read.jdbc(
            url=db_url,
            table=query,
            properties=db_properties
        )

    return spark.read.jdbc(
        url=db_url,
        table=query,
        column="pulocationid",
        lowerBound=1,
        upperBound=300,
        numPartitions=2,
        properties=db_properties
    )
    
def load_zone(spark, db_url, db_properties):
    return spark.read.jdbc(
        url=db_url,
        table="dim_zone",
        properties=db_properties
    ).dropDuplicates(["location_id"])

def load_weather(spark, db_url, db_properties, start_date, end_date):
    query = f"""
    (
        SELECT *
        FROM dim_weather
        WHERE date >= '{start_date}'
        AND date <= '{end_date}'
    ) AS subquery
    """
    return spark.read.jdbc(
        url=db_url,
        table=query,
        properties=db_properties
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
        "precipitation_sum",
        "wind_speed_max",
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
    df = df.withColumn("wind_speed_max", col("wind_speed_max").cast("double"))

    df = df.fillna({
        "temperature_mean": 0,
        "precipitation_sum": 0,
        "wind_speed_max": 0
    })

    df = df.withColumn("is_rainy", (col("precipitation_sum") > 0).cast("int"))
    df = df.withColumn("is_heavy_rain", (col("precipitation_sum") >= 10).cast("int"))
    df = df.withColumn("is_hot", (col("temperature_mean") > 25).cast("int"))

    return df

# LAG FEATURES (FIXED ORDERING)
def add_lag_features(df):

    # ensure correct ordering
    df = df.repartition(8, "pulocationid").sortWithinPartitions("hour_ts")

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

    spark = get_spark()
    try:
        db_config = get_db_config()
        db_url = db_config["db_url"]
        db_properties = db_config["db_properties"]
        user = db_config["user"]
        password = db_config["password"]

        fact = load_fact(spark, db_url, db_properties, start_date, end_date)
        zone = load_zone(spark, db_url, db_properties)
        weather = load_weather(spark, db_url, db_properties, start_date, end_date)

        df = join_all(fact, zone, weather)
        df = add_time_features(df)
        df = add_weather_features(df)
        df = add_lag_features(df)
        df = encode_location_features(df)
        df = final_clean(df)

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
            "wind_speed_max",
            "is_rainy",
            "is_heavy_rain",
            "is_hot",
            "demand_lag_1",
            "demand_lag_2",
            "demand_lag_24",
            "rolling_mean_3h",
            *[c for c in df.columns if c.startswith("borough_")],
            *[c for c in df.columns if c.startswith("service_zone_")],
        )

        # WRITE TO POSTGRES
        df.coalesce(2).write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "pickup_features") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", 5000) \
            .mode(WRITE_MODE) \
            .save()

        print("\nFeature engineering completed and saved to Postgres.")
    finally:
        spark.stop()

    print("\nFeature engineering completed and saved to Postgres.")

