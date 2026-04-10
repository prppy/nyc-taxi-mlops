import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, dayofmonth,
    weekofyear, sin, cos, when,
    lag, avg, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# ======================
# INIT
# ======================
spark = SparkSession.builder \
    .appName("FeatureEngineering") \
    .config("spark.jars", "/app/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

BASE_PATH = "/app/data/processed"
INPUT_PICKUP_PATH = f"{BASE_PATH}/eda/joined/pickup.parquet"
OUTPUT_PICKUP_PATH = f"{BASE_PATH}/feature_engineered/pickup_features"


# ======================
# LOAD DATA (PURE SPARK)
# ======================
def load_data(path):
    df = spark.read.parquet(path)
    print(f"Loaded {path}")
    print(f"Rows: {df.count()}, Columns: {len(df.columns)}")
    return df

# ======================
# BASE FEATURES
# ======================
def prepare_base_features(df):

    df = df.withColumn("hour_ts", col("hour_ts").cast("timestamp"))
    df = df.dropna(subset=["hour_ts"])

    df = df.withColumn("hour", hour("hour_ts")) \
        .withColumn("day_of_week", (dayofweek("hour_ts") % 7)) \
        .withColumn("month", month("hour_ts")) \
        .withColumn("day", dayofmonth("hour_ts")) \
        .withColumn("weekofyear", weekofyear("hour_ts"))

    df = df.withColumn("is_weekend",
        when(col("day_of_week").isin([0,6]), 1).otherwise(0)
    )

    # cyclical encoding
    df = df.withColumn("hour_sin", sin(2 * 3.14159 * col("hour") / 24)) \
        .withColumn("hour_cos", cos(2 * 3.14159 * col("hour") / 24)) \
        .withColumn("dayofweek_sin", sin(2 * 3.14159 * col("day_of_week") / 7)) \
        .withColumn("dayofweek_cos", cos(2 * 3.14159 * col("day_of_week") / 7)) \
        .withColumn("month_sin", sin(2 * 3.14159 * col("month") / 12)) \
        .withColumn("month_cos", cos(2 * 3.14159 * col("month") / 12))

    df = df.withColumn("is_peak_hour",
        when(col("hour").isin([17,18,19]), 1).otherwise(0)
    )

    df = df.withColumn("is_night",
        when(col("hour").isin([0,1,2,3,4,5]), 1).otherwise(0)
    )

    return df


# ======================
# WEATHER FEATURES
# ======================
def add_weather_features(df):
    df = df.withColumn("precipitation_sum", col("precipitation_sum").cast(DoubleType()))
    df = df.fillna({"precipitation_sum": 0})

    df = df.withColumn("temperature_mean", col("temperature_mean").cast(DoubleType()))

    # Fill missing temperature using median (approximate)
    median_temp = df.approxQuantile("temperature_mean", [0.5], 0.01)[0]
    df = df.fillna({"temperature_mean": median_temp})

    # BOOLEAN version (recommended)
    df = df.withColumn("is_rainy", col("precipitation_sum") > 0)
    df = df.withColumn("is_heavy_rain", col("precipitation_sum") >= 10)

    df = df.withColumn("is_cold", col("temperature_mean") < 5)
    df = df.withColumn("is_hot", col("temperature_mean") > 25)

    df = df.withColumn(
        "extreme_weather_flag",
        (
            col("is_cold") |
            col("is_hot") |
            col("is_heavy_rain")
        ).cast("int")
    )

    return df


# ======================
# LAG FEATURES
# ======================
def add_lag_features(df, group_cols):

    window_spec = Window.partitionBy(*group_cols).orderBy("hour_ts")

    for lag_val in [1, 2, 24, 168]:
        df = df.withColumn(
            f"demand_lag_{lag_val}h",
            lag("demand", lag_val).over(window_spec)
        )

    # rolling windows
    df = df.withColumn(
        "demand_rolling_mean_3h",
        avg("demand").over(window_spec.rowsBetween(-3, -1))
    )

    df = df.withColumn(
        "demand_rolling_mean_24h",
        avg("demand").over(window_spec.rowsBetween(-24, -1))
    )

    return df


# ======================
# ENCODING
# ======================
def encode_location(df):

    categorical_cols = ["borough", "service_zone"]

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in categorical_cols if c in df.columns
    ]

    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec")
        for c in categorical_cols if c in df.columns
    ]

    pipeline = Pipeline(stages=indexers + encoders)
    model = pipeline.fit(df)
    df = model.transform(df)

    return df


# ======================
# FINAL CLEAN
# ======================
def final_cleaning(df):

    df = df.withColumn("target_demand", col("demand"))
    df = df.dropna(subset=["target_demand"])
    df = df.fillna(0)

    return df


# ======================
# MAIN
# ======================
def main():

    df = load_data(INPUT_PICKUP_PATH)

    df = df.select(
        "hour_ts",
        "pulocationid",
        "demand",
        "avg_trip_distance",
        "avg_total_amount",
        "temperature_mean",
        "precipitation_sum",
        "wind_speed_max",
        "borough",
        "service_zone"
    )

    df = prepare_base_features(df)
    df = add_weather_features(df)
    df = add_lag_features(df, ["pulocationid"])
    df = encode_location(df)
    df = final_cleaning(df)

    df.write.mode("overwrite").parquet(OUTPUT_PICKUP_PATH)
    df = df.drop("borough_vec", "service_zone_vec")

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://airflow_postgres:5432/airflow") \
        .option("dbtable", "pickup_features") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Feature engineering completed.")


if __name__ == "__main__":
    main()