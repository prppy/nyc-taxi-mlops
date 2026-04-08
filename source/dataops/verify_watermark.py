import os
from utils.monitoring import monitor
from utils.watermark import verify_data_integrity
from pyspark.sql import SparkSession

#WATERMARK_FILE = "docs/watermark.json"

# @monitor
# def get_last_processed():
#     if not os.path.exists(WATERMARK_FILE):
#         return None

#     with open(WATERMARK_FILE, "r") as f:
#         return json.load(f)
    
@monitor
def validate_data_watermark(**context):
    execution_date = context["execution_date"]
    year, month = execution_date.year, execution_date.month
    
    # Path to the data saved by the transform task
    pickup_path = f"/opt/airflow/data/processed/fact_trips_pickup/{year}-{month:02d}"
    #pair_path = f"/opt/airflow/data/processed/fact_trips_pair/{year}-{month:02d}"
    
    spark = SparkSession.builder.appName("WatermarkVerification").getOrCreate()
    
    print(f"Auditing data integrity for {year}-{month:02d}...")
    
    try:
        if os.path.exists(pickup_path):
            print(f"Verifying pickup fact watermark: {pickup_path}")
            pickup_df = spark.read.parquet(pickup_path)
            verify_data_integrity(pickup_df)
        else:
            raise FileNotFoundError(f"Pickup fact path missing: {pickup_path}")
        '''
        if os.path.exists(pair_path):
            print(f"Verifying pair fact watermark: {pair_path}")
            pair_df = spark.read.parquet(pair_path)
            verify_data_integrity(pair_df)
        else:
            raise FileNotFoundError(f"Pair fact path missing: {pair_path}")
        '''
    finally:
        spark.stop()

# @monitor
# def update_watermark(**context):

#     execution_date = context["execution_date"].strftime("%Y-%m-%d %H:%M:%S")
    
#     watermark_content = {
#         "last_processed_timestamp": execution_date,
#         "last_run_status": "Success",
#         "updated_at": str(datetime.now())
#     }

#     os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
#     with open(WATERMARK_FILE, "w") as f:
#         json.dump(watermark_content, f)
    
#     print(f"Watermark updated to: {execution_date}")
