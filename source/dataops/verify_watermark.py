#import json
#import os
from utils.monitoring import monitor
from utils.watermark import verify_data_integrity
from datetime import datetime
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
    fact_path = f"/opt/airflow/data/processed/fact_trips/{year}-{month:02d}"
    
    spark = SparkSession.builder.appName("WatermarkVerification").getOrCreate()
    
    print(f"Auditing data integrity for {year}-{month:02d}...")
    
    try:
        df = spark.read.parquet(fact_path)
        # If this fails, the task turns RED and the DAG stops
        verify_data_integrity(df)
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
