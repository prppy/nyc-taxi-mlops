from extract import extract_taxi
from transform import transform_fact
from validate import validate_data
from load import load_to_postgres
from lineage import log_lineage
from watermark import update_watermark

INPUT_PATH = "data/raw/yellow_tripdata_2025-01.parquet"
OUTPUT_PATH = "data/processed/fact_trips"

def run_pipeline():
    print("Starting ETL Pipeline...")

    df = extract_taxi(INPUT_PATH)

    df_transformed = transform_fact(df)

    validate_data(df_transformed)

    df_transformed.write.mode("overwrite").parquet(OUTPUT_PATH)

    load_to_postgres(df_transformed, "fact_trips")

    log_lineage(INPUT_PATH, OUTPUT_PATH)

    update_watermark("2025-01")

    print("Pipeline completed!")

if __name__ == "__main__":
    run_pipeline()