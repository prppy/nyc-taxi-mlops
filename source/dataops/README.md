# dataops directory

if you want to reset postgres history (CAUTION): 
docker compose down -v

regular shutting down docker (persists over sessions):
docker compose down --remove-orphans

if you want backfill a long range wo stopping for failures:
docker exec -it airflow_scheduler airflow dags backfill taxi_data_pipeline \
  --start-date 2023-01-01 \
  --end-date 2024-12-01 \
  --continue-on-failures