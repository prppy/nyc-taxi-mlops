from sqlalchemy import create_engine
import pandas as pd

DB_URI = "postgresql://airflow:airflow@airflow_postgres:5432/airflow"


def get_engine():
    return create_engine(DB_URI)


def load_features():
    engine = get_engine()

    query = """
    SELECT *
    FROM pickup_features
    """

    df = pd.read_sql(query, engine)

    print(f"Loaded from Postgres: {df.shape}")
    return df