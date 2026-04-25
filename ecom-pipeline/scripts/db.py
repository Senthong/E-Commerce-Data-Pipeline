import psycopg2
import os

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "ecom_db"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
        port=os.getenv("DB_PORT", 5432),
    )
