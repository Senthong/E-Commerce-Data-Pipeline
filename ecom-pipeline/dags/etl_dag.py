"""
etl_dag.py
DAG chính: chạy lúc 1:00 AM mỗi ngày
Flow: generate → staging → warehouse → marts
"""
import sys
sys.path.insert(0, "/opt/airflow/scripts")

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator

from generator import generate_daily_data
from staging import run_staging
from transform import run_transform
from marts import run_marts

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="ecom_etl_pipeline",
    default_args=default_args,
    description="Daily E-Commerce ETL: generate → stage → warehouse → marts",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 1 * * *",   # 1:00 AM every day
    catchup=False,
    tags=["ecom", "etl", "daily"],
) as dag:

    t1 = PythonOperator(
        task_id="generate_daily_data",
        python_callable=generate_daily_data,
        op_kwargs={"num_orders": 250},
        doc_md="Sinh ~250 đơn hàng giả lập cho ngày hôm nay bằng Faker",
    )

    t2 = PythonOperator(
        task_id="run_staging",
        python_callable=run_staging,
        op_kwargs={"run_date": date.today()},
        doc_md="Validate + clean raw data → staging layer với is_valid flag",
    )

    t3 = PythonOperator(
        task_id="load_warehouse",
        python_callable=run_transform,
        op_kwargs={"run_date": date.today()},
        doc_md="Load vào star schema: dim_customers (SCD2), dim_products, fct_orders",
    )

    t4 = PythonOperator(
        task_id="build_marts",
        python_callable=run_marts,
        op_kwargs={"run_date": date.today()},
        doc_md="Tính business metrics: daily revenue, product performance, customer segments",
    )

    t1 >> t2 >> t3 >> t4
