"""DAG пайплайна DWH: load_ods → transform_dds → build_dm (@daily)."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from clickhouse_sql import task_build_dm, task_load_ods, task_transform_dds

default_args = {
    "owner": "dwh",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="dwh_pipeline",
    description="PG→ODS→DDS→DM: три таска в одном DAG",
    start_date=datetime(2025, 8, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    is_paused_upon_creation=False,
    tags=["dwh", "pipeline"],
) as dag:

    load_ods = PythonOperator(
        task_id="load_ods",
        python_callable=task_load_ods,
    )
    transform_dds = PythonOperator(
        task_id="transform_dds",
        python_callable=task_transform_dds,
    )
    build_dm = PythonOperator(
        task_id="build_dm",
        python_callable=task_build_dm,
    )

    load_ods >> transform_dds >> build_dm
