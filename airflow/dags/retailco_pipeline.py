from __future__ import annotations

import os
import sys
from datetime import datetime

import pandas as pd
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/dags/current/airflow/dags/")


from pipeline import build_connection, cargar, extraer, transformar


RAW_PATH = "/tmp/raw_sales.parquet"
TRANSFORMED_PATH = "/tmp/transformed_sales.parquet"


@dag(
    dag_id="retailco_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retailco", "etl"],
)
def retailco_pipeline():
    @task(task_id="t1_extraer")
    def t1_extraer() -> str:
        ruta = os.getenv("CSV_PATH", "/opt/airflow/data/new_data/sales_data_sample.csv")
        df = extraer(ruta)
        df.to_parquet(RAW_PATH, index=False)
        return RAW_PATH

    @task(task_id="t2_transformar")
    def t2_transformar(raw_path: str) -> str:
        df = pd.read_parquet(raw_path)
        transformed = transformar(df)
        transformed.to_parquet(TRANSFORMED_PATH, index=False)
        return TRANSFORMED_PATH

    @task(task_id="t3_cargar")
    def t3_cargar(transformed_path: str) -> None:
        df = pd.read_parquet(transformed_path)
        with build_connection() as conn:
            cargar(df, conn)

    t1 = t1_extraer()
    t2 = t2_transformar(t1)
    t3 = t3_cargar(t2)

    t1 >> t2 >> t3


retailco_pipeline()
