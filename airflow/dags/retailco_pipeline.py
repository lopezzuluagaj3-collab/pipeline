from __future__ import annotations
import os
import sys
import json
import math
from datetime import datetime
import pandas as pd
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/dags/current/airflow/dags/")
from pipeline import build_connection, cargar, extraer, transformar

DATA_DIR = "/opt/airflow/data"
T1_OUTPUT = os.path.join(DATA_DIR, "t1_raw.json")
T2_OUTPUT = os.path.join(DATA_DIR, "t2_transformed.json")

def _df_to_json(df: pd.DataFrame, path: str) -> None:
    records = df.to_dict(orient="records")
    clean = [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(clean, f)

def _json_to_df(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        return pd.DataFrame(json.load(f))

@dag(
    dag_id="retailco_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retailco", "etl"],
)
def retailco_pipeline():

    @task(task_id="t1_extraer", multiple_outputs=False)
    def t1_extraer() -> str:
        ruta = os.getenv("CSV_PATH", "/opt/airflow/data/sales_data_sample.csv")
        df = extraer(ruta)
        _df_to_json(df, T1_OUTPUT)
        return T1_OUTPUT

    @task(task_id="t2_transformar", multiple_outputs=False)
    def t2_transformar(t1_path: str) -> str:
        df = _json_to_df(t1_path)
        transformed = transformar(df)
        _df_to_json(transformed, T2_OUTPUT)
        return T2_OUTPUT

    @task(task_id="t3_cargar", multiple_outputs=False)
    def t3_cargar(t2_path: str) -> None:
        df = _json_to_df(t2_path)
        with build_connection() as conn:
            cargar(df, conn)

    t1 = t1_extraer()
    t2 = t2_transformar(t1)
    t3 = t3_cargar(t2)

retailco_pipeline()
