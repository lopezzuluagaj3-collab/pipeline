from __future__ import annotations
import os
import sys
from datetime import datetime
import pandas as pd
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/dags/current/airflow/dags/")
from pipeline import build_connection, cargar, extraer, transformar

def _clean_records(df: pd.DataFrame) -> list:
    import math
    records = df.to_dict(orient="records")
    clean = []
    for row in records:
        clean_row = {}
        for k, v in row.items():
            if isinstance(v, float) and math.isnan(v):
                clean_row[k] = None
            elif isinstance(v, pd.Timestamp):
                clean_row[k] = v.isoformat()
            else:
                clean_row[k] = v
        clean.append(clean_row)
    return clean

@dag(
    dag_id="retailco_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retailco", "etl"],
)
def retailco_pipeline():

    @task(task_id="t1_extraer", multiple_outputs=False)
    def t1_extraer():
        ruta = os.getenv("CSV_PATH", "/opt/airflow/data/sales_data_sample.csv")
        df = extraer(ruta)
        return _clean_records(df)

    @task(task_id="t2_transformar", multiple_outputs=False)
    def t2_transformar(raw_data):
        df = pd.DataFrame(raw_data)
        transformed = transformar(df)
        return _clean_records(transformed)

    @task(task_id="t3_cargar", multiple_outputs=False)
    def t3_cargar(transformed_data):
        df = pd.DataFrame(transformed_data)
        # Restaurar tipos datetime que fueron serializados como string en XCom
        if "orderdate" in df.columns:
            df["orderdate"] = pd.to_datetime(df["orderdate"])
        with build_connection() as conn:
            cargar(df, conn)

    t1 = t1_extraer()
    t2 = t2_transformar(t1)
    t3 = t3_cargar(t2)

retailco_pipeline()
