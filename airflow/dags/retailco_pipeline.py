from __future__ import annotations
import os
import sys
from datetime import datetime
import pandas as pd
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/dags/current/airflow/dags/")
from pipeline import build_connection, cargar, extraer, transformar

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
        import math
        ruta = os.getenv("CSV_PATH", "/opt/airflow/data/sales_data_sample.csv")
        df = extraer(ruta)
        data = df.to_dict(orient="records")
        return [
            {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
            for row in data
        ]

    @task(task_id="t2_transformar", multiple_outputs=False)
    def t2_transformar(raw_data):
        df = pd.DataFrame(raw_data)
        transformed = transformar(df)
        return transformed.to_dict(orient="records")

    @task(task_id="t3_cargar", multiple_outputs=False)
    def t3_cargar(transformed_data):
        df = pd.DataFrame(transformed_data)
        with build_connection() as conn:
            cargar(df, conn)

    t1 = t1_extraer()
    t2 = t2_transformar(t1)
    t3 = t3_cargar(t2)

retailco_pipeline()
