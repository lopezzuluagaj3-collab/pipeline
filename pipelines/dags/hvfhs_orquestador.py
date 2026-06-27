from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone
import requests
import time
import os

def generar_periodos(anio_inicio, anio_fin):
    periodos = []
    hoy = datetime.today()
    for anio in range(anio_inicio, anio_fin + 1):
        for mes in range(1, 13):
            if datetime(anio, mes, 1) > hoy.replace(day=1):
                return periodos
            periodos.append((anio, mes))
    return periodos

def get_token():
    base = os.environ["AIRFLOW_API_URL"]
    r = requests.post(
        f"{base}/auth/token",
        json={
            "username": os.environ["AIRFLOW_API_USER"],
            "password": os.environ["AIRFLOW_API_PASS"],
        },
    )
    r.raise_for_status()
    return r.json()["access_token"]

def ejecutar_pipeline(dag_id, anio_inicio, anio_fin, **context):
    base = os.environ["AIRFLOW_API_URL"]
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    for anio, mes in generar_periodos(anio_inicio, anio_fin):
        logical_date = datetime.now(tz=timezone.utc).isoformat()

        r = requests.post(
            f"{base}/api/v2/dags/{dag_id}/dagRuns",
            json={"logical_date": logical_date, "conf": {"anio": anio, "mes": mes}},
            headers=headers,
        )
        if not r.ok:
            raise Exception(f"Error {r.status_code}: {r.text}")

        dag_run_id = r.json()["dag_run_id"]

        while True:
            r = requests.get(
                f"{base}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}",
                headers=headers,
            )
            state = r.json().get("state")
            if state == "success":
                break
            if state == "failed":
                raise Exception(f"Pipeline falló: {anio}-{mes:02d}")
            time.sleep(60)

with DAG(
    dag_id='fhvhv_orquestador',
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['fhv', 'orquestador'],
) as dag:
    PythonOperator(
        task_id='ejecutar_todos_los_meses',
        python_callable=ejecutar_pipeline,
        op_kwargs={
            'dag_id': 'fhvhv_staging_pipeline',
            'anio_inicio': 2020,
            'anio_fin': 2026,
        },
        execution_timeout=None,
    )
