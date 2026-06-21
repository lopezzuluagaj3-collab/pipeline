from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone
import requests
import time

AIRFLOW_API = "https://sirius.coderhivex.com/api/v2"
AUTH = ("admin", "admin")

def generar_periodos(anio_inicio, anio_fin):
    periodos = []
    hoy = datetime.today()
    for anio in range(anio_inicio, anio_fin + 1):
        for mes in range(1, 13):
            if datetime(anio, mes, 1) > hoy.replace(day=1):
                return periodos
            periodos.append((anio, mes))
    return periodos

def ejecutar_pipeline(dag_id, anio_inicio, anio_fin, **context):
    for anio, mes in generar_periodos(anio_inicio, anio_fin):
        run_id = f'orq_{anio}_{mes:02d}_{datetime.now().strftime("%Y%m%d%H%M%S")}'

        # Disparar
        r = requests.post(
            f"{AIRFLOW_API}/dags/{dag_id}/dagRuns",
            json={"dag_run_id": run_id, "conf": {"anio": anio, "mes": mes}},
            auth=AUTH,
            verify=False,
        )
        r.raise_for_status()

        # Esperar
        while True:
            r = requests.get(
                f"{AIRFLOW_API}/dags/{dag_id}/dagRuns/{run_id}",
                auth=AUTH,
                verify=False,
            )
            state = r.json().get("state")
            if state == "success":
                break
            if state == "failed":
                raise Exception(f"Pipeline falló: {anio}-{mes:02d}")
            time.sleep(60)

with DAG(
    dag_id='fhv_orquestador',
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['fhv', 'orquestador'],
) as dag:
    PythonOperator(
        task_id='ejecutar_todos_los_meses',
        python_callable=ejecutar_pipeline,
        op_kwargs={
            'dag_id': 'fhv_staging_pipeline',
            'anio_inicio': 2015,
            'anio_fin': 2026,
        },
        execution_timeout=None,
    )
