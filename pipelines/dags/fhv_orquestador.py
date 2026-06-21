from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.client.local_client import Client
from datetime import datetime, timezone
import time

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
    client = Client(None, None)
    for anio, mes in generar_periodos(anio_inicio, anio_fin):
        run_id = f'orq_{anio}_{mes:02d}_{datetime.now().strftime("%Y%m%d%H%M%S")}'
        client.trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            conf={'anio': anio, 'mes': mes}
        )
        # Esperar a que termine antes de disparar el siguiente
        while True:
            state = client.get_dag_run_state(dag_id=dag_id, run_id=run_id)
            if state in ('success', 'failed'):
                break
            if state == 'failed':
                raise Exception(f'Failed: {anio}-{mes:02d}')
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
    )
