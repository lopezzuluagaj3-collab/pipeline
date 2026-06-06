from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timezone

def generar_periodos(anio_inicio, anio_fin):
    periodos = []
    hoy = datetime.today()
    for anio in range(anio_inicio, anio_fin + 1):
        for mes in range(1, 13):
            if datetime(anio, mes, 1) > hoy.replace(day=1):
                return periodos
            periodos.append((anio, mes))
    return periodos

with DAG(
    dag_id='green_orquestador',
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_tasks=1,
    tags=['green', 'orquestador'],
) as dag:
    anterior = None
    for anio, mes in generar_periodos(2013, 2026):
        t = TriggerDagRunOperator(
            task_id=f'trigger_{anio}_{mes:02d}',
            trigger_dag_id='green_staging_pipeline',
            conf={'anio': anio, 'mes': mes},
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )
        if anterior:
            anterior >> t
        anterior = t