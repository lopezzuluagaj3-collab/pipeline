from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="dag_prueba",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def dag_prueba():
    @task
    def tarea_prueba():
        print("Hola desde el worker!")
        return "ok"

    tarea_prueba()

dag_prueba()