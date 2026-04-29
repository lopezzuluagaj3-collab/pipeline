from airflow.decorators import dag
from tasks.echandia.task1 import extraer, transformar, cargar

@dag(
    dag_id="echandia_exercise",
    schedule=None,
    catchup=False,
    tags=["example"],
)
def echandia_dag():
    extraer() >> transformar() >> cargar()

dag_instance = echandia_dag()