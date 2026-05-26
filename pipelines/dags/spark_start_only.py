from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import boto3
import os

# ─── CONFIGURACIÓN DESDE .env ─────────────────────────────────────────────────

SPARK_ROLE_ARN = os.environ["SPARK_ROLE_ARN"]
SPARK_REGION   = os.environ["SPARK_REGION"]

SPARK_INSTANCE_IDS = [
    "i-0dc6390f17528d2f2",  # SVR-MASTER-SPARK
    "i-013fe65f5dfb3a615",  # SVR-WORKER-SPARK-2
    "i-0f17022ecefb9bf9a",  # SVR-WORKER-SPARK-3
    "i-06e31c09ad09ad301",  # SVR-WORKER-SPARK-4
    "i-016238606a706548f",  # SVR-WORKER-SPARK-5
    "i-07325967ae576aa9c",  # SVR-WORKER-SPARK-6
    "i-071409f25064b2d8e",  # SVR-WORKER-SPARK-7
    "i-0e7300ce4baf5ddfc",  # SVR-WORKER-SPARK-8
    "i-09102f527db28cbe6",  # SVR-WORKER-SPARK-9
    "i-0afd1bda307909772",  # SVR-WORKER-SPARK-10
    "i-046f6f9d94c0e1901",  # SVR-WORKER-SPARK-11
    "i-0a0cde4467f64a90b",  # SVR-WORKER-SPARK-12
]

# ─── HELPER ───────────────────────────────────────────────────────────────────

def get_ec2_client():
    sts = boto3.client("sts")
    creds = sts.assume_role(
        RoleArn=SPARK_ROLE_ARN,
        RoleSessionName="airflow-spark-session"
    )["Credentials"]
    return boto3.client(
        "ec2",
        region_name=SPARK_REGION,
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"]
    )

# ─── TAREAS ───────────────────────────────────────────────────────────────────

def start_instances():
    ec2 = get_ec2_client()
    ec2.start_instances(InstanceIds=SPARK_INSTANCE_IDS)
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(
        InstanceIds=SPARK_INSTANCE_IDS,
        WaiterConfig={"Delay": 15, "MaxAttempts": 40}
    )
    print("Todas las instancias están running.")


def stop_instances():
    ec2 = get_ec2_client()
    ec2.stop_instances(InstanceIds=SPARK_INSTANCE_IDS)
    print("Todas las instancias apagadas.")

# ─── DAGs ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="spark_start",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "control"],
) as dag_start:

    PythonOperator(
        task_id="start_spark_instances",
        python_callable=start_instances,
    )


with DAG(
    dag_id="spark_stop",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "control"],
) as dag_stop:

    PythonOperator(
        task_id="stop_spark_instances",
        python_callable=stop_instances,
    )