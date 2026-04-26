from __future__ import annotations
import os, sys, io
from datetime import datetime
import pandas as pd
import boto3
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/dags/current/airflow/dags/")
from pipeline import build_connection, cargar, transformar


# ── helpers S3 ────────────────────────────────────────────────────────────────

def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

def _read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    response = _s3_client().get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()), encoding="latin-1")

def _write_csv_to_s3(df: pd.DataFrame, bucket: str, key: str) -> str:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    _s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8")
    )
    return f"s3://{bucket}/{key}"


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="retailco_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retailco", "etl"],
)
def retailco_pipeline():

    @task(task_id="t1_extraer")
    def t1_extraer(**context) -> str:
        bucket = os.getenv("S3_BUCKET", "bucket-pipeline-1.0")
        key = "logs/new data/sales_data_sample.csv"
        run_id = context["run_id"].replace(":", "-").replace("+", "-")

        df = _read_csv_from_s3(bucket, key)

        # Guarda raw en S3 para t2
        raw_key = f"tmp/{run_id}/raw.csv"
        return _write_csv_to_s3(df, bucket, raw_key)

    @task(task_id="t2_transformar")
    def t2_transformar(raw_s3_path: str, **context) -> str:
        run_id = context["run_id"].replace(":", "-").replace("+", "-")
        bucket = os.getenv("S3_BUCKET", "bucket-pipeline-1.0")

        # Lee raw desde S3
        key = raw_s3_path.replace(f"s3://{bucket}/", "")
        df = _read_csv_from_s3(bucket, key)

        transformed = transformar(df)

        # Guarda transformado en S3 para t3
        clean_key = f"tmp/{run_id}/transformed.csv"
        return _write_csv_to_s3(transformed, bucket, clean_key)

    @task(task_id="t3_cargar")
    def t3_cargar(transformed_s3_path: str) -> None:
        bucket = os.getenv("S3_BUCKET", "bucket-pipeline-1.0")

        # Lee transformado desde S3
        key = transformed_s3_path.replace(f"s3://{bucket}/", "")
        df = _read_csv_from_s3(bucket, key)

        if "orderdate" in df.columns:
            df["orderdate"] = pd.to_datetime(df["orderdate"])

        # Carga a PostgreSQL
        with build_connection() as conn:
            cargar(df, conn)

        # Guarda CSV limpio en clean data/
        _write_csv_to_s3(df, bucket, "logs/clean data/sales_data_clean.csv")

    t1 = t1_extraer()
    t2 = t2_transformar(t1)
    t3 = t3_cargar(t2)

retailco_pipeline()
