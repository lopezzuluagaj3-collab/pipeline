from __future__ import annotations

import logging
import sys
from io import BytesIO
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.sdk import Variable, dag, task
from airflow.sdk.bases.hook import BaseHook


PIPELINES_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PIPELINES_DIR) not in sys.path:
    sys.path.append(str(PIPELINES_DIR))

from tasks.onboarding_pipeline.airbnb_etl import (  # noqa: E402
    clean_airbnb_data,
    read_airbnb_dataset,
    write_clean_csv,
    write_report,
)


LOGGER = logging.getLogger(__name__)


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    normalized_path = s3_path.removeprefix("s3://").strip()
    bucket, separator, key = normalized_path.partition("/")

    if not bucket or not separator or not key:
        raise ValueError(
            "S3 path must include bucket and key. "
            "Example: sirius-logs-riwi/new data/airbnb_Open_Data.csv"
        )

    return bucket, key


def parse_s3_prefix(s3_path: str) -> tuple[str, str]:
    bucket, prefix = parse_s3_path(s3_path)
    return bucket, prefix.strip("/")


def read_dataset_from_config(config: dict[str, Any]) -> pd.DataFrame:
    if config["input_source"] == "s3":
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        bucket_name, key = parse_s3_path(config["s3_path"])
        hook = S3Hook(aws_conn_id=config["s3_conn_id"])

        if not hook.check_for_key(key=key, bucket_name=bucket_name):
            raise FileNotFoundError(f"S3 object was not found: s3://{bucket_name}/{key}")

        obj = hook.get_key(key=key, bucket_name=bucket_name)
        content = obj.get()["Body"].read()
        return pd.read_csv(BytesIO(content), low_memory=False)

    return read_airbnb_dataset(config["input_path"])


def upload_file_to_s3(file_path: str, s3_output_path: str, s3_conn_id: str) -> str:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    local_path = Path(file_path)
    if not local_path.exists():
        raise FileNotFoundError(f"Output file was not found: {local_path}")

    bucket_name, prefix = parse_s3_prefix(s3_output_path)
    key = f"{prefix}/{local_path.name}" if prefix else local_path.name

    hook = S3Hook(aws_conn_id=s3_conn_id)
    hook.load_file(
        filename=str(local_path),
        key=key,
        bucket_name=bucket_name,
        replace=True,
    )

    return f"s3://{bucket_name}/{key}"


@dag(
    dag_id="airbnb_onboarding_etl_pipeline",
    description="ETL pipeline for the Airbnb Open Data dataset.",
    start_date=datetime(2026, 5, 7),
    schedule="@daily",
    catchup=False,
    tags=["airbnb", "etl", "taskflow"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
)
def airbnb_onboarding_etl_pipeline():
    @task
    def resolve_pipeline_config() -> dict[str, Any]:
        input_source = Variable.get("airbnb_input_source", default_var="local").strip().lower()
        input_path = Variable.get(
            "airbnb_input_csv_path",
            default_var=str(PROJECT_ROOT / "Airbnb_Open_Data.csv"),
        )
        s3_path = Variable.get(
            "airbnb_s3_path",
            default_var="sirius-logs-riwi/new data/airbnb_Open_Data.csv",
        )
        s3_output_path = Variable.get(
            "airbnb_s3_output_path",
            default_var="sirius-logs-riwi/clean data/",
        )
        s3_conn_id = Variable.get("airbnb_s3_conn_id", default_var="aws_default")
        upload_outputs_to_s3 = (
            Variable.get("airbnb_upload_outputs_to_s3", default_var="true").strip().lower() == "true"
        )
        output_dir = Variable.get(
            "airbnb_output_dir",
            default_var=str(PROJECT_ROOT / "output" / "airbnb"),
        )
        connection_id = Variable.get(
            "airbnb_file_connection_id",
            default_var="airbnb_local_filesystem",
        )

        connection_to_check = s3_conn_id if input_source == "s3" else connection_id

        if input_source not in {"local", "s3"}:
            raise ValueError("Airflow Variable 'airbnb_input_source' must be either 'local' or 's3'.")

        try:
            connection = BaseHook.get_connection(connection_to_check)
            LOGGER.info(
                "Using Airflow connection '%s' of type '%s'.",
                connection_to_check,
                connection.conn_type,
            )
        except Exception as exc:
            LOGGER.warning(
                "Airflow connection '%s' was not found. The task may fail if this source requires it. Error: %s",
                connection_to_check,
                exc,
            )

        return {
            "input_source": input_source,
            "input_path": input_path,
            "s3_path": s3_path,
            "s3_output_path": s3_output_path,
            "s3_conn_id": s3_conn_id,
            "upload_outputs_to_s3": upload_outputs_to_s3,
            "output_dir": output_dir,
            "connection_id": connection_id,
        }

    @task
    def extract_dataset(config: dict[str, Any]) -> dict[str, Any]:
        try:
            df = read_dataset_from_config(config)
            source_name = config["s3_path"] if config["input_source"] == "s3" else config["input_path"]
            LOGGER.info("Extracted %s records from %s.", len(df), source_name)
            return {
                **config,
                "output_dir": config["output_dir"],
                "incoming_records": len(df),
                "columns": list(df.columns),
            }
        except Exception:
            LOGGER.exception("Failed while extracting the Airbnb dataset.")
            raise

    @task
    def transform_dataset(extraction: dict[str, Any]) -> dict[str, Any]:
        try:
            df = read_dataset_from_config(extraction)
            clean_df, metrics = clean_airbnb_data(df)
            clean_path = write_clean_csv(clean_df, extraction["output_dir"])

            LOGGER.info("Clean CSV generated at %s.", clean_path)
            LOGGER.info("Pipeline metrics: %s", metrics)

            return {
                "clean_csv_path": clean_path,
                "output_dir": extraction["output_dir"],
                "metrics": metrics,
                "s3_conn_id": extraction["s3_conn_id"],
                "s3_output_path": extraction["s3_output_path"],
                "upload_outputs_to_s3": extraction["upload_outputs_to_s3"],
            }
        except Exception:
            LOGGER.exception("Failed while transforming the Airbnb dataset.")
            raise

    @task
    def generate_report(transformation: dict[str, Any], report_format: str) -> str:
        try:
            report_path = write_report(
                transformation["metrics"],
                transformation["output_dir"],
                report_format,
            )
            LOGGER.info("%s report generated at %s.", report_format.upper(), report_path)
            return report_path
        except Exception:
            LOGGER.exception("Failed while generating the %s report.", report_format)
            raise

    @task
    def upload_output_file(file_path: str, transformation: dict[str, Any]) -> str | None:
        if not transformation["upload_outputs_to_s3"]:
            LOGGER.info("Skipping S3 upload for %s because upload_outputs_to_s3 is false.", file_path)
            return None

        try:
            s3_uri = upload_file_to_s3(
                file_path=file_path,
                s3_output_path=transformation["s3_output_path"],
                s3_conn_id=transformation["s3_conn_id"],
            )
            LOGGER.info("Uploaded %s to %s.", file_path, s3_uri)
            return s3_uri
        except Exception:
            LOGGER.exception("Failed while uploading %s to S3.", file_path)
            raise

    config = resolve_pipeline_config()
    extraction = extract_dataset(config)
    transformation = transform_dataset(extraction)
    report_paths = generate_report.partial(transformation=transformation).expand(report_format=["txt", "json"])
    upload_output_file(transformation["clean_csv_path"], transformation)
    upload_output_file.partial(transformation=transformation).expand(file_path=report_paths)


airbnb_onboarding_etl_pipeline()
