# RetailCo Analysis

Prueba tecnica orientada a analisis de datos y orquestacion ETL para RetailCo. El proyecto queda listo para explorar ventas, transformarlas y cargarlas a PostgreSQL con soporte para ejecucion en Airflow.

## Descripcion del proyecto

Este repositorio implementa un ETL de ventas para RetailCo con carga a PostgreSQL, analisis exploratorio en Python y orquestacion con Airflow usando CeleryExecutor y RabbitMQ.

## Requisitos previos

- Python 3.11 o superior
- `uv` o `pip`
- Docker y Docker Compose
- PostgreSQL accesible para la base `retailco_db`
- Archivo `.env` configurado con tus variables locales

## Estructura

```text
data/new_data/
docs/
scripts/
airflow/dags/
master/
worker/
sql/
outputs/
```

## Como configurar el .env

Crear `.env` a partir de `.env.example` y completar credenciales solo en local.

```bash
cp .env.example .env
```

Variables esperadas:

```bash
CSV_PATH=data/new_data/sales_data_sample.csv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retailco_db
DB_USER=postgres
DB_PASSWORD=postgres
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=pyamqp://guest:guest@rabbitmq:5672//
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW_UID=50000
WORKER_CONCURRENCY=2
```

## Instrucciones para levantar master

```bash
cd master
docker compose --env-file ../.env up -d
```

## Instrucciones para levantar worker

```bash
cp .env.example worker/.env
cd worker
docker compose --env-file .env up -d
```

## Estructura del repositorio

- `airflow/dags/`: DAGs de orquestacion
- `scripts/`: ETL y analisis en Python
- `sql/`: definicion del esquema analitico
- `data/new_data/`: datos fuente
- `docs/`: documentacion funcional y tecnica
- `master/` y `worker/`: despliegue distribuido de Airflow

## Hallazgos principales del analisis

- Las ventas pueden analizarse por tiempo, producto, cliente y pais con un modelo estrella simple.
- El pipeline elimina duplicados por linea de orden antes de cargar.
- La carga incremental evita reprocesar registros gracias a `ON CONFLICT DO NOTHING`.
- Airflow intercambia datos temporales en Parquet para separar extraccion, transformacion y carga.
- La estructura del repositorio ya queda lista para entrega tecnica y demostracion.

## Ejecucion local de scripts

```bash
uv sync
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f sql/schema.sql
python scripts/01_python_puro.py
python scripts/02_eda.py
python scripts/03_limpieza_carga.py
python scripts/04_analisis.py
python scripts/05_sql_pandas.py
```

## Pipeline

El flujo principal vive en [scripts/pipeline.py](/home/cohorte6/pipeline/scripts/pipeline.py) y expone `extraer(ruta)`, `transformar(df)` y `cargar(df, conn)`.

El DAG [airflow/dags/retailco_pipeline.py](/home/cohorte6/pipeline/airflow/dags/retailco_pipeline.py) ejecuta:

```text
t1 >> t2 >> t3
```
