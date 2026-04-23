# RetailCo Pipeline ETL

Pipeline de datos distribuido para RetailCo que extrae, transforma y carga datos de ventas en un modelo estrella en PostgreSQL, orquestado con Apache Airflow 3.2.0.

---

## Arquitectura

```
                        ┌─────────────────────────────────┐
                        │         Master (IP master)      │
                        │  Airflow Scheduler + API Server  │
                        │  PostgreSQL (metadatos Airflow)  │
                        └────────────────┬────────────────┘
                                         │
                        ┌────────────────▼────────────────┐
                        │       RabbitMQ (IP RabbiMQ)       │
                        │         Message Broker           │
                        └──────┬──────────┬───────┬───────┘
                               │          │       │
              ┌────────────────▼─┐  ┌─────▼──┐  ┌▼───────────────┐
              │  Worker 1         │  │Worker 2│  │   Worker 3      │
              │  IP worker       │  │IP Worker│  │  IP worker    │
              │  CeleryExecutor   │  │Celery  │  │  CeleryExecutor │
              │  + git-sync       │  │git-sync│  │  + git-sync     │
              └──────────────────┘  └────────┘  └────────────────┘
```

**Flujo del pipeline:**

```
CSV (sales_data_sample.csv)
        │
        ▼
  t1_extraer          → lee CSV, limpia NaN y Timestamps, pushea a XCom
        │
        ▼
  t2_transformar      → normaliza columnas, aplica reglas de negocio, limpia para XCom
        │
        ▼
  t3_cargar           → restaura tipos datetime, ejecuta schema.sql, carga modelo estrella
        │
        ▼
  PostgreSQL (DB de destino) — modelo estrella: dim_fecha, dim_producto, dim_cliente, fact_ventas
```

---

## Estructura del repositorio

```
pipeline/
├── airflow/
│   └── dags/
│       ├── retailco_pipeline.py   # DAG principal
│       ├── pipeline.py            # Funciones extraer(), transformar(), cargar()
│       └── sql/
│           └── schema.sql         # DDL del modelo estrella
├── master/
│   └── docker-compose.yml         # Scheduler, API Server, PostgreSQL, Flower
├── worker/
│   ├── docker-compose.yml         # CeleryWorker + git-sync
│   └── .env                       # Variables de entorno por worker
├── scripts/
│   ├── pipeline.py                # Script standalone (ejecución local)
│   ├── 01_python_puro.py
│   ├── 02_eda.py
│   ├── 03_limpieza_carga.py
│   ├── 04_analisis.py
│   └── 05_sql_pandas.py
├── sql/
│   ├── schema.sql                 # DDL modelo estrella
│   └── load_star_schema_postgres.sql
├── docs/                          # Documentación funcional y técnica
├── .env.example                   # Plantilla de variables de entorno
├── .gitignore
├── .python-version                # Python 3.11+
└── pyproject.toml
```

---

## Requisitos previos

- Python 3.11 o superior
- Docker y Docker Compose
- Acceso SSH a los nodos EC2 (master y workers)
- PostgreSQL accesible para la base de datos de destino

---

## Configuración

### 1. Variables de entorno — Master

Crear `/home/ubuntu/pipeline/master/.env` a partir de `.env.example`:

```bash
cp .env.example master/.env
```

Variables requeridas en el master:

```bash
# Base de datos de metadatos Airflow
POSTGRES_PASSWORD=tu_password

# Broker
AIRFLOW__CELERY__BROKER_URL=amqp://admin:password@10.0.2.87:5672/

# Seguridad
AIRFLOW__CORE__FERNET_KEY=tu_fernet_key
AIRFLOW__API__SECRET_KEY=tu_secret_key
AIRFLOW__API_AUTH__JWT_SECRET=tu_jwt_secret

# Usuario Airflow (Simple Auth Manager)
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin

# Límite XCom — requerido para datasets > 1024 filas
AIRFLOW__CORE__MAX_MAP_LENGTH=10000
```

### 2. Variables de entorno — Workers

Crear `/home/ubuntu/pipeline/worker/.env` en cada nodo worker:

```bash
# Sistema
AIRFLOW_UID=1000

# IPs de infraestructura
AIRFLOW_MASTER_PRIVATE_IP=10.0.1.423
RABBITMQ_PRIVATE_IP=10.0.2.421

# Credenciales Airflow (deben coincidir exactamente con el master)
POSTGRES_PASSWORD=tu_password
RABBITMQ_PASSWORD=tu_password
AIRFLOW__CORE__FERNET_KEY=tu_fernet_key
AIRFLOW__API__SECRET_KEY=tu_secret_key
AIRFLOW__API_AUTH__JWT_SECRET=tu_jwt_secret
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin

# Concurrencia
WORKER_CONCURRENCY=3

# Repositorio git-sync
GIT_REPO_URL=https://github.com/lopezzuluagaj3-collab/pipeline
GIT_BRANCH=main
GIT_SYNC_PERIOD=60s

# Credenciales DB de destino (RetailCo)
DB_HOST=ip_de_tu_postgres
DB_PORT=5432
DB_NAME=nombre_de_tu_db
DB_USER=tu_usuario
DB_PASSWORD=tu_password
```

---

## Despliegue

### Levantar el master

```bash
cd /home/ubuntu/pipeline/master
docker compose up -d

# Verificar que todos los servicios estén healthy
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Servicios que deben levantar: `airflow-postgres`, `airflow-init`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-flower`.

### Levantar los workers

Ejecutar en cada nodo worker (o desde el master vía SSH):

```bash
# Desde el master, para los 3 workers de una vez
for worker in 10.0.2.111 10.0.2.222 10.0.2.333; do
  echo "=== $worker ==="
  scp -i ~/.ssh/general-key.pem \
    /home/ubuntu/pipeline/worker/docker-compose.yml \
    ubuntu@$worker:/home/ubuntu/pipeline/worker/docker-compose.yml
  ssh -i ~/.ssh/general-key.pem ubuntu@$worker \
    "cd /home/ubuntu/pipeline/worker && docker compose up -d"
done
```

git-sync sincroniza el repo cada 60 segundos. Los DAGs quedan disponibles en `/opt/airflow/dags/current/airflow/dags/`.

---

## Ejecución del pipeline

### Trigger manual

```bash
docker exec airflow-scheduler airflow dags trigger retailco_pipeline
```

### Monitorear estado de tareas

```bash
# Reemplazar el run_id con el generado por el trigger
docker exec airflow-scheduler airflow tasks states-for-dag-run \
  retailco_pipeline manual__2026-04-23T00:52:17.448405+00:00
```

### Ver runs disponibles

```bash
docker exec airflow-scheduler airflow dags list-runs retailco_pipeline \
  --output plain 2>/dev/null | grep manual
```

### UI de Airflow

Acceder en `http://tuIPMaster:8080` con las credenciales definidas en `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`.

### UI de Flower (monitoreo Celery)

Acceder en `http://tuIPWorker:5555`.

---

## Ejecución local (sin Airflow)

```bash
# Instalar dependencias
uv sync
# o
pip install -r requirements.txt

# Crear el schema en PostgreSQL
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f sql/schema.sql

# Ejecutar scripts de análisis
python scripts/01_python_puro.py
python scripts/02_eda.py
python scripts/03_limpieza_carga.py
python scripts/04_analisis.py
python scripts/05_sql_pandas.py
```

---

## Modelo de datos

El pipeline carga un modelo estrella en PostgreSQL con las siguientes tablas:

| Tabla | Tipo | Descripción |
|---|---|---|
| `dim_fecha` | Dimensión | Fecha de orden con atributos temporales (año, trimestre, mes, día) |
| `dim_producto` | Dimensión | Producto, línea de producto y MSRP |
| `dim_cliente` | Dimensión | Cliente, país, ciudad y territorio |
| `fact_ventas` | Hecho | Ventas por línea de orden con métricas (cantidad, precio, ventas totales) |

Características:
- Carga incremental con `ON CONFLICT DO NOTHING` — no reprocesa registros existentes
- Deduplicación por línea de orden antes de cargar
- Tipos de dato correctos restaurados antes de cada inserción

---

## Notas técnicas importantes

### XCom y serialización

Airflow 3.2.0 serializa los valores de XCom con restricciones estrictas:

- `MAX_MAP_LENGTH` por defecto es 1024 elementos — insuficiente para datasets reales. Se debe configurar `AIRFLOW__CORE__MAX_MAP_LENGTH=10000` en master y workers.
- Los valores `NaN` de pandas no son serializables en JSON — deben convertirse a `None` antes del `return`.
- Los `pandas.Timestamp` tampoco son serializables — deben convertirse a string ISO 8601 con `.isoformat()`.
- Al reconstruir el DataFrame en tareas posteriores, las columnas de fecha deben restaurarse con `pd.to_datetime()`.

### git-sync y rutas de archivos

Los workers reciben los DAGs vía git-sync montado en `worker_dags-volume`. Todos los archivos referenciados por `pipeline.py` (como `schema.sql`) deben estar dentro de `airflow/dags/` para que git-sync los replique. Archivos fuera de ese directorio no estarán disponibles en los workers.

### Variables de entorno en workers

Las variables definidas en `.env` del worker deben declararse **explícitamente** en el bloque `environment` del `docker-compose.yml` para que Docker las inyecte al contenedor. Tener la variable en `.env` no es suficiente.

---

## Tecnologías

| Tecnología | Versión | Rol |
|---|---|---|
| Apache Airflow | 3.2.0 | Orquestador |
| CeleryExecutor | — | Ejecución distribuida |
| RabbitMQ | — | Message broker |
| PostgreSQL | 16 | Metadatos Airflow + DB destino |
| git-sync | v4.2.1 | Sincronización de DAGs en workers |
| pandas | — | Transformación de datos |
| SQLAlchemy + psycopg2 | — | Conexión a PostgreSQL |
| Docker + Docker Compose | — | Infraestructura |
| Python | 3.13 | Runtime |
