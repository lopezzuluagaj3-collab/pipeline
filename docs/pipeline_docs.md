# Pipeline Docs

El flujo principal se divide en tres pasos:

1. `extraer(ruta)`
2. `transformar(df)`
3. `cargar(df, conn)`

En Airflow los pasos intercambian datos usando `/tmp/raw_sales.parquet` y `/tmp/transformed_sales.parquet`.
