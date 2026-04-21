from __future__ import annotations

import os

from dotenv import load_dotenv

from pipeline import build_connection, cargar, extraer, transformar


def main() -> None:
    load_dotenv()
    ruta = os.getenv("CSV_PATH", "data/new_data/sales_data_sample.csv")
    df = transformar(extraer(ruta))
    with build_connection() as conn:
        cargar(df, conn)
    print("Carga completada correctamente.")


if __name__ == "__main__":
    main()
