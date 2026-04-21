from __future__ import annotations

import csv
import os
from collections import Counter
from pathlib import Path


def main() -> None:
    csv_path = Path(os.getenv("CSV_PATH", "data/new_data/sales_data_sample.csv"))
    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontro el CSV en {csv_path}")

    total_sales = 0.0
    total_rows = 0
    countries = Counter()

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            total_rows += 1
            total_sales += float(row.get("sales") or 0)
            countries[row.get("country") or "UNKNOWN"] += 1

    print(f"Filas procesadas: {total_rows}")
    print(f"Ventas totales: {total_sales:.2f}")
    print(f"Paises detectados: {dict(countries)}")


if __name__ == "__main__":
    main()
