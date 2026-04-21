from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


def main() -> None:
    csv_path = Path(os.getenv("CSV_PATH", "data/new_data/sales_data_sample.csv"))
    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    df = pd.read_csv(csv_path)
    summary = pd.DataFrame(
        {
            "rows": [len(df)],
            "columns": [df.shape[1]],
            "null_cells": [int(df.isna().sum().sum())],
            "duplicate_rows": [int(df.duplicated().sum())],
            "sales_sum": [float(df.get("sales", pd.Series(dtype=float)).fillna(0).sum())],
        }
    )
    summary.to_csv(output_dir / "eda_summary.csv", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
