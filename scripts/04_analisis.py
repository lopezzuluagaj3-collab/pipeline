from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


def main() -> None:
    csv_path = Path(os.getenv("CSV_PATH", "data/new_data/sales_data_sample.csv"))
    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    df = pd.read_csv(csv_path)
    result = (
        df.groupby("productline", dropna=False)["sales"]
        .sum()
        .sort_values(ascending=False)
        .reset_index(name="total_sales")
    )
    result.to_csv(output_dir / "sales_by_productline.csv", index=False)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
