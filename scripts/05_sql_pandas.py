from __future__ import annotations

from dotenv import load_dotenv
import pandas as pd

from pipeline import build_engine


def main() -> None:
    load_dotenv()
    engine = build_engine()
    query = """
    SELECT
        p.productline,
        COUNT(*) AS order_lines,
        COALESCE(SUM(f.sales), 0) AS total_sales
    FROM fact_ventas f
    JOIN dim_producto p ON p.producto_key = f.producto_key
    GROUP BY p.productline
    ORDER BY total_sales DESC;
    """
    df = pd.read_sql(query, engine)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
