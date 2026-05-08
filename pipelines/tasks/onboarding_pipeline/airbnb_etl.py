from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


CRITICAL_COLUMNS = [
    "id",
    "NAME",
    "host id",
    "neighbourhood group",
    "neighbourhood",
    "lat",
    "long",
    "room type",
    "price",
]

OPTIONAL_TEXT_COLUMNS = [
    "host_identity_verified",
    "host name",
    "country",
    "country code",
    "instant_bookable",
    "cancellation_policy",
    "last review",
]

OPTIONAL_NUMERIC_COLUMNS = [
    "Construction year",
    "service fee",
    "minimum nights",
    "number of reviews",
    "reviews per month",
    "review rate number",
    "calculated host listings count",
    "availability 365",
]


def read_airbnb_dataset(input_path: str | Path) -> pd.DataFrame:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input dataset was not found: {path}")

    return pd.read_csv(path, low_memory=False)


def convert_money_column(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype("string").str.replace(r"[$,]", "", regex=True).str.strip(),
        errors="coerce",
    )


def normalize_text_column(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)


def clean_airbnb_data(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    incoming_records = len(df)

    clean_df = df.copy()
    clean_df.columns = [column.strip() for column in clean_df.columns]

    clean_df = clean_df.drop(columns=["house_rules", "license"], errors="ignore")
    clean_df = clean_df.drop_duplicates(subset=["id"], keep="first")
    records_after_dedup = len(clean_df)

    for column in ["price", "service fee"]:
        if column in clean_df.columns:
            clean_df[column] = convert_money_column(clean_df[column])

    for column in OPTIONAL_NUMERIC_COLUMNS:
        if column in clean_df.columns:
            clean_df[column] = pd.to_numeric(clean_df[column], errors="coerce")

    for column in OPTIONAL_TEXT_COLUMNS:
        if column in clean_df.columns:
            clean_df[column] = normalize_text_column(clean_df[column]).fillna("Unknown")

    clean_df["city"] = normalize_text_column(clean_df["neighbourhood group"]).str.title()
    clean_df["neighbourhood"] = normalize_text_column(clean_df["neighbourhood"]).str.title()
    clean_df["room type"] = normalize_text_column(clean_df["room type"]).str.title()
    clean_df["NAME"] = normalize_text_column(clean_df["NAME"])

    clean_df = clean_df.dropna(subset=CRITICAL_COLUMNS + ["city"])

    fill_values = {
        "service fee": 0,
        "minimum nights": 1,
        "number of reviews": 0,
        "reviews per month": 0,
        "review rate number": 0,
        "calculated host listings count": 0,
        "availability 365": 0,
        "Construction year": 0,
    }
    clean_df = clean_df.fillna(value={key: value for key, value in fill_values.items() if key in clean_df.columns})
    clean_df = clean_df.fillna("Unknown")

    valid_mask = (
        (clean_df["price"] > 0)
        & (clean_df["service fee"] >= 0)
        & (clean_df["minimum nights"] >= 1)
        & (clean_df["number of reviews"] >= 0)
        & (clean_df["lat"].between(-90, 90))
        & (clean_df["long"].between(-180, 180))
        & (clean_df["availability 365"].between(0, 365))
        & (clean_df["city"].ne(""))
    )
    clean_df = clean_df.loc[valid_mask].copy()

    integer_columns = [
        "id",
        "host id",
        "Construction year",
        "minimum nights",
        "number of reviews",
        "review rate number",
        "calculated host listings count",
        "availability 365",
    ]
    for column in integer_columns:
        if column in clean_df.columns:
            clean_df[column] = clean_df[column].astype(int)

    metric_values = build_metrics(clean_df)
    metrics = {
        "incoming_records": incoming_records,
        "duplicates_removed": incoming_records - records_after_dedup,
        "invalid_or_null_records_removed": records_after_dedup - len(clean_df),
        **metric_values,
    }

    return clean_df, metrics


def build_metrics(clean_df: pd.DataFrame) -> dict[str, Any]:
    city_counts = clean_df["city"].value_counts()
    city_with_most_listings = city_counts.index[0] if not city_counts.empty else None

    return {
        "average_price": round(float(clean_df["price"].mean()), 2),
        "city_with_most_listings": city_with_most_listings,
        "city_with_most_listings_count": int(city_counts.iloc[0]) if not city_counts.empty else 0,
        "total_valid_records": int(len(clean_df)),
        "average_reviews": round(float(clean_df["number of reviews"].mean()), 2),
    }


def write_clean_csv(clean_df: pd.DataFrame, output_dir: str | Path) -> str:
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    clean_path = path / "airbnb_clean_data.csv"
    clean_df.to_csv(clean_path, index=False)
    return str(clean_path)


def write_report(metrics: dict[str, Any], output_dir: str | Path, report_format: str) -> str:
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    if report_format == "json":
        report_path = path / "airbnb_report.json"
        report_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        return str(report_path)

    if report_format == "txt":
        report_path = path / "airbnb_report.txt"
        removed_records = metrics["duplicates_removed"] + metrics["invalid_or_null_records_removed"]
        removal_rate = round((removed_records / metrics["incoming_records"]) * 100, 2)
        valid_rate = round((metrics["total_valid_records"] / metrics["incoming_records"]) * 100, 2)

        lines = [
            "Airbnb ETL Report - Hallazgos del Analisis",
            "==========================================",
            "",
            "1. Resumen general",
            "------------------",
            f"Durante el proceso ETL se analizaron {metrics['incoming_records']} registros originales.",
            (
                f"Despues de aplicar reglas de limpieza y validacion quedaron "
                f"{metrics['total_valid_records']} registros validos, equivalentes al {valid_rate}% "
                "del dataset inicial."
            ),
            (
                f"En total se removieron {removed_records} registros, equivalentes al {removal_rate}% "
                "de los datos procesados."
            ),
            "",
            "2. Calidad de datos",
            "-------------------",
            (
                f"Se identificaron y eliminaron {metrics['duplicates_removed']} registros duplicados "
                "usando el campo id como identificador principal."
            ),
            (
                f"Tambien se eliminaron {metrics['invalid_or_null_records_removed']} registros con valores "
                "nulos criticos o datos invalidos, por ejemplo precios vacios, precios menores o iguales "
                "a cero, coordenadas fuera de rango, noches minimas invalidas o disponibilidad fuera del "
                "rango esperado de 0 a 365 dias."
            ),
            "Las columnas house_rules y license fueron descartadas por su baja utilidad para las metricas solicitadas.",
            "",
            "3. Hallazgos comerciales",
            "------------------------",
            (
                f"El precio promedio de los alojamientos validos es {metrics['average_price']}. "
                "Este valor se calculo despues de convertir la columna price desde texto con simbolos "
                "monetarios a formato numerico."
            ),
            (
                f"La zona con mayor cantidad de alojamientos es {metrics['city_with_most_listings']}, "
                f"con {metrics['city_with_most_listings_count']} registros validos."
            ),
            (
                f"El promedio de reviews por alojamiento es {metrics['average_reviews']}, lo que permite "
                "tener una referencia general del nivel de interaccion historica de los usuarios con los "
                "alojamientos publicados."
            ),
            "",
            "4. Transformaciones aplicadas",
            "-----------------------------",
            "Se convirtieron las columnas price y service fee a valores numericos.",
            "Se normalizaron textos de ciudad/zona, barrio y tipo de habitacion.",
            "Se rellenaron valores opcionales con Unknown o cero segun el tipo de columna.",
            "Se validaron coordenadas, disponibilidad, precios, reviews y noches minimas.",
            "Se genero una columna city a partir de neighbourhood group para facilitar el analisis por zona.",
            "",
            "5. Archivos generados",
            "---------------------",
            "airbnb_clean_data.csv: dataset limpio listo para dashboards o cargas posteriores.",
            "airbnb_report.json: reporte estructurado con metricas principales.",
            "airbnb_report.txt: reporte descriptivo con hallazgos del analisis.",
            "",
            "6. Conclusion",
            "-------------",
            (
                "El dataset final conserva la gran mayoria de los registros originales y queda preparado "
                "para alimentar dashboards internos, analisis comerciales y procesos posteriores de carga "
                "en almacenamiento limpio."
            ),
        ]
        report_path.write_text("\n".join(lines), encoding="utf-8")
        return str(report_path)

    raise ValueError(f"Unsupported report format: {report_format}")
