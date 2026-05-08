from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


def load_report(report_path: str | Path) -> dict[str, Any]:
    path = Path(report_path)
    if not path.exists():
        raise FileNotFoundError(f"Report JSON was not found: {path}")

    return json.loads(path.read_text(encoding="utf-8"))


def plot_airbnb_report(report_path: str | Path, output_path: str | Path | None = None) -> str:
    metrics = load_report(report_path)
    report_path = Path(report_path)
    output = Path(output_path) if output_path else report_path.with_name("airbnb_report_dashboard.png")
    output.parent.mkdir(parents=True, exist_ok=True)

    records_labels = ["Originales", "Validos", "Duplicados", "Invalidos/nulos"]
    records_values = [
        metrics["incoming_records"],
        metrics["total_valid_records"],
        metrics["duplicates_removed"],
        metrics["invalid_or_null_records_removed"],
    ]

    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    fig.suptitle("Airbnb ETL Report", fontsize=18, fontweight="bold")

    axes[0, 0].bar(records_labels, records_values, color=["#2563eb", "#16a34a", "#f59e0b", "#dc2626"])
    axes[0, 0].set_title("Registros del Pipeline")
    axes[0, 0].set_ylabel("Cantidad")
    axes[0, 0].tick_params(axis="x", rotation=20)
    axes[0, 0].bar_label(axes[0, 0].containers[0], fmt="%.0f", padding=3)

    valid_records = metrics["total_valid_records"]
    removed_records = metrics["duplicates_removed"] + metrics["invalid_or_null_records_removed"]
    axes[0, 1].pie(
        [valid_records, removed_records],
        labels=["Validos", "Removidos"],
        autopct="%1.1f%%",
        startangle=90,
        colors=["#16a34a", "#ef4444"],
    )
    axes[0, 1].set_title("Calidad del Dataset")

    axes[1, 0].bar(
        ["Precio promedio", "Reviews promedio"],
        [metrics["average_price"], metrics["average_reviews"]],
        color=["#7c3aed", "#0891b2"],
    )
    axes[1, 0].set_title("Promedios Comerciales")
    axes[1, 0].bar_label(axes[1, 0].containers[0], fmt="%.2f", padding=3)

    city_label = metrics["city_with_most_listings"]
    city_count = metrics["city_with_most_listings_count"]
    axes[1, 1].barh([city_label], [city_count], color="#ea580c")
    axes[1, 1].set_title("Zona con Mas Alojamientos")
    axes[1, 1].set_xlabel("Alojamientos")
    axes[1, 1].bar_label(axes[1, 1].containers[0], fmt="%.0f", padding=3)

    for ax in axes.flat:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(axis="y", alpha=0.2)

    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(output, dpi=160, bbox_inches="tight")
    plt.close(fig)

    return str(output)


if __name__ == "__main__":
    default_report = Path("output") / "airbnb" / "airbnb_report.json"
    print(plot_airbnb_report(default_report))
