from __future__ import annotations

import csv
from pathlib import Path
from typing import Protocol

from .models import ApartmentTarget, PricePoint
from .storage import parse_date


class PriceCollector(Protocol):
    source: str

    def collect(self, target: ApartmentTarget) -> list[PricePoint]:
        ...


class CsvCollector:
    source = "csv"

    def __init__(self, path: Path) -> None:
        self.path = path

    def collect(self, target: ApartmentTarget) -> list[PricePoint]:
        if not self.path.exists():
            return []

        points: list[PricePoint] = []
        with self.path.open(newline="", encoding="utf-8") as csv_file:
            for row in csv.DictReader(csv_file):
                if row.get("apartment_name") != target.name:
                    continue
                points.append(
                    PricePoint(
                        source=row.get("source") or self.source,
                        apartment_name=row["apartment_name"],
                        observed_date=parse_date(row["observed_date"]),
                        price_krw=int(row["price_krw"]),
                        area_m2=float(row["area_m2"]) if row.get("area_m2") else None,
                        trade_type=row.get("trade_type") or "sale",
                        floor=row.get("floor") or None,
                        note=row.get("note") or None,
                    )
                )
        return points


class PermissionRequiredCollector:
    def __init__(self, source: str, reason: str) -> None:
        self.source = source
        self.reason = reason

    def collect(self, target: ApartmentTarget) -> list[PricePoint]:
        raise PermissionError(
            f"{self.source} collector is disabled: {self.reason}. "
            "Use an official API, written permission, or import an exported CSV."
        )


def default_collectors(csv_path: Path = Path("data") / "apartment_prices.csv") -> list[PriceCollector]:
    return [
        CsvCollector(csv_path),
        PermissionRequiredCollector("hogangnono", "robots.txt disallows general crawling"),
        PermissionRequiredCollector("naver", "enable only after checking the current terms and request limits"),
        PermissionRequiredCollector("asil", "enable only after checking the current terms and request limits"),
    ]
