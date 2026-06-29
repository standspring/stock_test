from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .models import ApartmentTarget
from .scrapers import CsvCollector, PermissionRequiredCollector, default_collectors
from .storage import get_connection, init_db, save_price_points

DEFAULT_TARGETS_PATH = Path("data") / "apartment_targets.json"
DEFAULT_CSV_PATH = Path("data") / "apartment_prices.csv"
LOG_PATH = Path("logs") / "collect_prices.log"


def configure_logging() -> None:
    LOG_PATH.parent.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def load_targets(path: Path) -> list[ApartmentTarget]:
    if not path.exists():
        raise FileNotFoundError(
            f"Target config not found: {path}. Create it from data/apartment_targets.example.json."
        )

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("Target config must be a JSON array.")

    targets: list[ApartmentTarget] = []
    for row in raw:
        if not isinstance(row, dict):
            raise ValueError("Each target must be an object.")
        if not row.get("name"):
            raise ValueError("Each target must include a name.")
        targets.append(
            ApartmentTarget(
                name=row["name"],
                area_m2=_optional_float(row.get("area_m2")),
                naver_complex_no=_optional_str(row.get("naver_complex_no")),
                asil_apt_id=_optional_str(row.get("asil_apt_id")),
                hogangnono_id=_optional_str(row.get("hogangnono_id")),
            )
        )
    return targets


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def collectors_for(source: str, csv_path: Path):
    if source == "csv":
        return [CsvCollector(csv_path)]
    if source == "all":
        return default_collectors(csv_path)
    raise ValueError(f"Unsupported source: {source}")


def collect(targets: list[ApartmentTarget], source: str, csv_path: Path) -> int:
    total_inserted = 0
    collectors = collectors_for(source, csv_path)

    with get_connection() as conn:
        init_db(conn)
        for target in targets:
            logging.info("Collecting target=%s", asdict(target))
            for collector in collectors:
                try:
                    points = collector.collect(target)
                except PermissionError as exc:
                    logging.warning("%s", exc)
                    continue
                except Exception:
                    logging.exception("Collector failed: source=%s target=%s", collector.source, target.name)
                    continue

                inserted = save_price_points(conn, points)
                total_inserted += inserted
                logging.info(
                    "Collector finished: source=%s target=%s found=%s inserted=%s",
                    collector.source,
                    target.name,
                    len(points),
                    inserted,
                )

    logging.info("Collect job finished: inserted=%s", total_inserted)
    return total_inserted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect apartment price points.")
    parser.add_argument("--targets", type=Path, default=DEFAULT_TARGETS_PATH, help="Apartment target JSON path.")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH, help="CSV source path.")
    parser.add_argument("--source", choices=["csv", "all"], default="csv", help="Collector source to run.")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    targets = load_targets(args.targets)
    inserted = collect(targets, args.source, args.csv)
    print(f"inserted={inserted}")


if __name__ == "__main__":
    main()
