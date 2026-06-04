from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path

from .models import PricePoint

DB_PATH = Path("data") / "real_estate.sqlite3"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            apartment_name TEXT NOT NULL,
            observed_date TEXT NOT NULL,
            price_krw INTEGER NOT NULL,
            area_m2 REAL,
            trade_type TEXT NOT NULL DEFAULT 'sale',
            floor TEXT,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, apartment_name, observed_date, price_krw, area_m2, trade_type, floor)
        )
        """
    )
    conn.commit()


def save_price_points(conn: sqlite3.Connection, points: list[PricePoint]) -> int:
    inserted = 0
    for point in points:
        result = conn.execute(
            """
            INSERT OR IGNORE INTO price_points
                (source, apartment_name, observed_date, price_krw, area_m2, trade_type, floor, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                point.source,
                point.apartment_name,
                point.observed_date.isoformat(),
                point.price_krw,
                point.area_m2,
                point.trade_type,
                point.floor,
                point.note,
            ),
        )
        inserted += result.rowcount
    conn.commit()
    return inserted


def list_apartments(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT apartment_name FROM price_points ORDER BY apartment_name"
    ).fetchall()
    return [row["apartment_name"] for row in rows]


def list_price_points(conn: sqlite3.Connection, apartment_name: str | None = None) -> list[dict]:
    params: tuple[str, ...] = ()
    where = ""
    if apartment_name:
        where = "WHERE apartment_name = ?"
        params = (apartment_name,)

    rows = conn.execute(
        f"""
        SELECT source, apartment_name, observed_date, price_krw, area_m2, trade_type, floor, note
        FROM price_points
        {where}
        ORDER BY observed_date, source, price_krw
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def seed_demo_data(conn: sqlite3.Connection) -> int:
    if conn.execute("SELECT COUNT(*) AS count FROM price_points").fetchone()["count"]:
        return 0

    points = [
        PricePoint("sample", "래미안 원베일리", date(2025, 9, 1), 3_980_000_000, 84.95, note="샘플 데이터"),
        PricePoint("sample", "래미안 원베일리", date(2025, 11, 1), 4_120_000_000, 84.95, note="샘플 데이터"),
        PricePoint("sample", "래미안 원베일리", date(2026, 1, 1), 4_050_000_000, 84.95, note="샘플 데이터"),
        PricePoint("sample", "래미안 원베일리", date(2026, 3, 1), 4_260_000_000, 84.95, note="샘플 데이터"),
        PricePoint("sample", "래미안 원베일리", date(2026, 5, 1), 4_330_000_000, 84.95, note="샘플 데이터"),
    ]
    return save_price_points(conn, points)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()
