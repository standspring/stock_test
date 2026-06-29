from __future__ import annotations

import hmac
import os
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from .models import ApartmentTarget, PricePoint
from .scrapers import CsvCollector
from .storage import get_connection, init_db, list_apartments, list_price_points, parse_date, save_price_points, seed_demo_data

ENTRY_PASSWORD_ENV = "REAL_ESTATE_ENTRY_PASSWORD"
DEFAULT_ENTRY_PASSWORD = "change-me"


def is_valid_entry_password(value: str) -> bool:
    expected = os.environ.get(ENTRY_PASSWORD_ENV, DEFAULT_ENTRY_PASSWORD)
    return hmac.compare_digest(value, expected)


def create_app() -> Flask:
    app = Flask(__name__)

    with get_connection() as conn:
        init_db(conn)
        seed_demo_data(conn)

    @app.get("/")
    def index():
        with get_connection() as conn:
            apartments = list_apartments(conn)
        return render_template("index.html", apartments=apartments)

    @app.get("/api/prices")
    def api_prices():
        apartment_name = request.args.get("apartment") or None
        with get_connection() as conn:
            return jsonify(
                {
                    "apartments": list_apartments(conn),
                    "points": list_price_points(conn, apartment_name),
                }
            )

    @app.post("/api/import-csv")
    def import_csv():
        password = request.form.get("password", "")
        if not is_valid_entry_password(password):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 401

        apartment_name = request.form.get("apartment_name", "").strip()
        csv_path = Path(request.form.get("path", "data/apartment_prices.csv"))
        if not apartment_name:
            return jsonify({"error": "apartment_name is required"}), 400

        collector = CsvCollector(csv_path)
        points = collector.collect(ApartmentTarget(name=apartment_name))
        with get_connection() as conn:
            inserted = save_price_points(conn, points)
        return jsonify({"imported": inserted, "found": len(points)})

    @app.post("/api/prices")
    def add_price():
        data = request.get_json(silent=True) or request.form
        password = str(data.get("password", ""))
        if not is_valid_entry_password(password):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 401

        apartment_name = str(data.get("apartment_name", "")).strip()
        observed_date = str(data.get("observed_date", "")).strip()
        price_value = str(data.get("price_krw", "")).replace(",", "").strip()

        if not apartment_name or not observed_date or not price_value:
            return jsonify({"error": "단지명, 기준일, 가격은 필수입니다."}), 400

        try:
            price_krw = int(price_value)
            if price_krw <= 0:
                raise ValueError
            area_value = str(data.get("area_m2", "")).strip()
            area_m2 = float(area_value) if area_value else None
            if area_m2 is not None and area_m2 <= 0:
                raise ValueError
            point_date = parse_date(observed_date)
        except ValueError:
            return jsonify({"error": "날짜, 가격 또는 면적 형식을 확인해 주세요."}), 400

        point = PricePoint(
            source="manual",
            apartment_name=apartment_name,
            observed_date=point_date,
            price_krw=price_krw,
            area_m2=area_m2,
            floor=str(data.get("floor", "")).strip() or None,
            note=str(data.get("note", "")).strip() or None,
        )
        with get_connection() as conn:
            inserted = save_price_points(conn, [point])

        if not inserted:
            return jsonify({"error": "동일한 가격 데이터가 이미 등록되어 있습니다."}), 409
        return jsonify({"saved": True, "apartment_name": apartment_name}), 201

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
