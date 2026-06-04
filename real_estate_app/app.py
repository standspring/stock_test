from __future__ import annotations

from pathlib import Path

from flask import Flask, jsonify, render_template, request

from .models import ApartmentTarget
from .scrapers import CsvCollector
from .storage import get_connection, init_db, list_apartments, list_price_points, save_price_points, seed_demo_data


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
        apartment_name = request.form.get("apartment_name", "").strip()
        csv_path = Path(request.form.get("path", "data/apartment_prices.csv"))
        if not apartment_name:
            return jsonify({"error": "apartment_name is required"}), 400

        collector = CsvCollector(csv_path)
        points = collector.collect(ApartmentTarget(name=apartment_name))
        with get_connection() as conn:
            inserted = save_price_points(conn, points)
        return jsonify({"imported": inserted, "found": len(points)})

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
