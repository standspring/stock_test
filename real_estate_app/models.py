from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ApartmentTarget:
    name: str
    area_m2: float | None = None
    naver_complex_no: str | None = None
    asil_apt_id: str | None = None
    hogangnono_id: str | None = None


@dataclass(frozen=True)
class PricePoint:
    source: str
    apartment_name: str
    observed_date: date
    price_krw: int
    area_m2: float | None = None
    trade_type: str = "sale"
    floor: str | None = None
    note: str | None = None
