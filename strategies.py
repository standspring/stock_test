from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class PortfolioLike(Protocol):
    cash: float
    shares: int
    cost_basis: float
    avg_cost: float


@dataclass(frozen=True)
class InfiniteOrder:
    side: str
    order_type: str
    amount: float | None = None
    shares: int | None = None
    limit_price: float | None = None
    label: str = ""


class InfiniteBuyingV3Strategy:
    """Raoh-style infinite buying V3 approximation for daily OHLC backtests."""

    def __init__(self, ticker: str, initial_cash: float, target_profit_pct: float | None = None) -> None:
        self.ticker = ticker.upper()
        self.target_profit_pct = target_profit_pct or self._default_target_profit_pct(self.ticker)
        self.split_count = 20
        self.unit_amount = initial_cash / self.split_count
        self.quarter_mode_days_left = 0

    @staticmethod
    def _default_target_profit_pct(ticker: str) -> float:
        if ticker.startswith("SOXL"):
            return 20.0
        if ticker.startswith("TQQQ"):
            return 15.0
        return 15.0

    def t_value(self, portfolio: PortfolioLike) -> float:
        if self.unit_amount <= 0:
            return 0.0
        return round(portfolio.cost_basis / self.unit_amount, 2)

    def star_pct(self, portfolio: PortfolioLike) -> float:
        t = self.t_value(portfolio)
        if self.quarter_mode_days_left > 0:
            return -self.target_profit_pct
        return self.target_profit_pct - (self.target_profit_pct / 10) * t

    def orders(self, portfolio: PortfolioLike) -> list[InfiniteOrder]:
        orders: list[InfiniteOrder] = []

        if portfolio.shares > 0:
            avg_cost = portfolio.avg_cost
            target_price = avg_cost * (1 + self.target_profit_pct / 100)

            if 19 < self.t_value(portfolio) < 20:
                quarter_shares = max(1, portfolio.shares // 4)
                orders.append(InfiniteOrder("SELL", "MOC", shares=quarter_shares, label="quarter_moc_sell"))
                orders.append(
                    InfiniteOrder(
                        "SELL",
                        "LIMIT",
                        shares=portfolio.shares - quarter_shares,
                        limit_price=target_price,
                        label="target_sell",
                    )
                )
                self.quarter_mode_days_left = 5
            else:
                quarter_shares = max(1, portfolio.shares // 4)
                star_price = avg_cost * (1 + self.star_pct(portfolio) / 100)
                orders.append(InfiniteOrder("SELL", "LOC", shares=quarter_shares, limit_price=star_price, label="quarter_loc_sell"))
                orders.append(
                    InfiniteOrder(
                        "SELL",
                        "LIMIT",
                        shares=portfolio.shares - quarter_shares,
                        limit_price=target_price,
                        label="target_sell",
                    )
                )

        if portfolio.cash <= 0:
            return orders

        t = self.t_value(portfolio)
        reference_price = self._reference_buy_price(portfolio)
        star_buy_price = reference_price * (1 + self.star_pct(portfolio) / 100)
        if t < 10:
            orders.append(InfiniteOrder("BUY", "LOC", amount=self.unit_amount / 2, limit_price=reference_price, label="avg_loc_buy"))
            orders.append(InfiniteOrder("BUY", "LOC", amount=self.unit_amount / 2, limit_price=star_buy_price, label="star_loc_buy"))
        else:
            orders.append(InfiniteOrder("BUY", "LOC", amount=self.unit_amount, limit_price=star_buy_price, label="star_loc_buy"))

        return orders

    def after_sell(self, realized_profit: float, portfolio: PortfolioLike) -> None:
        if realized_profit > 0:
            self.unit_amount += realized_profit / 40
        if portfolio.shares == 0:
            self.quarter_mode_days_left = 0
        elif self.quarter_mode_days_left > 0:
            self.quarter_mode_days_left -= 1

    def _reference_buy_price(self, portfolio: PortfolioLike) -> float:
        if portfolio.shares > 0:
            return portfolio.avg_cost
        return float("inf")
