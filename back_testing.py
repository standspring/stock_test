from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pandas as pd
import yfinance as yf

from strategies import InfiniteBuyingV3Strategy, InfiniteOrder


DATA_DIR = Path("data")


def round_price(price: float) -> float:
    return round(float(price), 2)


@dataclass(frozen=True)
class BacktestConfig:
    ticker: str
    initial_cash: float = 10_000_000
    years: int = 5
    commission_rate: float = 0.00015
    tax_rate: float = 0.0018
    short_window: int = 20
    long_window: int = 60
    strategy: str = "ma"
    target_profit_pct: float | None = None


@dataclass
class Portfolio:
    cash: float
    shares: int = 0
    cost_basis: float = 0.0

    def value(self, close_price: float) -> float:
        return self.cash + self.shares * close_price

    @property
    def avg_cost(self) -> float:
        if self.shares <= 0:
            return 0.0
        return self.cost_basis / self.shares


class Strategy(Protocol):
    def decide(self, history: pd.DataFrame, portfolio: Portfolio) -> str:
        """Return one of: BUY, SELL, HOLD."""


class MovingAverageStrategy:
    """Example strategy: hold while short moving average is above long moving average."""

    def __init__(self, short_window: int = 20, long_window: int = 60) -> None:
        if short_window >= long_window:
            raise ValueError("short_window must be smaller than long_window")
        self.short_window = short_window
        self.long_window = long_window

    def decide(self, history: pd.DataFrame, portfolio: Portfolio) -> str:
        if len(history) < self.long_window:
            return "HOLD"

        short_ma = history["close"].tail(self.short_window).mean()
        long_ma = history["close"].tail(self.long_window).mean()

        if short_ma > long_ma and portfolio.shares == 0:
            return "BUY"
        if short_ma <= long_ma and portfolio.shares > 0:
            return "SELL"
        return "HOLD"


def normalize_ohlc(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    data = data.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    data.index.name = "date"
    data = data.reset_index()
    data["date"] = pd.to_datetime(data["date"]).dt.date

    required = ["date", "open", "high", "low", "close", "volume"]
    data = data[required].dropna().sort_values("date").reset_index(drop=True)
    for column in ["open", "high", "low", "close"]:
        data[column] = data[column].map(round_price)
    return data


def fetch_daily_ohlc(ticker: str, years: int = 5, use_cache: bool = True) -> pd.DataFrame:
    DATA_DIR.mkdir(exist_ok=True)
    cache_path = DATA_DIR / f"{ticker.replace('.', '_')}_{years}y_daily.csv"

    if use_cache and cache_path.exists():
        data = pd.read_csv(cache_path, parse_dates=["date"])
        data["date"] = data["date"].dt.date
        for column in ["open", "high", "low", "close"]:
            data[column] = data[column].map(round_price)
        return data

    raw = yf.download(
        ticker,
        period=f"{years}y",
        interval="1d",
        auto_adjust=False,
        progress=False,
    )
    data = normalize_ohlc(raw)

    if data.empty:
        raise RuntimeError(f"No daily OHLC data found for ticker: {ticker}")

    data.to_csv(cache_path, index=False)
    return data


def buy_with_all_cash(portfolio: Portfolio, price: float, commission_rate: float) -> int:
    buyable_shares = int(portfolio.cash // (price * (1 + commission_rate)))
    if buyable_shares <= 0:
        return 0

    cost = buyable_shares * price
    commission = cost * commission_rate
    portfolio.cash -= cost + commission
    portfolio.shares += buyable_shares
    portfolio.cost_basis += cost
    return buyable_shares


def sell_shares(portfolio: Portfolio, shares_to_sell: int, price: float, commission_rate: float, tax_rate: float) -> tuple[int, float]:
    shares_to_sell = min(shares_to_sell, portfolio.shares)
    if shares_to_sell <= 0:
        return 0, 0.0

    basis_sold = portfolio.avg_cost * shares_to_sell
    proceeds = shares_to_sell * price
    commission = proceeds * commission_rate
    tax = proceeds * tax_rate
    portfolio.cash += proceeds - commission - tax
    portfolio.shares -= shares_to_sell
    portfolio.cost_basis -= basis_sold
    if portfolio.shares == 0:
        portfolio.cost_basis = 0.0
    realized_profit = proceeds - commission - tax - basis_sold
    return shares_to_sell, realized_profit


def buy_with_amount(portfolio: Portfolio, amount: float, price: float, commission_rate: float) -> int:
    usable_cash = min(amount, portfolio.cash)
    buyable_shares = int(usable_cash // (price * (1 + commission_rate)))
    if buyable_shares <= 0:
        return 0

    cost = buyable_shares * price
    commission = cost * commission_rate
    portfolio.cash -= cost + commission
    portfolio.shares += buyable_shares
    portfolio.cost_basis += cost
    return buyable_shares


def sell_all(portfolio: Portfolio, price: float, commission_rate: float, tax_rate: float) -> int:
    shares_sold, _ = sell_shares(portfolio, portfolio.shares, price, commission_rate, tax_rate)
    return shares_sold


def run_backtest(data: pd.DataFrame, strategy: Strategy, config: BacktestConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    portfolio = Portfolio(cash=config.initial_cash)
    daily_records: list[dict] = []
    trade_records: list[dict] = []

    for i in range(len(data)):
        today = data.iloc[i]
        history = data.iloc[: i + 1]
        close_price = round_price(today["close"])
        decision = strategy.decide(history, portfolio)
        executed = "HOLD"
        shares_changed = 0

        # This starter engine executes decisions at the day's close.
        if decision == "BUY":
            shares_changed = buy_with_all_cash(portfolio, close_price, config.commission_rate)
            executed = "BUY" if shares_changed else "HOLD"
        elif decision == "SELL":
            shares_changed = sell_all(portfolio, close_price, config.commission_rate, config.tax_rate)
            executed = "SELL" if shares_changed else "HOLD"

        total_value = portfolio.value(close_price)
        daily_records.append(
            {
                "date": today["date"],
                "open": today["open"],
                "high": today["high"],
                "low": today["low"],
                "close": close_price,
                "decision": decision,
                "executed": executed,
                "shares": portfolio.shares,
                "avg_cost": round(portfolio.avg_cost, 4),
                "cash": round(portfolio.cash, 2),
                "total_value": round(total_value, 2),
                "return_pct": round((total_value / config.initial_cash - 1) * 100, 4),
            }
        )

        if executed in {"BUY", "SELL"}:
            trade_records.append(
                {
                    "date": today["date"],
                    "side": executed,
                    "price": round_price(close_price),
                    "shares": shares_changed,
                    "cash_after": round(portfolio.cash, 2),
                    "total_value_after": round(total_value, 2),
                }
            )

    return pd.DataFrame(daily_records), pd.DataFrame(trade_records)


def run_infinite_buying_v3_backtest(data: pd.DataFrame, config: BacktestConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    portfolio = Portfolio(cash=config.initial_cash)
    strategy = InfiniteBuyingV3Strategy(
        ticker=config.ticker,
        initial_cash=config.initial_cash,
        target_profit_pct=config.target_profit_pct,
    )
    daily_records: list[dict] = []
    trade_records: list[dict] = []

    for _, today in data.iterrows():
        close_price = round_price(today["close"])
        high_price = round_price(today["high"])
        orders = strategy.orders(portfolio)
        executed_labels: list[str] = []

        sell_orders = [order for order in orders if order.side == "SELL"]
        buy_orders = [order for order in orders if order.side == "BUY"]

        for order in sell_orders:
            if portfolio.shares <= 0:
                break

            fill_price = get_sell_fill_price(order, close_price, high_price)
            if fill_price is None:
                continue

            shares_sold, realized_profit = sell_shares(
                portfolio,
                order.shares or 0,
                fill_price,
                config.commission_rate,
                config.tax_rate,
            )
            if shares_sold <= 0:
                continue

            strategy.after_sell(realized_profit, portfolio)
            executed_labels.append(order.label)
            trade_records.append(
                {
                    "date": today["date"],
                    "side": "SELL",
                    "order_type": order.order_type,
                    "label": order.label,
                    "price": round_price(fill_price),
                    "shares": shares_sold,
                    "realized_profit": round(realized_profit, 2),
                    "avg_cost_after": round(portfolio.avg_cost, 4),
                    "cash_after": round(portfolio.cash, 2),
                    "unit_amount_after": round(strategy.unit_amount, 2),
                }
            )

        if portfolio.shares > 0 or not executed_labels:
            for order in buy_orders:
                fill_price = get_buy_fill_price(order, close_price)
                if fill_price is None:
                    continue

                shares_bought = buy_with_amount(
                    portfolio,
                    order.amount or 0.0,
                    fill_price,
                    config.commission_rate,
                )
                if shares_bought <= 0:
                    continue

                executed_labels.append(order.label)
                trade_records.append(
                    {
                        "date": today["date"],
                        "side": "BUY",
                        "order_type": order.order_type,
                        "label": order.label,
                    "price": round_price(fill_price),
                        "shares": shares_bought,
                        "realized_profit": 0.0,
                        "avg_cost_after": round(portfolio.avg_cost, 4),
                        "cash_after": round(portfolio.cash, 2),
                        "unit_amount_after": round(strategy.unit_amount, 2),
                    }
                )

        total_value = portfolio.value(close_price)
        daily_records.append(
            {
                "date": today["date"],
                "open": today["open"],
                "high": today["high"],
                "low": today["low"],
                "close": close_price,
                "decision": "V3",
                "executed": ",".join(executed_labels) if executed_labels else "HOLD",
                "t_value": strategy.t_value(portfolio),
                "star_pct": round(strategy.star_pct(portfolio), 4),
                "unit_amount": round(strategy.unit_amount, 2),
                "shares": portfolio.shares,
                "avg_cost": round(portfolio.avg_cost, 4),
                "cost_basis": round(portfolio.cost_basis, 2),
                "cash": round(portfolio.cash, 2),
                "total_value": round(total_value, 2),
                "return_pct": round((total_value / config.initial_cash - 1) * 100, 4),
            }
        )

    return pd.DataFrame(daily_records), pd.DataFrame(trade_records)


def get_buy_fill_price(order: InfiniteOrder, close_price: float) -> float | None:
    if order.order_type != "LOC":
        return None
    if order.limit_price is None:
        return None
    limit_price = round_price(order.limit_price)
    close_price = round_price(close_price)
    if close_price <= limit_price:
        return close_price
    return None


def get_sell_fill_price(order: InfiniteOrder, close_price: float, high_price: float) -> float | None:
    close_price = round_price(close_price)
    high_price = round_price(high_price)
    if order.order_type == "MOC":
        return close_price
    if order.limit_price is None:
        return None
    limit_price = round_price(order.limit_price)
    if order.order_type == "LOC" and close_price >= limit_price:
        return close_price
    if order.order_type == "LIMIT" and high_price >= limit_price:
        return limit_price
    return None


def summarize(result: pd.DataFrame, trades: pd.DataFrame, config: BacktestConfig) -> dict:
    first_value = config.initial_cash
    last_value = float(result.iloc[-1]["total_value"])
    total_return_pct = (last_value / first_value - 1) * 100

    running_max = result["total_value"].cummax()
    drawdown = (result["total_value"] / running_max - 1) * 100

    return {
        "ticker": config.ticker,
        "strategy": config.strategy,
        "start_date": result.iloc[0]["date"],
        "end_date": result.iloc[-1]["date"],
        "initial_cash": round(first_value, 2),
        "final_value": round(last_value, 2),
        "total_return_pct": round(total_return_pct, 2),
        "max_drawdown_pct": round(float(drawdown.min()), 2),
        "trade_count": len(trades),
    }


def save_outputs(ticker: str, result: pd.DataFrame, trades: pd.DataFrame, strategy_name: str) -> tuple[Path, Path]:
    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    safe_ticker = ticker.replace(".", "_")
    result_path = output_dir / f"{safe_ticker}_{strategy_name}_daily_backtest.csv"
    trades_path = output_dir / f"{safe_ticker}_{strategy_name}_trades.csv"

    result.to_csv(result_path, index=False)
    trades.to_csv(trades_path, index=False)
    return result_path, trades_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily OHLC backtesting starter.")
    parser.add_argument("--ticker", required=True, help="Ticker symbol. Examples: AAPL, MSFT, 005930.KS")
    parser.add_argument("--cash", type=float, default=10_000_000, help="Initial cash amount.")
    parser.add_argument("--years", type=int, default=5, help="Years of daily data to download.")
    parser.add_argument("--strategy", choices=["ma", "v3"], default="ma", help="Backtest strategy.")
    parser.add_argument("--short", type=int, default=20, help="Short moving-average window.")
    parser.add_argument("--long", type=int, default=60, help="Long moving-average window.")
    parser.add_argument("--target-profit", type=float, default=None, help="V3 target profit pct. Defaults: SOXL 20, TQQQ 15.")
    parser.add_argument("--no-cache", action="store_true", help="Download fresh data instead of reading cached CSV.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = BacktestConfig(
        ticker=args.ticker,
        initial_cash=args.cash,
        years=args.years,
        short_window=args.short,
        long_window=args.long,
        strategy=args.strategy,
        target_profit_pct=args.target_profit,
    )

    data = fetch_daily_ohlc(config.ticker, config.years, use_cache=not args.no_cache)
    if config.strategy == "v3":
        result, trades = run_infinite_buying_v3_backtest(data, config)
    else:
        strategy = MovingAverageStrategy(config.short_window, config.long_window)
        result, trades = run_backtest(data, strategy, config)

    summary = summarize(result, trades, config)
    result_path, trades_path = save_outputs(config.ticker, result, trades, config.strategy)

    print("Backtest summary")
    for key, value in summary.items():
        print(f"- {key}: {value}")
    print(f"- daily_result_csv: {result_path}")
    print(f"- trades_csv: {trades_path}")


if __name__ == "__main__":
    main()
