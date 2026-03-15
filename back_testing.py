import math
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional

import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt


# =========================
# 설정값
# =========================
TICKER = "SOXL"
START = "2015-01-01"
END = "2025-12-31"

INITIAL_CASH = 10_000_000          # 시작 자금 (원화 느낌의 숫자지만 실제 통화 단위는 임의)
MAX_SPLITS = 20                    # 총 분할 횟수
TAKE_PROFIT_PCT = 0.20             # 평균단가 대비 +7% 익절
BUY_INTERVAL_DAYS = 1              # n 거래일마다 1회 매수
FEE_RATE = 0.0003                  # 매수/매도 수수료 0.05%
SLIPPAGE_RATE = 0.0005             # 슬리피지 0.05%
ALLOW_FRACTIONAL = False           # 소수점 주식 허용 여부(yfinance 백테스트용)
USE_ADJ_CLOSE = True               # 수정종가 사용 여부


# =========================
# 보조 구조체
# =========================
@dataclass
class TradeLog:
    entry_date: str
    exit_date: str
    cycle_no: int
    buy_count: int
    total_invested: float
    avg_buy_price: float
    sell_price: float
    shares: float
    proceeds: float
    pnl: float
    pnl_pct: float
    hold_days: int


# =========================
# 지표 함수
# =========================
def max_drawdown(equity_curve: pd.Series) -> float:
    running_max = equity_curve.cummax()
    drawdown = equity_curve / running_max - 1.0
    return float(drawdown.min())


def cagr(equity_curve: pd.Series) -> float:
    if len(equity_curve) < 2:
        return 0.0
    start_val = float(equity_curve.iloc[0])
    end_val = float(equity_curve.iloc[-1])
    days = (equity_curve.index[-1] - equity_curve.index[0]).days
    if days <= 0 or start_val <= 0:
        return 0.0
    years = days / 365.25
    return (end_val / start_val) ** (1 / years) - 1


def annualized_volatility(daily_returns: pd.Series) -> float:
    daily_returns = daily_returns.dropna()
    if len(daily_returns) < 2:
        return 0.0
    return float(daily_returns.std() * (252 ** 0.5))


# =========================
# 데이터 로드
# =========================
def load_price_data(ticker: str, start: str, end: str, use_adj_close: bool = True) -> pd.DataFrame:
    # yfinance.download() 사용
    df = yf.download(
        tickers=ticker,
        start=start,
        end=end,
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        raise ValueError(f"{ticker} 데이터가 비어 있습니다.")

    # 컬럼 평탄화 대응
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    price_col = "Adj Close" if use_adj_close and "Adj Close" in df.columns else "Close"
    required_cols = [price_col,"High"]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"{col} 컬럼이 없습니다.")

    out = df[required_cols].copy()
    out = out.rename(columns={
        price_col: "price",
        "High": "high",
    })
    out = out.dropna()
    return out


# =========================
# 백테스트 본체
# =========================
def backtest_infinite_buy(
    df: pd.DataFrame,
    initial_cash: float,
    max_splits: int,
    take_profit_pct: float,
    buy_interval_days: int,
    fee_rate: float,
    slippage_rate: float,
    allow_fractional: bool = True,
) -> Dict:
    cash = float(initial_cash)
    shares = 0.0
    avg_cost = 0.0
    invested_amount = 0.0

    split_budget = initial_cash / max_splits

    cycle_no = 0
    cycle_buy_count = 0
    last_buy_bar_idx: Optional[int] = None
    entry_date: Optional[pd.Timestamp] = None

    trade_logs: List[TradeLog] = []
    equity_records = []

    for bar_idx, (dt, row) in enumerate(df.iterrows()):
        raw_price = float(row["price"])

        # 매수는 불리하게, 매도는 불리하게 체결되도록 슬리피지 반영
        buy_fill = raw_price * (1 + slippage_rate)
        sell_fill = raw_price * (1 - slippage_rate)

        # 1) 익절 조건 먼저 검사
        if shares > 0 and raw_price >= avg_cost * (1 + take_profit_pct):
            gross_proceeds = shares * sell_fill
            sell_fee = gross_proceeds * fee_rate
            net_proceeds = gross_proceeds - sell_fee
            cash += net_proceeds

            pnl = net_proceeds - invested_amount
            pnl_pct = pnl / invested_amount if invested_amount > 0 else 0.0

            trade_logs.append(
                TradeLog(
                    entry_date=entry_date.strftime("%Y-%m-%d") if entry_date else dt.strftime("%Y-%m-%d"),
                    exit_date=dt.strftime("%Y-%m-%d"),
                    cycle_no=cycle_no,
                    buy_count=cycle_buy_count,
                    total_invested=invested_amount,
                    avg_buy_price=avg_cost,
                    sell_price=sell_fill,
                    shares=shares,
                    proceeds=net_proceeds,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    hold_days=(dt - entry_date).days if entry_date is not None else 0,
                )
            )

            # 포지션 초기화
            shares = 0.0
            avg_cost = 0.0
            invested_amount = 0.0
            cycle_buy_count = 0
            last_buy_bar_idx = None
            entry_date = None

        # 2) 매수 조건
        can_buy_by_interval = (
            last_buy_bar_idx is None or (bar_idx - last_buy_bar_idx) >= buy_interval_days
        )
        can_buy_by_count = cycle_buy_count < max_splits
        can_buy_by_cash = cash > 0

        if can_buy_by_interval and can_buy_by_count and can_buy_by_cash:
            budget = min(split_budget, cash)

            buy_fee = budget * fee_rate
            net_budget_for_shares = budget - buy_fee
            if net_budget_for_shares > 0:
                buy_shares = net_budget_for_shares / buy_fill

                if not allow_fractional:
                    buy_shares = math.floor(buy_shares)
                    net_budget_for_shares = buy_shares * buy_fill
                    budget = net_budget_for_shares + (net_budget_for_shares * fee_rate)

                if buy_shares > 0:
                    prev_cost_basis = shares * avg_cost
                    shares += buy_shares
                    new_cost_basis = prev_cost_basis + (buy_shares * buy_fill)
                    avg_cost = new_cost_basis / shares

                    cash -= budget
                    invested_amount += budget
                    cycle_buy_count += 1
                    last_buy_bar_idx = bar_idx

                    if cycle_buy_count == 1:
                        cycle_no += 1
                        entry_date = dt

        # 3) 평가금액 기록
        equity = cash + shares * sell_fill  # 당일 청산 가정 평가
        equity_records.append(
            {
                "date": dt,
                "price": raw_price,
                "cash": cash,
                "shares": shares,
                "avg_cost": avg_cost if shares > 0 else 0.0,
                "market_value": shares * sell_fill,
                "equity": equity,
                "cycle_no": cycle_no,
                "buy_count_in_cycle": cycle_buy_count,
            }
        )

    equity_df = pd.DataFrame(equity_records).set_index("date")
    trades_df = pd.DataFrame([asdict(x) for x in trade_logs])

    # 미실현 포지션 반영 최종값
    final_equity = float(equity_df["equity"].iloc[-1]) if not equity_df.empty else initial_cash
    total_return = final_equity / initial_cash - 1.0 if initial_cash > 0 else 0.0

    daily_returns = equity_df["equity"].pct_change().fillna(0.0)
    result = {
        "ticker": TICKER,
        "start": str(df.index.min().date()),
        "end": str(df.index.max().date()),
        "initial_cash": initial_cash,
        "final_equity": final_equity,
        "total_return_pct": total_return * 100,
        "cagr_pct": cagr(equity_df["equity"]) * 100,
        "mdd_pct": max_drawdown(equity_df["equity"]) * 100,
        "annual_vol_pct": annualized_volatility(daily_returns) * 100,
        "num_completed_cycles": len(trades_df),
        "win_rate_pct": (trades_df["pnl"] > 0).mean() * 100 if not trades_df.empty else 0.0,
        "avg_cycle_return_pct": trades_df["pnl_pct"].mean() * 100 if not trades_df.empty else 0.0,
        "avg_hold_days": trades_df["hold_days"].mean() if not trades_df.empty else 0.0,
        "max_buy_count_used": int(trades_df["buy_count"].max()) if not trades_df.empty else 0,
        "open_position_shares": float(equity_df["shares"].iloc[-1]) if not equity_df.empty else 0.0,
        "open_position_avg_cost": float(equity_df["avg_cost"].iloc[-1]) if not equity_df.empty else 0.0,
        "equity_curve": equity_df,
        "trade_logs": trades_df,
    }
    return result


# =========================
# 실행
# =========================
if __name__ == "__main__":
    price_df = load_price_data(TICKER, START, END, USE_ADJ_CLOSE)

    result = backtest_infinite_buy(
        df=price_df,
        initial_cash=INITIAL_CASH,
        max_splits=MAX_SPLITS,
        take_profit_pct=TAKE_PROFIT_PCT,
        buy_interval_days=BUY_INTERVAL_DAYS,
        fee_rate=FEE_RATE,
        slippage_rate=SLIPPAGE_RATE,
        allow_fractional=ALLOW_FRACTIONAL,
    )

    print("=" * 60)
    print(f"Ticker                 : {result['ticker']}")
    print(f"Period                 : {result['start']} ~ {result['end']}")
    print(f"Initial Cash           : {result['initial_cash']:,.0f}")
    print(f"Final Equity           : {result['final_equity']:,.0f}")
    print(f"Total Return           : {result['total_return_pct']:.2f}%")
    print(f"CAGR                   : {result['cagr_pct']:.2f}%")
    print(f"MDD                    : {result['mdd_pct']:.2f}%")
    print(f"Annualized Volatility  : {result['annual_vol_pct']:.2f}%")
    print(f"Completed Cycles       : {result['num_completed_cycles']}")
    print(f"Win Rate               : {result['win_rate_pct']:.2f}%")
    print(f"Avg Cycle Return       : {result['avg_cycle_return_pct']:.2f}%")
    print(f"Avg Hold Days          : {result['avg_hold_days']:.1f}")
    print(f"Max Buy Count Used     : {result['max_buy_count_used']}")
    print(f"Open Position Shares   : {result['open_position_shares']:.6f}")
    print(f"Open Position Avg Cost : {result['open_position_avg_cost']:.4f}")
    print("=" * 60)

    # 거래 로그 저장
    result["trade_logs"].to_csv("soxl_infinite_buy_trade_logs.csv", index=False, encoding="utf-8-sig")
    result["equity_curve"].to_csv("soxl_infinite_buy_equity_curve.csv", encoding="utf-8-sig")

    # 누적자산 곡선
    plt.figure(figsize=(12, 6))
    plt.plot(result["equity_curve"].index, result["equity_curve"]["equity"])
    plt.title("SOXL Infinite Buy Backtest Equity Curve")
    plt.xlabel("Date")
    plt.ylabel("Equity")
    plt.grid(True)
    plt.tight_layout()
    plt.show()