import datetime
import json
import os
import tempfile

import pytz

from broker import KoreaInvestmentBroker


class PaperBroker(KoreaInvestmentBroker):
    def __init__(self, app_key, app_secret, cano, acnt_prdt_cd="01", initial_cash=100000.0, state_file=None):
        super().__init__(app_key, app_secret, cano, acnt_prdt_cd)
        self.initial_cash = max(0.0, float(initial_cash or 0.0))
        self.state_file = state_file or f"data/paper_broker_state_{cano}.json"
        self.est = pytz.timezone("US/Eastern")
        self._ensure_state()

    def _default_state(self):
        return {
            "cash": round(self.initial_cash, 2),
            "holdings": {},
            "executions": [],
            "orders": [],
            "next_order_seq": 1,
        }

    def _ensure_state(self):
        dir_name = os.path.dirname(self.state_file) or "."
        os.makedirs(dir_name, exist_ok=True)
        if not os.path.exists(self.state_file):
            self._save_state(self._default_state())

    def _load_state(self):
        self._ensure_state()
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                defaults = self._default_state()
                for key, value in defaults.items():
                    data.setdefault(key, value)
                return data
        except Exception:
            pass
        return self._default_state()

    def _save_state(self, data):
        dir_name = os.path.dirname(self.state_file) or "."
        os.makedirs(dir_name, exist_ok=True)
        fd = None
        temp_path = None
        try:
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                fd = None
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.state_file)
            temp_path = None
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    def _now_est(self):
        return datetime.datetime.now(self.est)

    def _today_compact(self):
        return self._now_est().strftime("%Y%m%d")

    def _next_order_id(self, state):
        seq = int(state.get("next_order_seq", 1))
        state["next_order_seq"] = seq + 1
        return f"P{self._today_compact()}_{seq:06d}"

    def _resolve_fill_price(self, ticker, side, price, order_type):
        if order_type in ["MOC", "MOO"] or float(price or 0.0) <= 0.0:
            ref_price = float(self.get_ask_price(ticker) or 0.0) if side == "BUY" else float(self.get_bid_price(ticker) or 0.0)
            if ref_price <= 0.0:
                ref_price = float(self.get_current_price(ticker) or 0.0)
            return self._ceil_2(ref_price)
        return self._ceil_2(float(price or 0.0))

    def get_account_balance(self):
        state = self._load_state()
        holdings = {}
        for ticker, item in (state.get("holdings") or {}).items():
            qty = int(float(item.get("qty") or 0))
            avg = float(item.get("avg") or 0.0)
            if qty > 0:
                holdings[ticker] = {"qty": qty, "ord_psbl_qty": qty, "avg": round(avg, 4)}
        return round(float(state.get("cash", 0.0)), 2), holdings

    def get_unfilled_orders_detail(self, ticker):
        state = self._load_state()
        return [o for o in (state.get("orders") or []) if o.get("pdno") == ticker and o.get("status") == "OPEN"]

    def get_unfilled_orders(self, ticker):
        return [o.get("odno") for o in self.get_unfilled_orders_detail(ticker)]

    def cancel_all_orders_safe(self, ticker, side=None):
        state = self._load_state()
        changed = False
        for order in state.get("orders") or []:
            if order.get("pdno") != ticker or order.get("status") != "OPEN":
                continue
            if side == "BUY" and order.get("sll_buy_dvsn_cd") != "02":
                continue
            if side == "SELL" and order.get("sll_buy_dvsn_cd") != "01":
                continue
            order["status"] = "CANCELLED"
            changed = True
        if changed:
            self._save_state(state)
        return True

    def cancel_order(self, ticker, order_id):
        state = self._load_state()
        changed = False
        for order in state.get("orders") or []:
            if order.get("pdno") == ticker and order.get("odno") == order_id and order.get("status") == "OPEN":
                order["status"] = "CANCELLED"
                changed = True
                break
        if changed:
            self._save_state(state)

    def send_order(self, ticker, side, qty, price, order_type="LIMIT"):
        try:
            order_qty = int(float(qty))
        except (TypeError, ValueError):
            return {"rt_cd": "999", "msg1": f"Invalid order quantity type: {qty!r}"}

        if order_qty <= 0:
            return {"rt_cd": "999", "msg1": f"Invalid order quantity: {qty}"}

        fill_price = self._resolve_fill_price(ticker, side, price, order_type)
        if fill_price <= 0.0:
            return {"rt_cd": "999", "msg1": f"Invalid order price: {price}"}

        state = self._load_state()
        holdings = state.setdefault("holdings", {})
        executions = state.setdefault("executions", [])
        orders = state.setdefault("orders", [])
        cash = float(state.get("cash", 0.0))

        current = holdings.get(ticker, {"qty": 0, "avg": 0.0})
        current_qty = int(float(current.get("qty") or 0))
        current_avg = float(current.get("avg") or 0.0)
        gross_amount = round(order_qty * fill_price, 2)

        if side == "BUY":
            if cash + 1e-9 < gross_amount:
                return {"rt_cd": "999", "msg1": f"Paper cash insufficient: need ${gross_amount:.2f}, have ${cash:.2f}"}
            new_qty = current_qty + order_qty
            new_avg = ((current_qty * current_avg) + (order_qty * fill_price)) / new_qty if new_qty > 0 else 0.0
            holdings[ticker] = {"qty": new_qty, "avg": round(new_avg, 4)}
            state["cash"] = round(cash - gross_amount, 2)
            side_cd = "02"
        else:
            if current_qty < order_qty:
                return {"rt_cd": "999", "msg1": f"Paper holdings insufficient: sell {order_qty}, have {current_qty}"}
            new_qty = current_qty - order_qty
            if new_qty > 0:
                holdings[ticker] = {"qty": new_qty, "avg": round(current_avg, 4)}
            else:
                holdings.pop(ticker, None)
            state["cash"] = round(cash + gross_amount, 2)
            side_cd = "01"

        now_est = self._now_est()
        odno = self._next_order_id(state)
        ord_tmd = now_est.strftime("%H%M%S")
        ord_dt = now_est.strftime("%Y%m%d")

        orders.append(
            {
                "odno": odno,
                "pdno": ticker,
                "sll_buy_dvsn_cd": side_cd,
                "ord_qty": str(order_qty),
                "tot_ccld_qty": str(order_qty),
                "ft_ord_unpr3": str(fill_price),
                "ord_unpr": str(fill_price),
                "ovrs_ord_unpr": str(fill_price),
                "ord_dvsn_cd": order_type,
                "status": "FILLED",
                "ord_dt": ord_dt,
                "ord_tmd": ord_tmd,
            }
        )
        executions.append(
            {
                "odno": odno,
                "pdno": ticker,
                "ord_dt": ord_dt,
                "ord_tmd": ord_tmd,
                "sll_buy_dvsn_cd": side_cd,
                "ft_ccld_qty": str(order_qty),
                "ft_ccld_unpr3": str(fill_price),
            }
        )
        self._save_state(state)
        return {"rt_cd": "0", "msg1": "PAPER_FILLED", "odno": odno}

    def get_execution_history(self, ticker, start_date, end_date):
        state = self._load_state()
        items = []
        for ex in state.get("executions") or []:
            if ex.get("pdno") != ticker:
                continue
            ord_dt = str(ex.get("ord_dt") or "")
            if start_date and ord_dt < start_date:
                continue
            if end_date and ord_dt > end_date:
                continue
            items.append(dict(ex))
        return items
