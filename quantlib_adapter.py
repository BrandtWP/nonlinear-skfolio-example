from skfolio.prior import InstrumentAdapter, MarketContext
import QuantLib as ql
from typing import Dict
import datetime as dt
import pandas as pd


def parse_ql_date(date_str):
    if date_str is None:
        return None
    elif isinstance(date_str, ql.Date):
        return date_str
    elif isinstance(date_str, str):
        return ql.Date(date_str, "%Y-%m-%d")
    elif isinstance(date_str, pd.Timestamp) or isinstance(date_str, dt.date):
        return ql.Date(date_str.day, date_str.month, date_str.year)
    else:
        raise ValueError(f"Unrecognized date type {type(date_str)}")


ql_date_to_dt_date = lambda ql_date: dt.date(
    ql_date.year(), ql_date.month(), ql_date.dayOfMonth()
)


class QLMarketContext(MarketContext):
    quotes: Dict[str, ql.SimpleQuote] = {}
    relinkable_handles: Dict[str, "ql.RelinkableHandle"] = {}

    def __init__(
        self,
        date=None,
        ql_env: Dict[str, "ql.QuoteHandle | ql.RelinkableHandle"] | None = None,
        **kwargs,
    ):
        super().__init__(date, **kwargs)

        if ql_env:
            for k, v in ql_env.items():
                if isinstance(v, ql.SimpleQuote):
                    self.quotes[k] = v
                elif getattr(v, "linkTo", False):
                    self.relinkable_handles[k] = v
                else:
                    raise ValueError(
                        f"Unsupported QuantLib object for key '{k}': {type(v)}"
                    )

    def update_eval_date(self):
        ql_date = ql.Date(self.date.day, self.date.month, self.date.year)

        # Avoid triggering update when date is unchanged
        current_date = ql.Settings.instance().evaluationDate
        if ql_date != current_date:
            ql.Settings.instance().evaluationDate = ql_date

    def update_ql_env(self):
        self.update_eval_date()

        for id, quote in self.quotes.items():
            if self.get(id, False):
                quote.setValue(self[id])

        for id, handle in self.relinkable_handles.items():
            if self.get(id, False):
                handle.linkTo(self[id])

    def __sklearn_clone__(self):
        # Unfortunately, QuantLib objects are not cloneable
        # However, we must implement __sklearn_clone__ to conform to the SciKit learn API
        new_ql_env = {}
        for k, v in self.quotes.items():
            new_ql_env[k] = (
                v  # SimpleQuotes are mutable but we assume user handles cloning if needed
            )
        for k, v in self.relinkable_handles.items():
            new_ql_env[k] = (
                v  # RelinkableHandles are mutable but we assume user handles cloning if needed
            )
        return QLMarketContext(date=self.date, ql_env=new_ql_env, **self.data)


class QLInstrumentAdapter(InstrumentAdapter):
    def price(self, market_context: QLMarketContext) -> float:
        market_context.update_ql_env()
        return self.instrument.NPV()

    def cashflow(self, market_context: QLMarketContext) -> float:
        eval_date = parse_ql_date(market_context.date)
        if hasattr(self.instrument, "cashflows"):
            cashflows = self.instrument.cashflows()
            cashflow_amount = 0.0

            for cashflow in cashflows:
                if cashflow.date() == eval_date:
                    cashflow_amount += cashflow.amount()

            if eval_date == self.instrument.maturityDate():
                cashflow_amount -= self.instrument.redemption().amount()

            return cashflow_amount
        return 0.0

    def __sklearn_clone__(self):
        # Unfortunately, QuantLib objects are not cloneable
        # However, we must implement __sklearn_clone__ to conform to the SciKit learn API
        return QLInstrumentAdapter(self.instrument)
