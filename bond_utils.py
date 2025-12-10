import datetime as dt
from typing import Callable

import numpy as np
import pandas as pd
import QuantLib as ql
import sklearn.utils.metadata_routing as skm
from skfolio.prior import (
    EntropyPooling,
    Instrument,
    MarketContext,
    PortfolioInstruments,
)
from tqdm import tqdm

from quantlib_adapter import QLInstrumentAdapter, QLMarketContext, parse_ql_date


def create_fixed_rate_bond(
    coupon_rate,
    issue_date,
    maturity_date,
    calendar,
    day_count,
    coupon_frequency,
    curve_handle,
    spread_quote,
) -> ql.FixedRateBond:
    business_convention = ql.Following
    settlement_days = 0
    face_amount = 100.0
    issue_date = parse_ql_date(issue_date)
    maturity_date = parse_ql_date(maturity_date)

    schedule = ql.Schedule(
        issue_date,
        maturity_date,
        ql.Period(coupon_frequency),
        calendar,
        business_convention,
        business_convention,
        ql.DateGeneration.Backward,
        False,  # end of month
    )
    bond = ql.FixedRateBond(
        settlement_days, face_amount, schedule, [coupon_rate], day_count
    )

    pricing_curve = ql.YieldTermStructureHandle(
        ql.ZeroSpreadedTermStructure(curve_handle, ql.QuoteHandle(spread_quote))
    )
    pricing_engine = ql.DiscountingBondEngine(pricing_curve)
    bond.setPricingEngine(pricing_engine)

    return bond


# Calculate the accrued coupons and add them to the clean prices to get dirty prices
def calculate_accrued_coupons(
    dates,
    portfolio_instruments: PortfolioInstruments,
) -> pd.DataFrame:
    accrued_coupons = {}
    for date in dates:
        ql.Settings.instance().evaluationDate = parse_ql_date(date)
        accrued_coupons[date] = {}
        for isin, bond in portfolio_instruments.items():
            accrued_coupons[date][isin] = bond.accruedAmount()

    return pd.DataFrame.from_dict(accrued_coupons, orient="index")


def calculate_z_spread(
    market_price: float, bond: Instrument, market_context: MarketContext, spread_id: str
) -> float:
    # The root-finding function for finding spread that makes the bond
    # price close to market price

    def z_spread_func(z_spread):
        market_context[spread_id] = z_spread
        return bond.price(market_context) - market_price

    # Create and configure the root finder.
    # It could be done with a solver from scipy, for example,
    # but the implementation QuantLib works best here in my experience.
    accuracy = 1e-6
    min = -1e-5
    max = 0.2
    solver = ql.Ridder()
    solver.setMaxEvaluations(10000)

    # Solve the spread
    z_spread = solver.solve(
        z_spread_func, accuracy, market_context[spread_id], min, max
    )

    return z_spread


def bond_prices_to_z_spreads(
    market_quotes: pd.DataFrame,
    portfolio_instruments: PortfolioInstruments,
    reference_market_context: MarketContext,
    market_data_parser: Callable[
        [pd.Series, MarketContext, PortfolioInstruments], MarketContext
    ]
    | None = None,
) -> pd.DataFrame:
    z_spreads = {}
    for date, row in tqdm(list(market_quotes.iterrows())):
        if market_data_parser:
            reference_market_context = market_data_parser(
                row, reference_market_context, portfolio_instruments
            )
        else:
            reference_market_context.update_from_series(row)

        ql_date = parse_ql_date(date)
        z_spreads[date] = {}
        for bond_id in portfolio_instruments.keys():
            bond = portfolio_instruments[bond_id]
            market_price = row[bond_id]
            # Make sure to handle bonds before inception or after maturity
            if np.isnan(market_price) or (bond.maturityDate() == ql_date):
                z_spread = np.nan
            else:
                try:
                    z_spread = calculate_z_spread(
                        market_price,
                        bond,
                        reference_market_context,
                        f"z_spread_{bond_id}",
                    )
                except Exception as e:
                    print(bond_id, date, market_price)
                    print(bond.price(reference_market_context))
                    raise e
            z_spreads[date]["z_spread_" + bond_id] = z_spread

    return pd.DataFrame.from_dict(z_spreads, orient="index")


def build_bootstrapped_curve(
    rates_row: pd.Series, quotes_dict
) -> ql.YieldTermStructure:
    index = ql.Sofr()
    settlement_days = 2

    ois_helpers = ql.RateHelperVector()

    cal = ql.BespokeCalendar("my-cal")
    cal.addWeekend(ql.Saturday)
    cal.addWeekend(ql.Sunday)

    for tenor, quote in rates_row.items():
        q = quotes_dict[tenor]
        q.setValue(quote)
        ois_helpers.append(
            ql.OISRateHelper(
                settlement_days,
                ql.Period(tenor),
                ql.QuoteHandle(q),
                index,
                paymentFrequency=ql.Annual,
            )
        )

    return ql.PiecewiseLinearZero(
        0,
        cal,
        ois_helpers,
        ql.Actual360(),
    )


# Function to build zero curve from risky curve nodes
def build_interpolated_curve(date, yield_row):
    """
    Build a zero curve from yield curve nodes using linear interpolation.

    Parameters:
    - date: ql.Date for the evaluation date
    - yield_row: pandas Series with tenor columns (e.g., '3M', '1Y', '5Y', etc.)

    Returns:
    - ql.YieldTermStructureHandle
    """
    # Set the evaluation date
    date = parse_ql_date(date)
    ql.Settings.instance().evaluationDate = date

    # Prepare dates and rates from the yield curve nodes
    dates = [date]
    rates = [yield_row.get("3M", 0.0) / 100.0]  # Overnight rate as the first point
    calendar = ql.TARGET()
    day_count = ql.Actual365Fixed()

    for tenor_str, rate in yield_row.items():
        if pd.isna(rate):
            continue

        # Convert rate from percentage to decimal
        rate_decimal = rate / 100.0

        # Parse tenor string (e.g., "3M", "1Y", "5Y", "10Y", "30Y")
        if tenor_str.endswith("M"):
            months = int(tenor_str[:-1])
            period = ql.Period(months, ql.Months)
        elif tenor_str.endswith("Y"):
            years = int(tenor_str[:-1])
            period = ql.Period(years, ql.Years)
        else:
            continue

        # Calculate the maturity date for this tenor
        maturity_date = calendar.advance(date, period)

        dates.append(maturity_date)
        rates.append(rate_decimal)

    # Sort by dates to ensure proper ordering
    sorted_pairs = sorted(zip(dates, rates), key=lambda x: x[0])
    dates, rates = zip(*sorted_pairs) if sorted_pairs else ([], [])

    # Build the zero curve using linear interpolation
    curve = ql.ZeroCurve(
        list(dates),
        list(rates),
        day_count,
        calendar,
        ql.Linear(),
        ql.Compounded,
        ql.Annual,
    )

    curve.enableExtrapolation()

    return curve


def nelson_siegel(tau, beta0, beta1, beta2, lambda_param, log=True):
    """
    Calculate Nelson-Siegel yield for a given maturity.

    Parameters:
    - tau: maturity (in years)
    - beta0: level factor
    - beta1: slope factor
    - beta2: curvature factor
    - lambda_param: decay parameter

    Returns:
    - yield rate
    """
    if tau == 0:
        curve_yield = beta0 + beta1
    else:
        factor1 = (1 - np.exp(-lambda_param * tau)) / (lambda_param * tau)
        factor2 = factor1 - np.exp(-lambda_param * tau)
        curve_yield = beta0 + beta1 * factor1 + beta2 * factor2
    if log:
        return np.exp(curve_yield)
    return curve_yield


class NelsonSiegelCurve:
    def __init__(self, level, slope, curvature, lambda_param=0.0609):
        self.level = level
        self.slope = slope
        self.curvature = curvature
        self.lambda_param = lambda_param

    def yield_at(self, tau):
        return nelson_siegel(tau, self.level, self.slope, self.curvature, self.lambda_param)

def fit_nelson_siegel(z_spreads, maturities, lambda_param=0.0609):
    """
    Fit Nelson-Siegel parameters using ordinary least squares.

    Parameters:
    - z_spreads: array-like of z-spread values
    - maturities: array-like of maturities (in years)
    - lambda_param: decay parameter (fixed, default 0.0609 is common choice)

    Returns:
    - NelsonSiegelCurve object with fitted parameters
    """
    z_spreads = np.array(z_spreads)
    maturities = np.array(maturities)

    # Build the design matrix X for OLS
    # y = beta0 + beta1 * factor1 + beta2 * factor2
    n = len(maturities)
    X = np.zeros((n, 3))

    for i, tau in enumerate(maturities):
        if tau == 0:
            X[i, 0] = 1  # beta0
            X[i, 1] = 1  # beta1
            X[i, 2] = 0  # beta2
        else:
            factor1 = (1 - np.exp(-lambda_param * tau)) / (lambda_param * tau)
            factor2 = factor1 - np.exp(-lambda_param * tau)

            X[i, 0] = 1  # beta0
            X[i, 1] = factor1  # beta1
            X[i, 2] = factor2  # beta2

    # Solve OLS: beta = (X'X)^-1 X'y
    betas = np.linalg.lstsq(X, np.log(z_spreads), rcond=None)[0]

    level, slope, curvature = betas

    return NelsonSiegelCurve(level, slope, curvature, lambda_param)


class DynamicEntropyPooling(EntropyPooling):
    def __init__(
        self,
        mean_views_generator: Callable[[pd.DataFrame], list[str]] | None = None,
        variance_views_generator: Callable[[pd.DataFrame], list[str]] | None = None,
        correlation_views_generator: Callable[[pd.DataFrame], list[str]] | None = None,
        skew_views_generator: Callable[[pd.DataFrame], list[str]] | None = None,
        kurtosis_views_generator: Callable[[pd.DataFrame], list[str]] | None = None,
        value_at_risk_views_generator: Callable[[pd.DataFrame], list[str]]
        | None = None,
        cvar_views_generator: Callable[[pd.DataFrame], list[str]] | None = None,
        **kwargs,
    ) -> None:
        self.mean_views_generator = mean_views_generator
        self.variance_views_generator = variance_views_generator
        self.correlation_views_generator = correlation_views_generator
        self.skew_views_generator = skew_views_generator
        self.kurtosis_views_generator = kurtosis_views_generator
        self.value_at_risk_views_generator = value_at_risk_views_generator
        self.cvar_views_generator = cvar_views_generator
        super().__init__(**kwargs)

    def get_metadata_routing(self):
        router = (
            skm.MetadataRouter(owner=self.__class__.__name__)
            .add_self_request(self)
            .add(
                prior_estimator=self.prior_estimator,
                method_mapping=skm.MethodMapping().add(caller="fit", callee="fit"),
            )
        )
        return router

    def fit(self, X, y=None, signal=None, **fit_params) -> "DynamicEntropyPooling":
        if signal is not None:
            # noinspection PyTypeChecker
            fit_params["signal"] = signal
        else:
            raise ValueError(
                "signal metadata must be provided to fit DynamicEntropyPooling."
            )

        if self.mean_views_generator is not None:
            self.mean_views_ = self.mean_views_generator(signal)
        if self.variance_views_generator is not None:
            self.variance_views_ = self.variance_views_generator(signal)
        if self.correlation_views_generator is not None:
            self.correlation_views_ = self.correlation_views_generator(signal)
        if self.skew_views_generator is not None:
            self.skew_views_ = self.skew_views_generator(signal)
        if self.kurtosis_views_generator is not None:
            self.kurtosis_views_ = self.kurtosis_views_generator(signal)
        if self.value_at_risk_views_generator is not None:
            self.value_at_risk_views_ = self.value_at_risk_views_generator(signal)
        if self.cvar_views_generator is not None:
            self.cvar_views_ = self.cvar_views_generator(signal)

        super().fit(X, y, **fit_params)
        return self
