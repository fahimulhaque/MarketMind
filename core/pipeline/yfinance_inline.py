"""Inline yfinance quarterly financial data fetch.

When the financial_periods DB table has no quarterly data for a ticker,
this module fetches income statement, balance sheet, and cash flow data
directly from yfinance and upserts it into the DB.

This avoids requiring FMP/Alpha Vantage API keys for basic quarterly data.
yfinance provides ~4 quarters of data for most US-listed equities.
"""

from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def _safe_float(val) -> float | None:
    """Safely convert a value to float, handling NaN and None."""
    if val is None:
        return None
    try:
        f = float(val)
        import math
        return None if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return None


def inline_fetch_yfinance_quarterly(ticker: str) -> int:
    """Fetch quarterly financials from yfinance and store in DB.

    Returns number of periods stored.
    """
    try:
        import yfinance as yf
        from core import db
    except ImportError as exc:
        logger.warning("yfinance not available for inline fetch: %s", exc)
        return 0

    try:
        t = yf.Ticker(ticker)
    except Exception as exc:
        logger.warning("yfinance Ticker init failed for %s: %s", ticker, exc)
        return 0

    stored = 0

    # --- Income Statement ---
    try:
        inc = t.quarterly_income_stmt
        if inc is not None and not inc.empty:
            for col_date in inc.columns:
                period_end = col_date.strftime("%Y-%m-%d") if hasattr(col_date, "strftime") else str(col_date)[:10]
                col = inc[col_date]

                income_data = {
                    "revenue": _safe_float(col.get("Total Revenue", col.get("Revenue"))),
                    "costOfRevenue": _safe_float(col.get("Cost Of Revenue")),
                    "grossProfit": _safe_float(col.get("Gross Profit")),
                    "operatingIncome": _safe_float(col.get("Operating Income")),
                    "netIncome": _safe_float(col.get("Net Income", col.get("Net Income Common Stockholders"))),
                    "ebitda": _safe_float(col.get("EBITDA")),
                    "epsdiluted": _safe_float(col.get("Diluted EPS")),
                    "eps": _safe_float(col.get("Basic EPS")),
                    "researchAndDevelopmentExpenses": _safe_float(col.get("Research And Development")),
                }

                # Compute ratios if possible
                rev = income_data.get("revenue")
                gp = income_data.get("grossProfit")
                oi = income_data.get("operatingIncome")
                ni = income_data.get("netIncome")
                if rev and rev > 0:
                    if gp is not None:
                        income_data["grossProfitRatio"] = round(gp / rev, 4)
                    if oi is not None:
                        income_data["operatingIncomeRatio"] = round(oi / rev, 4)
                    if ni is not None:
                        income_data["netIncomeRatio"] = round(ni / rev, 4)

                # Determine fiscal quarter
                dt = col_date if hasattr(col_date, "month") else datetime.strptime(period_end, "%Y-%m-%d")
                fq = (dt.month - 1) // 3 + 1

                try:
                    db.upsert_financial_period(
                        entity_id=None,
                        ticker=ticker,
                        period_type="quarterly",
                        period_end_date=period_end,
                        fiscal_year=dt.year,
                        fiscal_quarter=fq,
                        source_provider="yfinance",
                        income_statement=income_data,
                    )
                    stored += 1
                except Exception as exc:
                    logger.debug("yfinance income upsert error for %s %s: %s", ticker, period_end, exc)
    except Exception as exc:
        logger.warning("yfinance quarterly income fetch failed for %s: %s", ticker, exc)

    # --- Balance Sheet ---
    try:
        bs = t.quarterly_balance_sheet
        if bs is not None and not bs.empty:
            for col_date in bs.columns:
                period_end = col_date.strftime("%Y-%m-%d") if hasattr(col_date, "strftime") else str(col_date)[:10]
                col = bs[col_date]

                balance_data = {
                    "totalAssets": _safe_float(col.get("Total Assets")),
                    "totalLiabilities": _safe_float(col.get("Total Liabilities Net Minority Interest", col.get("Total Liab"))),
                    "totalEquity": _safe_float(col.get("Total Equity Gross Minority Interest", col.get("Stockholders Equity"))),
                    "totalDebt": _safe_float(col.get("Total Debt")),
                    "cashAndEquivalents": _safe_float(col.get("Cash And Cash Equivalents")),
                    "netDebt": _safe_float(col.get("Net Debt")),
                    "totalCurrentAssets": _safe_float(col.get("Current Assets")),
                    "totalCurrentLiabilities": _safe_float(col.get("Current Liabilities")),
                }

                try:
                    db.upsert_financial_period(
                        entity_id=None,
                        ticker=ticker,
                        period_type="quarterly",
                        period_end_date=period_end,
                        source_provider="yfinance",
                        balance_sheet=balance_data,
                    )
                except Exception as exc:
                    logger.debug("yfinance balance upsert error for %s %s: %s", ticker, period_end, exc)
    except Exception as exc:
        logger.warning("yfinance quarterly balance sheet fetch failed for %s: %s", ticker, exc)

    # --- Cash Flow ---
    try:
        cf = t.quarterly_cashflow
        if cf is not None and not cf.empty:
            for col_date in cf.columns:
                period_end = col_date.strftime("%Y-%m-%d") if hasattr(col_date, "strftime") else str(col_date)[:10]
                col = cf[col_date]

                cashflow_data = {
                    "operatingCashFlow": _safe_float(col.get("Operating Cash Flow")),
                    "capitalExpenditure": _safe_float(col.get("Capital Expenditure")),
                    "freeCashFlow": _safe_float(col.get("Free Cash Flow")),
                    "dividendsPaid": _safe_float(col.get("Common Stock Dividend Paid", col.get("Cash Dividends Paid"))),
                    "stockRepurchased": _safe_float(col.get("Repurchase Of Capital Stock")),
                }

                try:
                    db.upsert_financial_period(
                        entity_id=None,
                        ticker=ticker,
                        period_type="quarterly",
                        period_end_date=period_end,
                        source_provider="yfinance",
                        cash_flow=cashflow_data,
                    )
                except Exception as exc:
                    logger.debug("yfinance cashflow upsert error for %s %s: %s", ticker, period_end, exc)
    except Exception as exc:
        logger.warning("yfinance quarterly cashflow fetch failed for %s: %s", ticker, exc)

    if stored:
        logger.info("yfinance inline: stored %d quarterly periods for %s", stored, ticker)
    return stored
