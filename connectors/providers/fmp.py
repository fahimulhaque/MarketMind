"""Financial Modeling Prep (FMP) data provider.

Free tier: 250 API calls/day.  Provides income statements, balance sheets,
cash flow statements, key metrics, company profile, and news.

Docs: https://site.financialmodelingprep.com/developer/docs
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core.config import get_settings
from core import db

logger = logging.getLogger(__name__)

_FMP_BASE = "https://financialmodelingprep.com/api/v3"


class FmpProvider(BaseProvider):
    """FMP structured financial data provider."""

    _daily_limit = 250

    @property
    def provider_name(self) -> str:
        return "fmp"

    def is_configured(self) -> bool:
        return bool(get_settings().fmp_api_key)

    def _get(self, path: str, params: dict | None = None) -> Any:
        """Make an authenticated FMP API call."""
        if not self.rate_limit_ok():
            logger.warning("FMP daily rate limit reached (%d/%d)", self._calls_today, self._daily_limit)
            return None
        settings = get_settings()
        p = {"apikey": settings.fmp_api_key}
        if params:
            p.update(params)
        self._track_call()
        try:
            resp = httpx.get(f"{_FMP_BASE}/{path}", params=p, timeout=20)
            if resp.status_code != 200:
                logger.info("FMP %s returned %d", path, resp.status_code)
                return None
            return resp.json()
        except Exception as exc:
            logger.warning("FMP request failed for %s: %s", path, exc)
            return None

    # -----------------------------------------------------------------
    # Income Statement
    # -----------------------------------------------------------------

    def _fetch_income_statements(self, entity: dict) -> int:
        ticker = entity["ticker"]
        data = self._get(f"income-statement/{ticker}", {"period": "quarter", "limit": "20"})
        if not data or not isinstance(data, list):
            return 0
        stored = 0
        for item in data:
            period_end = item.get("date", "")
            if not period_end:
                continue
            fy = item.get("calendarYear")
            fq = item.get("period", "")  # "Q1", "Q2" etc
            fq_num = int(fq[1]) if fq and len(fq) == 2 and fq[1].isdigit() else None
            income = {
                "revenue": item.get("revenue"),
                "costOfRevenue": item.get("costOfRevenue"),
                "grossProfit": item.get("grossProfit"),
                "grossProfitRatio": item.get("grossProfitRatio"),
                "operatingIncome": item.get("operatingIncome"),
                "operatingIncomeRatio": item.get("operatingIncomeRatio"),
                "netIncome": item.get("netIncome"),
                "netIncomeRatio": item.get("netIncomeRatio"),
                "eps": item.get("eps"),
                "epsdiluted": item.get("epsdiluted"),
                "researchAndDevelopmentExpenses": item.get("researchAndDevelopmentExpenses"),
                "sellingGeneralAndAdministrativeExpenses": item.get("sellingGeneralAndAdministrativeExpenses"),
                "ebitda": item.get("ebitda"),
                "ebitdaratio": item.get("ebitdaratio"),
            }
            try:
                db.upsert_financial_period(
                    entity_id=entity.get("id"),
                    ticker=ticker,
                    period_type="quarterly",
                    period_end_date=period_end,
                    fiscal_year=int(fy) if fy else None,
                    fiscal_quarter=fq_num,
                    source_provider=self.provider_name,
                    income_statement=income,
                )
                stored += 1
            except Exception as exc:
                logger.debug("FMP income store error: %s", exc)
        return stored

    # -----------------------------------------------------------------
    # Balance Sheet
    # -----------------------------------------------------------------

    def _fetch_balance_sheets(self, entity: dict) -> int:
        ticker = entity["ticker"]
        data = self._get(f"balance-sheet-statement/{ticker}", {"period": "quarter", "limit": "20"})
        if not data or not isinstance(data, list):
            return 0
        stored = 0
        for item in data:
            period_end = item.get("date", "")
            if not period_end:
                continue
            balance = {
                "totalAssets": item.get("totalAssets"),
                "totalLiabilities": item.get("totalLiabilities"),
                "totalStockholdersEquity": item.get("totalStockholdersEquity"),
                "cashAndCashEquivalents": item.get("cashAndCashEquivalents"),
                "totalCurrentAssets": item.get("totalCurrentAssets"),
                "totalCurrentLiabilities": item.get("totalCurrentLiabilities"),
                "longTermDebt": item.get("longTermDebt"),
                "totalDebt": item.get("totalDebt"),
                "netDebt": item.get("netDebt"),
                "goodwill": item.get("goodwill"),
                "inventory": item.get("inventory"),
                "netReceivables": item.get("netReceivables"),
                "propertyPlantEquipmentNet": item.get("propertyPlantEquipmentNet"),
            }
            try:
                db.upsert_financial_period(
                    entity_id=entity.get("id"),
                    ticker=ticker,
                    period_type="quarterly",
                    period_end_date=period_end,
                    source_provider=self.provider_name,
                    balance_sheet=balance,
                )
                stored += 1
            except Exception as exc:
                logger.debug("FMP balance store error: %s", exc)
        return stored

    # -----------------------------------------------------------------
    # Cash Flow
    # -----------------------------------------------------------------

    def _fetch_cash_flows(self, entity: dict) -> int:
        ticker = entity["ticker"]
        data = self._get(f"cash-flow-statement/{ticker}", {"period": "quarter", "limit": "20"})
        if not data or not isinstance(data, list):
            return 0
        stored = 0
        for item in data:
            period_end = item.get("date", "")
            if not period_end:
                continue
            cf = {
                "operatingCashFlow": item.get("operatingCashFlow"),
                "capitalExpenditure": item.get("capitalExpenditure"),
                "freeCashFlow": item.get("freeCashFlow"),
                "investingCashFlow": item.get("netCashUsedForInvestingActivites"),
                "financingCashFlow": item.get("netCashUsedProvidedByFinancingActivities"),
                "dividendsPaid": item.get("dividendsPaid"),
                "commonStockRepurchased": item.get("commonStockRepurchased"),
                "depreciationAndAmortization": item.get("depreciationAndAmortization"),
            }
            try:
                db.upsert_financial_period(
                    entity_id=entity.get("id"),
                    ticker=ticker,
                    period_type="quarterly",
                    period_end_date=period_end,
                    source_provider=self.provider_name,
                    cash_flow=cf,
                )
                stored += 1
            except Exception as exc:
                logger.debug("FMP cash flow store error: %s", exc)
        return stored

    # -----------------------------------------------------------------
    # Key Metrics
    # -----------------------------------------------------------------

    def _fetch_key_metrics(self, entity: dict) -> int:
        ticker = entity["ticker"]
        data = self._get(f"key-metrics/{ticker}", {"period": "quarter", "limit": "20"})
        if not data or not isinstance(data, list):
            return 0
        stored = 0
        for item in data:
            period_end = item.get("date", "")
            if not period_end:
                continue
            metrics = {
                "revenuePerShare": item.get("revenuePerShare"),
                "netIncomePerShare": item.get("netIncomePerShare"),
                "operatingCashFlowPerShare": item.get("operatingCashFlowPerShare"),
                "freeCashFlowPerShare": item.get("freeCashFlowPerShare"),
                "peRatio": item.get("peRatio"),
                "priceToSalesRatio": item.get("priceToSalesRatio"),
                "pbRatio": item.get("pbRatio"),
                "evToEbitda": item.get("enterpriseValueOverEBITDA"),
                "debtToEquity": item.get("debtToEquity"),
                "currentRatio": item.get("currentRatio"),
                "roe": item.get("roe"),
                "roa": item.get("returnOnTangibleAssets"),
                "dividendYield": item.get("dividendYield"),
                "payoutRatio": item.get("payoutRatio"),
            }
            try:
                db.upsert_financial_period(
                    entity_id=entity.get("id"),
                    ticker=ticker,
                    period_type="quarterly",
                    period_end_date=period_end,
                    source_provider=self.provider_name,
                    key_metrics=metrics,
                )
                stored += 1
            except Exception as exc:
                logger.debug("FMP key metrics store error: %s", exc)
        return stored

    # -----------------------------------------------------------------
    # Company Profile (lightweight — 1 API call for snapshot enrichment)
    # -----------------------------------------------------------------

    def fetch_profile(self, ticker: str) -> dict | None:
        """Fetch company profile: market cap, beta, sector, industry, price.

        Returns a flat dict or None if unavailable.
        Used by source_discovery._fmp_enrich_snapshot().
        """
        data = self._get(f"profile/{ticker}")
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        item = data[0]
        return {
            "market_cap": item.get("mktCap"),
            "beta": item.get("beta"),
            "sector": item.get("sector"),
            "industry": item.get("industry"),
            "price": item.get("price"),
            "dividend_yield": item.get("lastDiv"),
            "avg_volume": item.get("volAvg"),
            "employees": item.get("fullTimeEmployees"),
        }

    # -----------------------------------------------------------------
    # Ratios TTM (trailing twelve months — margins, PE, PEG, growth)
    # -----------------------------------------------------------------

    def fetch_ratios_ttm(self, ticker: str) -> dict | None:
        """Fetch trailing-twelve-month financial ratios.

        Returns a flat dict or None if unavailable.
        Used by source_discovery._fmp_enrich_snapshot().
        """
        data = self._get(f"ratios-ttm/{ticker}")
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        item = data[0]
        return {
            "trailing_pe": item.get("peRatioTTM"),
            "peg_ratio": item.get("pegRatioTTM"),
            "gross_margin": item.get("grossProfitMarginTTM"),
            "operating_margin": item.get("operatingProfitMarginTTM"),
            "profit_margin": item.get("netProfitMarginTTM"),
            "revenue_growth": item.get("revenueGrowthTTM"),
            "earnings_growth": item.get("netIncomeGrowthTTM"),
            "debt_to_equity": item.get("debtEquityRatioTTM"),
            "current_ratio": item.get("currentRatioTTM"),
            "dividend_yield": item.get("dividendYieldTTM"),
        }

    # -----------------------------------------------------------------
    # Main entry point
    # -----------------------------------------------------------------

    def fetch_company_data(self, entity: dict) -> list[ProviderResult]:
        results: list[ProviderResult] = []
        ticker = entity.get("ticker", "")
        if not ticker:
            return [ProviderResult(provider=self.provider_name, data_type="all", success=False, error="No ticker")]

        # Income statements (1 API call)
        inc = self._fetch_income_statements(entity)
        results.append(ProviderResult(provider=self.provider_name, data_type="income_statement",
                                      records_stored=inc, success=inc > 0))

        # Balance sheets (1 API call)
        bal = self._fetch_balance_sheets(entity)
        results.append(ProviderResult(provider=self.provider_name, data_type="balance_sheet",
                                      records_stored=bal, success=bal > 0))

        # Cash flows (1 API call)
        cf = self._fetch_cash_flows(entity)
        results.append(ProviderResult(provider=self.provider_name, data_type="cash_flow",
                                      records_stored=cf, success=cf > 0))

        # Key metrics (1 API call)
        km = self._fetch_key_metrics(entity)
        results.append(ProviderResult(provider=self.provider_name, data_type="key_metrics",
                                      records_stored=km, success=km > 0))

        total = inc + bal + cf + km
        logger.info("FMP: stored %d records for %s (income=%d, balance=%d, cashflow=%d, metrics=%d)",
                     total, ticker, inc, bal, cf, km)
        return results
