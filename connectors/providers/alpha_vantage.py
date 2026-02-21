"""Alpha Vantage data provider.

Free tier: 25 API calls/day.  Provides income statements, balance sheets,
cash flow statements, earnings history, and company overview.

Docs: https://www.alphavantage.co/documentation/
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core.config import get_settings
from core import db

logger = logging.getLogger(__name__)

_AV_BASE = "https://www.alphavantage.co/query"


class AlphaVantageProvider(BaseProvider):
    """Alpha Vantage structured financial data provider."""

    _daily_limit = 25  # Free tier limit

    @property
    def provider_name(self) -> str:
        return "alpha_vantage"

    def is_configured(self) -> bool:
        return bool(get_settings().alpha_vantage_api_key)

    def _get(self, function: str, extra_params: dict | None = None) -> Any:
        """Make an authenticated Alpha Vantage API call."""
        if not self.rate_limit_ok():
            logger.warning("Alpha Vantage daily limit reached (%d/%d)", self._calls_today, self._daily_limit)
            return None
        settings = get_settings()
        params = {"function": function, "apikey": settings.alpha_vantage_api_key}
        if extra_params:
            params.update(extra_params)
        self._track_call()
        try:
            resp = httpx.get(_AV_BASE, params=params, timeout=30)
            if resp.status_code != 200:
                logger.info("Alpha Vantage %s returned %d", function, resp.status_code)
                return None
            data = resp.json()
            # AV returns error messages in JSON
            if "Error Message" in data or "Note" in data:
                logger.info("Alpha Vantage %s: %s", function, data.get("Error Message") or data.get("Note"))
                return None
            return data
        except Exception as exc:
            logger.warning("Alpha Vantage request failed for %s: %s", function, exc)
            return None

    # -----------------------------------------------------------------
    # Income Statement
    # -----------------------------------------------------------------

    def _fetch_income_statements(self, entity: dict) -> int:
        ticker = entity["ticker"]
        data = self._get("INCOME_STATEMENT", {"symbol": ticker})
        if not data:
            return 0
        stored = 0
        for item in data.get("quarterlyReports", []):
            period_end = item.get("fiscalDateEnding", "")
            if not period_end:
                continue
            income = {
                "totalRevenue": self._safe_float(item.get("totalRevenue")),
                "costOfRevenue": self._safe_float(item.get("costOfRevenue")),
                "grossProfit": self._safe_float(item.get("grossProfit")),
                "operatingIncome": self._safe_float(item.get("operatingIncome")),
                "netIncome": self._safe_float(item.get("netIncome")),
                "ebitda": self._safe_float(item.get("ebitda")),
                "researchAndDevelopment": self._safe_float(item.get("researchAndDevelopment")),
            }
            try:
                db.upsert_financial_period(
                    entity_id=entity.get("id"),
                    ticker=ticker,
                    period_type="quarterly",
                    period_end_date=period_end,
                    source_provider=self.provider_name,
                    income_statement=income,
                )
                stored += 1
            except Exception as exc:
                logger.debug("AV income store error: %s", exc)
        return stored

    # -----------------------------------------------------------------
    # Balance Sheet
    # -----------------------------------------------------------------

    def _fetch_balance_sheets(self, entity: dict) -> int:
        ticker = entity["ticker"]
        data = self._get("BALANCE_SHEET", {"symbol": ticker})
        if not data:
            return 0
        stored = 0
        for item in data.get("quarterlyReports", []):
            period_end = item.get("fiscalDateEnding", "")
            if not period_end:
                continue
            balance = {
                "totalAssets": self._safe_float(item.get("totalAssets")),
                "totalLiabilities": self._safe_float(item.get("totalLiabilities")),
                "totalShareholderEquity": self._safe_float(item.get("totalShareholderEquity")),
                "cashAndEquivalents": self._safe_float(item.get("cashAndCashEquivalentsAtCarryingValue")),
                "totalCurrentAssets": self._safe_float(item.get("totalCurrentAssets")),
                "totalCurrentLiabilities": self._safe_float(item.get("totalCurrentLiabilities")),
                "longTermDebt": self._safe_float(item.get("longTermDebt")),
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
                logger.debug("AV balance store error: %s", exc)
        return stored

    # -----------------------------------------------------------------
    # Cash Flow
    # -----------------------------------------------------------------

    def _fetch_cash_flows(self, entity: dict) -> int:
        ticker = entity["ticker"]
        data = self._get("CASH_FLOW", {"symbol": ticker})
        if not data:
            return 0
        stored = 0
        for item in data.get("quarterlyReports", []):
            period_end = item.get("fiscalDateEnding", "")
            if not period_end:
                continue
            cf = {
                "operatingCashflow": self._safe_float(item.get("operatingCashflow")),
                "capitalExpenditures": self._safe_float(item.get("capitalExpenditures")),
                "cashflowFromInvestment": self._safe_float(item.get("cashflowFromInvestment")),
                "cashflowFromFinancing": self._safe_float(item.get("cashflowFromFinancing")),
                "dividendPayout": self._safe_float(item.get("dividendPayout")),
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
                logger.debug("AV cash flow store error: %s", exc)
        return stored

    # -----------------------------------------------------------------
    # Earnings (for EPS history)
    # -----------------------------------------------------------------

    def _fetch_earnings(self, entity: dict) -> int:
        ticker = entity["ticker"]
        data = self._get("EARNINGS", {"symbol": ticker})
        if not data:
            return 0
        stored = 0
        for item in data.get("quarterlyEarnings", []):
            period_end = item.get("fiscalDateEnding", "")
            if not period_end:
                continue
            metrics = {
                "reportedEPS": self._safe_float(item.get("reportedEPS")),
                "estimatedEPS": self._safe_float(item.get("estimatedEPS")),
                "surprise": self._safe_float(item.get("surprise")),
                "surprisePercentage": self._safe_float(item.get("surprisePercentage")),
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
                logger.debug("AV earnings store error: %s", exc)
        return stored

    # -----------------------------------------------------------------
    # Company Overview
    # -----------------------------------------------------------------

    def _fetch_overview(self, entity: dict) -> dict:
        ticker = entity["ticker"]
        data = self._get("OVERVIEW", {"symbol": ticker})
        if not data:
            return {}
        return {
            "sector": data.get("Sector", ""),
            "industry": data.get("Industry", ""),
            "description": data.get("Description", ""),
            "marketCap": data.get("MarketCapitalization", ""),
            "peRatio": data.get("PERatio", ""),
            "pegRatio": data.get("PEGRatio", ""),
            "bookValue": data.get("BookValue", ""),
            "dividendPerShare": data.get("DividendPerShare", ""),
            "eps": data.get("EPS", ""),
            "analystTargetPrice": data.get("AnalystTargetPrice", ""),
            "52WeekHigh": data.get("52WeekHigh", ""),
            "52WeekLow": data.get("52WeekLow", ""),
        }

    # -----------------------------------------------------------------
    # Main entry point
    # -----------------------------------------------------------------

    def fetch_company_data(self, entity: dict) -> list[ProviderResult]:
        results: list[ProviderResult] = []
        ticker = entity.get("ticker", "")
        if not ticker:
            return [ProviderResult(provider=self.provider_name, data_type="all", success=False, error="No ticker")]

        # Income (1 call)
        inc = self._fetch_income_statements(entity)
        results.append(ProviderResult(provider=self.provider_name, data_type="income_statement",
                                      records_stored=inc, success=inc > 0))

        # Balance (1 call)
        bal = self._fetch_balance_sheets(entity)
        results.append(ProviderResult(provider=self.provider_name, data_type="balance_sheet",
                                      records_stored=bal, success=bal > 0))

        # Cash flow (1 call)
        cf = self._fetch_cash_flows(entity)
        results.append(ProviderResult(provider=self.provider_name, data_type="cash_flow",
                                      records_stored=cf, success=cf > 0))

        # Earnings (1 call)
        earn = self._fetch_earnings(entity)
        results.append(ProviderResult(provider=self.provider_name, data_type="earnings",
                                      records_stored=earn, success=earn > 0))

        total = inc + bal + cf + earn
        logger.info("Alpha Vantage: stored %d records for %s (inc=%d, bal=%d, cf=%d, earn=%d)",
                     total, ticker, inc, bal, cf, earn)
        return results
