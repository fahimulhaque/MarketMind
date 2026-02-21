"""Polygon.io provider for EOD prices and financials."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core.config import get_settings
from core import db

logger = logging.getLogger(__name__)

class PolygonProvider(BaseProvider):
    """Fetches EOD and XBRL financials from Polygon.io."""
    
    @property
    def provider_name(self) -> str:
        return "polygon"

    def is_configured(self) -> bool:
        settings = get_settings()
        return hasattr(settings, "polygon_api_key") and bool(settings.polygon_api_key)

    def fetch_company_data(self, entity: dict[str, Any]) -> list[ProviderResult]:
        ticker = entity.get("ticker")
        if not ticker:
            return []
            
        settings = get_settings()
        api_key = getattr(settings, "polygon_api_key", None)
        if not api_key:
            return []
            
        results = []
        stored = 0
            
        # 1. Fetch Financials
        url = f"https://api.polygon.io/vX/reference/financials"
        params = {
            "ticker": ticker,
            "timeframe": "quarterly",
            "limit": 4,
            "apiKey": api_key
        }
        
        try:
            resp = httpx.get(url, params=params, timeout=15.0)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("results", []):
                    period_end = item.get("end_date")
                    if not period_end:
                        continue
                        
                    financials = item.get("financials", {})
                    income_statement = financials.get("income_statement", {})
                    balance_sheet = financials.get("balance_sheet", {})
                    cash_flow = financials.get("cash_flow_statement", {})
                    
                    def extract_val(d, key):
                        if key in d and isinstance(d[key], dict):
                            return float(d[key].get("value", 0))
                        return 0.0
                        
                    inc = {
                        "totalRevenue": extract_val(income_statement, "revenues"),
                        "grossProfit": extract_val(income_statement, "gross_profit"),
                        "operatingIncome": extract_val(income_statement, "operating_income_loss"),
                        "netIncome": extract_val(income_statement, "net_income_loss")
                    }
                    bal = {
                        "totalAssets": extract_val(balance_sheet, "assets"),
                        "totalLiabilities": extract_val(balance_sheet, "liabilities"),
                        "totalShareholderEquity": extract_val(balance_sheet, "equity")
                    }
                    cf = {
                        "operatingCashflow": extract_val(cash_flow, "net_cash_flow_from_operating_activities")
                    }
                    
                    try:
                        db.upsert_financial_period(
                            entity_id=entity.get("id"),
                            ticker=ticker,
                            period_type="quarterly",
                            period_end_date=period_end,
                            source_provider=self.provider_name,
                            income_statement=inc,
                            balance_sheet=bal,
                            cash_flow=cf
                        )
                        stored += 1
                    except Exception as e:
                        logger.warning("Polygon financials error: %s", e)
                        
            results.append(ProviderResult(
                provider=self.provider_name,
                data_type="financials",
                success=stored > 0 or resp.status_code == 200,
                records_stored=stored,
            ))
            
        except Exception as exc:
            logger.warning("Polygon fetch failed for %s: %s", ticker, exc)
            results.append(ProviderResult(provider=self.provider_name, data_type="financials", success=False, error=str(exc)))
            
        return results
