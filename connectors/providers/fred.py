"""FRED (Federal Reserve Economic Data) provider.

Fetches key macroeconomic time series from the FRED API.
Free API key required (https://fred.stlouisfed.org/docs/api/api_key.html).
No meaningful rate limit for typical usage.

Docs: https://fred.stlouisfed.org/docs/api/fred/
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import httpx

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core.config import get_settings
from core import db

logger = logging.getLogger(__name__)

_FRED_BASE = "https://api.stlouisfed.org/fred"

# Core macro series to fetch
CORE_SERIES = {
    "GDP": "Gross Domestic Product",
    "CPIAUCSL": "Consumer Price Index (All Urban Consumers)",
    "UNRATE": "Unemployment Rate",
    "DFF": "Federal Funds Effective Rate",
    "T10YIE": "10-Year Breakeven Inflation Rate",
    "VIXCLS": "CBOE Volatility Index (VIX)",
    "SP500": "S&P 500 Index",
    "DTWEXBGS": "Trade Weighted US Dollar Index",
    "DGS10": "10-Year Treasury Constant Maturity Rate",
    "DGS2": "2-Year Treasury Constant Maturity Rate",
    "FEDFUNDS": "Federal Funds Rate",
    "MORTGAGE30US": "30-Year Fixed Rate Mortgage Average",
}


class FredProvider(BaseProvider):
    """FRED macroeconomic data provider."""

    _daily_limit = 0  # Effectively unlimited for our usage

    @property
    def provider_name(self) -> str:
        return "fred"

    def is_configured(self) -> bool:
        return bool(get_settings().fred_api_key)

    def _get_series_observations(
        self, series_id: str, start_date: str | None = None, limit: int = 100
    ) -> list[dict]:
        """Fetch observations for a FRED series."""
        settings = get_settings()
        if not start_date:
            start_date = (date.today() - timedelta(days=365 * 2)).isoformat()

        try:
            resp = httpx.get(
                f"{_FRED_BASE}/series/observations",
                params={
                    "series_id": series_id,
                    "api_key": settings.fred_api_key,
                    "file_type": "json",
                    "observation_start": start_date,
                    "sort_order": "desc",
                    "limit": str(limit),
                },
                timeout=20,
            )
            if resp.status_code != 200:
                logger.info("FRED %s returned %d", series_id, resp.status_code)
                return []
            data = resp.json()
            return data.get("observations", [])
        except Exception as exc:
            logger.warning("FRED fetch failed for %s: %s", series_id, exc)
            return []

    def _store_series(self, series_id: str, series_name: str) -> int:
        """Fetch and store observations for a single series."""
        observations = self._get_series_observations(series_id)
        stored = 0
        for obs in observations:
            obs_date = obs.get("date", "")
            value_str = obs.get("value", "")
            if not obs_date or value_str == "." or not value_str:
                continue
            try:
                value = float(value_str)
            except (ValueError, TypeError):
                continue
            try:
                db.upsert_macro_indicator(
                    series_id=series_id,
                    series_name=series_name,
                    observation_date=obs_date,
                    value=value,
                    source_provider=self.provider_name,
                )
                stored += 1
            except Exception as exc:
                logger.debug("FRED store error for %s/%s: %s", series_id, obs_date, exc)
        return stored

    def fetch_all_core_series(self) -> list[ProviderResult]:
        """Fetch all core macro series. Called on schedule (not per-entity)."""
        results: list[ProviderResult] = []
        total = 0
        for series_id, series_name in CORE_SERIES.items():
            count = self._store_series(series_id, series_name)
            total += count
            results.append(ProviderResult(
                provider=self.provider_name,
                data_type=f"macro:{series_id}",
                records_stored=count,
                success=count > 0,
            ))
        logger.info("FRED: stored %d total macro observations across %d series", total, len(CORE_SERIES))
        return results

    def fetch_company_data(self, entity: dict) -> list[ProviderResult]:
        """For FRED, company-level fetch just returns core macro data.

        FRED data is global (not company-specific), but we fetch it as
        part of any entity enrichment to ensure macro context is fresh.
        """
        return self.fetch_all_core_series()
