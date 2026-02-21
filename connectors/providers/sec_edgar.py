"""SEC EDGAR data provider.

Fetches structured financial data (XBRL company facts) and filing
metadata from the SEC EDGAR public API.  Completely free, official
US government API — no API key required, just a User-Agent header.

Rate limit: 10 requests/second (self-enforced).
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core.config import get_settings
from core import db

logger = logging.getLogger(__name__)

# SEC EDGAR XBRL US-GAAP tags we extract
_INCOME_TAGS = [
    "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
    "CostOfRevenue", "CostOfGoodsAndServicesSold",
    "GrossProfit", "OperatingIncomeLoss", "NetIncomeLoss",
    "EarningsPerShareBasic", "EarningsPerShareDiluted",
    "ResearchAndDevelopmentExpense",
    "SellingGeneralAndAdministrativeExpense",
]

_BALANCE_TAGS = [
    "Assets", "Liabilities", "StockholdersEquity",
    "CashAndCashEquivalentsAtCarryingValue",
    "LongTermDebt", "LongTermDebtNoncurrent",
    "CurrentAssets", "CurrentLiabilities",
    "AccountsReceivableNetCurrent", "InventoryNet",
    "PropertyPlantAndEquipmentNet", "Goodwill",
]

_CASHFLOW_TAGS = [
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInInvestingActivities",
    "NetCashProvidedByUsedInFinancingActivities",
    "PaymentsOfDividends", "PaymentsForRepurchaseOfCommonStock",
    "DepreciationDepletionAndAmortization",
    "CapitalExpenditure",
    "PaymentsToAcquirePropertyPlantAndEquipment",
]


class SecEdgarProvider(BaseProvider):
    """SEC EDGAR XBRL + filings provider."""

    _daily_limit = 0  # No daily limit; SEC enforces 10 req/sec
    _last_request_time: float = 0.0

    @property
    def provider_name(self) -> str:
        return "sec_edgar"

    def is_configured(self) -> bool:
        settings = get_settings()
        return bool(settings.sec_edgar_user_agent)

    def _throttle(self) -> None:
        """Enforce 10 req/sec rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.12:
            time.sleep(0.12 - elapsed)
        self._last_request_time = time.time()

    def _headers(self) -> dict:
        settings = get_settings()
        return {"User-Agent": settings.sec_edgar_user_agent, "Accept": "application/json"}

    # -----------------------------------------------------------------
    # CIK resolution
    # -----------------------------------------------------------------

    def _resolve_cik(self, ticker: str) -> str:
        """Resolve ticker → zero-padded CIK via SEC company_tickers.json."""
        self._throttle()
        try:
            resp = httpx.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers=self._headers(),
                timeout=15,
            )
            if resp.status_code != 200:
                return ""
            data = resp.json()
            t_upper = ticker.upper()
            for entry in data.values():
                if entry.get("ticker", "").upper() == t_upper:
                    return str(entry.get("cik_str", "")).zfill(10)
        except Exception as exc:
            logger.warning("SEC CIK resolution failed for %r: %s", ticker, exc)
        return ""

    # -----------------------------------------------------------------
    # XBRL Company Facts → financial_periods
    # -----------------------------------------------------------------

    def _fetch_company_facts(self, cik: str) -> dict:
        """Fetch full XBRL company facts JSON."""
        self._throttle()
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        try:
            resp = httpx.get(url, headers=self._headers(), timeout=30)
            if resp.status_code != 200:
                logger.info("SEC companyfacts returned %d for CIK %s", resp.status_code, cik)
                return {}
            return resp.json()
        except Exception as exc:
            logger.warning("SEC companyfacts fetch failed for CIK %s: %s", cik, exc)
            return {}

    def _extract_tag_values(self, facts: dict, tag: str) -> list[dict]:
        """Extract quarterly/annual values for a US-GAAP tag."""
        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        tag_data = us_gaap.get(tag, {})
        units = tag_data.get("units", {})
        # Try USD first, then 'USD/shares' for EPS, then 'shares'
        values = units.get("USD") or units.get("USD/shares") or units.get("shares") or []
        return values

    def _build_period_map(self, facts: dict, tags: list[str]) -> dict[str, dict]:
        """Build a {period_key: {tag: value, ...}} map from XBRL facts."""
        period_map: dict[str, dict] = {}
        for tag in tags:
            for entry in self._extract_tag_values(facts, tag):
                form = entry.get("form", "")
                # Only 10-K (annual) and 10-Q (quarterly) filings
                if form not in ("10-K", "10-Q"):
                    continue
                end_date = entry.get("end", "")
                if not end_date:
                    continue
                # Determine period type
                start = entry.get("start", "")
                period_type = "annual" if form == "10-K" else "quarterly"
                # Use filing date + period end as key
                key = f"{period_type}:{end_date}"
                if key not in period_map:
                    period_map[key] = {
                        "period_type": period_type,
                        "period_end_date": end_date,
                        "form": form,
                        "fiscal_year": int(end_date[:4]) if len(end_date) >= 4 else None,
                    }
                period_map[key][tag] = entry.get("val")
        return period_map

    def _store_financials(self, entity: dict, facts: dict) -> int:
        """Parse XBRL facts into financial_periods rows."""
        income_map = self._build_period_map(facts, _INCOME_TAGS)
        balance_map = self._build_period_map(facts, _BALANCE_TAGS)
        cashflow_map = self._build_period_map(facts, _CASHFLOW_TAGS)

        # Merge all period keys
        all_keys = set(income_map) | set(balance_map) | set(cashflow_map)
        stored = 0

        for key in all_keys:
            inc = income_map.get(key, {})
            bal = balance_map.get(key, {})
            cf = cashflow_map.get(key, {})

            # Determine period metadata from whichever map has it
            meta = inc or bal or cf
            period_type = meta.get("period_type", "quarterly")
            period_end_date = meta.get("period_end_date", "")
            if not period_end_date:
                continue

            income_stmt = {}
            if "Revenues" in inc or "RevenueFromContractWithCustomerExcludingAssessedTax" in inc:
                income_stmt["revenue"] = inc.get("Revenues", inc.get("RevenueFromContractWithCustomerExcludingAssessedTax"))
            if "CostOfRevenue" in inc or "CostOfGoodsAndServicesSold" in inc:
                income_stmt["costOfRevenue"] = inc.get("CostOfRevenue", inc.get("CostOfGoodsAndServicesSold"))
            if "GrossProfit" in inc:
                income_stmt["grossProfit"] = inc["GrossProfit"]
            if "OperatingIncomeLoss" in inc:
                income_stmt["operatingIncome"] = inc["OperatingIncomeLoss"]
            if "NetIncomeLoss" in inc:
                income_stmt["netIncome"] = inc["NetIncomeLoss"]
            if "EarningsPerShareBasic" in inc:
                income_stmt["eps"] = inc["EarningsPerShareBasic"]
            if "EarningsPerShareDiluted" in inc:
                income_stmt["epsdiluted"] = inc["EarningsPerShareDiluted"]

            balance_stmt = {}
            if "Assets" in bal: balance_stmt["totalAssets"] = bal["Assets"]
            if "Liabilities" in bal: balance_stmt["totalLiabilities"] = bal["Liabilities"]
            if "StockholdersEquity" in bal: balance_stmt["totalEquity"] = bal["StockholdersEquity"]
            if "CashAndCashEquivalentsAtCarryingValue" in bal: balance_stmt["cashAndEquivalents"] = bal["CashAndCashEquivalentsAtCarryingValue"]
            if "LongTermDebt" in bal or "LongTermDebtNoncurrent" in bal:
                balance_stmt["totalDebt"] = bal.get("LongTermDebt", bal.get("LongTermDebtNoncurrent"))

            cashflow_stmt = {}
            if "NetCashProvidedByUsedInOperatingActivities" in cf: cashflow_stmt["operatingCashFlow"] = cf["NetCashProvidedByUsedInOperatingActivities"]
            if "CapitalExpenditure" in cf: cashflow_stmt["capitalExpenditure"] = cf["CapitalExpenditure"]

            if not income_stmt and not balance_stmt and not cashflow_stmt:
                continue

            try:
                db.upsert_financial_period(
                    entity_id=entity.get("id"),
                    ticker=entity["ticker"],
                    period_type=period_type,
                    period_end_date=period_end_date,
                    fiscal_year=meta.get("fiscal_year"),
                    fiscal_quarter=None,  # SEC doesn't always indicate quarter
                    source_provider=self.provider_name,
                    income_statement=income_stmt,
                    balance_sheet=balance_stmt,
                    cash_flow=cashflow_stmt,
                )
                stored += 1
            except Exception as exc:
                logger.debug("Failed to store SEC financial period %s: %s", key, exc)

        return stored

    # -----------------------------------------------------------------
    # Filing index → entity_filings
    # -----------------------------------------------------------------

    def _fetch_filings(self, cik: str, entity: dict) -> int:
        """Fetch and store recent filing metadata."""
        self._throttle()
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        try:
            resp = httpx.get(url, headers=self._headers(), timeout=20)
            if resp.status_code != 200:
                return 0
            data = resp.json()
        except Exception as exc:
            logger.warning("SEC filings fetch failed for CIK %s: %s", cik, exc)
            return 0

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        descriptions = recent.get("primaryDocDescription", [])

        stored = 0
        target_forms = {"10-K", "10-Q", "8-K", "DEF 14A", "S-1"}

        for i in range(min(len(forms), 100)):  # Cap at 100 filings
            form = forms[i] if i < len(forms) else ""
            if form not in target_forms:
                continue

            acc = accessions[i] if i < len(accessions) else ""
            if not acc:
                continue

            filing_date = dates[i] if i < len(dates) else ""
            doc = primary_docs[i] if i < len(primary_docs) else ""
            desc = descriptions[i] if i < len(descriptions) else form

            acc_clean = acc.replace("-", "")
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{acc_clean}/{doc}" if doc else ""

            try:
                db.upsert_entity_filing(
                    entity_id=entity.get("id"),
                    ticker=entity["ticker"],
                    cik=cik,
                    filing_type=form,
                    filing_date=filing_date,
                    accession_number=acc,
                    filing_url=filing_url,
                    description=desc,
                    source_provider=self.provider_name,
                )
                stored += 1
            except Exception as exc:
                logger.debug("Failed to store SEC filing %s: %s", acc, exc)

        return stored

    # -----------------------------------------------------------------
    # Main entry point
    # -----------------------------------------------------------------

    def fetch_company_data(self, entity: dict) -> list[ProviderResult]:
        results: list[ProviderResult] = []
        ticker = entity.get("ticker", "")
        if not ticker:
            return [ProviderResult(provider=self.provider_name, data_type="all", success=False, error="No ticker")]

        # Resolve CIK
        cik = entity.get("cik") or self._resolve_cik(ticker)
        if not cik:
            return [ProviderResult(provider=self.provider_name, data_type="all", success=False,
                                   error=f"Could not resolve CIK for {ticker}")]

        # Fetch XBRL company facts → financials
        facts = self._fetch_company_facts(cik)
        if facts:
            fin_count = self._store_financials(entity, facts)
            results.append(ProviderResult(
                provider=self.provider_name, data_type="financials",
                records_stored=fin_count, success=fin_count > 0,
                error="" if fin_count > 0 else "No XBRL data parsed",
            ))
            logger.info("SEC EDGAR: stored %d financial periods for %s (CIK %s)", fin_count, ticker, cik)
        else:
            results.append(ProviderResult(
                provider=self.provider_name, data_type="financials", success=False,
                error="companyfacts returned empty",
            ))

        # Fetch filing index
        filing_count = self._fetch_filings(cik, entity)
        results.append(ProviderResult(
            provider=self.provider_name, data_type="filings",
            records_stored=filing_count, success=filing_count > 0,
        ))
        logger.info("SEC EDGAR: stored %d filings for %s", filing_count, ticker)

        return results
