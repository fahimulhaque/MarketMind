"""Base class for structured data providers.

Unlike connectors (which fetch raw text from URLs), providers extract
structured data from APIs and store it directly in the financial data tables.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ProviderResult:
    """Outcome of a single provider fetch operation."""

    provider: str
    data_type: str  # e.g. "financials", "filings", "macro", "social", "news"
    records_stored: int = 0
    success: bool = True
    error: str = ""
    fetched_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class BaseProvider(ABC):
    """Abstract base for structured data providers."""

    # Subclasses should override these class-level attributes
    _daily_limit: int = 0  # 0 = unlimited
    _calls_today: int = 0
    _last_reset_date: str = ""

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Unique name for this provider (e.g. 'sec_edgar', 'fmp')."""
        ...

    @abstractmethod
    def is_configured(self) -> bool:
        """Return True if required API keys / config are present."""
        ...

    @abstractmethod
    def fetch_company_data(self, entity: dict) -> list[ProviderResult]:
        """Fetch all available data for a company entity and store it.

        Args:
            entity: dict with keys: id, name, ticker, cik, sector, industry

        Returns:
            List of ProviderResult indicating what was stored.
        """
        ...

    def rate_limit_ok(self) -> bool:
        """Check if we have remaining API budget today."""
        if self._daily_limit <= 0:
            return True
        today = date.today().isoformat()
        if self._last_reset_date != today:
            self._calls_today = 0
            self._last_reset_date = today
        return self._calls_today < self._daily_limit

    def _track_call(self) -> None:
        """Increment daily call counter."""
        today = date.today().isoformat()
        if self._last_reset_date != today:
            self._calls_today = 0
            self._last_reset_date = today
        self._calls_today += 1

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """Safely cast to float."""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """Safely cast to int."""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
