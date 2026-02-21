"""DuckDuckGo search provider.

Uses the duckduckgo-search Python package to discover web pages and
news articles.  Results are fed into the existing source→ingest pipeline
so they get normalized, chunked, analyzed, and stored as evidence.

Free, no API key required.
"""

from __future__ import annotations

import logging
from typing import Any

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core import db

logger = logging.getLogger(__name__)


class DdgProvider(BaseProvider):
    """DuckDuckGo Search provider for web + news discovery."""

    _daily_limit = 0  # No formal limit; be reasonable

    @property
    def provider_name(self) -> str:
        return "ddg"

    def is_configured(self) -> bool:
        # Always available — no API key needed
        try:
            from duckduckgo_search import DDGS  # noqa: F401
            return True
        except ImportError:
            return False

    def _search_web(self, query: str, max_results: int = 15) -> list[dict]:
        """Run DuckDuckGo text search."""
        try:
            from duckduckgo_search import DDGS
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=max_results))
            return results
        except Exception as exc:
            logger.warning("DDG web search failed for %r: %s", query, exc)
            return []

    def _search_news(self, query: str, max_results: int = 15) -> list[dict]:
        """Run DuckDuckGo news search."""
        try:
            from duckduckgo_search import DDGS
            with DDGS() as ddgs:
                results = list(ddgs.news(query, max_results=max_results))
            return results
        except Exception as exc:
            logger.warning("DDG news search failed for %r: %s", query, exc)
            return []

    def _register_sources(self, results: list[dict], entity: dict, source_type: str) -> int:
        """Register search results as sources for the existing ingest pipeline."""
        registered = 0
        for item in results:
            url = item.get("href") or item.get("url") or item.get("link", "")
            title = item.get("title", "")
            if not url or not title:
                continue
            try:
                name = f"[DDG-{source_type}] {title[:120]} ({entity.get('ticker', '')})"
                db.add_source(name=name, url=url, connector_type="web")
                registered += 1
            except Exception as exc:
                logger.debug("DDG source register error: %s", exc)
        return registered

    def fetch_company_data(self, entity: dict) -> list[ProviderResult]:
        results: list[ProviderResult] = []
        ticker = entity.get("ticker", "")
        name = entity.get("name", ticker)

        # Web search: entity-focused queries
        web_query = f'"{ticker}" OR "{name}" financial analysis outlook 2025 2026'
        web_results = self._search_web(web_query, max_results=15)
        web_registered = self._register_sources(web_results, entity, "web")
        results.append(ProviderResult(
            provider=self.provider_name, data_type="web_search",
            records_stored=web_registered, success=web_registered > 0,
        ))

        # News search: direct entity name
        news_query = f'"{name}" {ticker} news analysis'
        news_results = self._search_news(news_query, max_results=15)
        news_registered = self._register_sources(news_results, entity, "news")
        results.append(ProviderResult(
            provider=self.provider_name, data_type="news_search",
            records_stored=news_registered, success=news_registered > 0,
        ))

        total = web_registered + news_registered
        logger.info("DDG: registered %d sources for %s (web=%d, news=%d)",
                     total, ticker, web_registered, news_registered)
        return results
