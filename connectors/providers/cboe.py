"""CBOE provider for Options Put/Call ratios and sentiment data."""

from __future__ import annotations

import logging
from typing import Any
from datetime import date

import httpx
from bs4 import BeautifulSoup

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core import db

logger = logging.getLogger(__name__)

class CboeProvider(BaseProvider):
    """Scrapes CBOE for put/call ratios."""
    
    @property
    def provider_name(self) -> str:
        return "cboe"

    def is_configured(self) -> bool:
        # Free web scraper
        return True

    def fetch_company_data(self, entity: dict[str, Any]) -> list[ProviderResult]:
        ticker = entity.get("ticker")
        if not ticker:
            return []
            
        # For CBOE, we'll hit an aggregate options page or similar sentiment tracking
        # Since scraping CBOE raw options chain requires deep auth/parsing, we use marketchameleon or similar public facing aggregates as a proxy for this provider implementation as OpenBB does in many unstructured scrape modes.
        url = f"https://marketchameleon.com/Overview/{ticker}/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        }
        
        try:
            resp = httpx.get(url, headers=headers, timeout=15.0)
            if resp.status_code != 200:
                logger.warning("CBOE Options proxy returned %d for %s", resp.status_code, ticker)
                return [ProviderResult(provider=self.provider_name, data_type="options_sentiment", success=False, error=str(resp.status_code))]
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Look for Option Volume and Put/Call ratio
            # A common marker is "Put/Call Ratio"
            put_call_ratio = None
            for p in soup.find_all(string=lambda t: t and 'Put/Call Ratio' in t):
                # Try to extract the number next to it
                parent = p.parent
                if parent:
                    # simplistic extraction from next sibling or surrounding
                    text_context = parent.get_text()
                    import re
                    match = re.search(r'Put/Call Ratio[\s:]*([\d\.]+)', text_context)
                    if match:
                        put_call_ratio = float(match.group(1))
                        break
                        
            if put_call_ratio is None:
                return [ProviderResult(provider=self.provider_name, data_type="options_sentiment", success=False, error="No Put/Call Ratio found")]
                
            # If Put/Call ratio is high (> 1.0), it's bearish. If low (< 0.7), it's bullish.
            sentiment_score = 0.0
            if put_call_ratio > 1.0:
                sentiment_score = -0.5
                sentiment_desc = "Bearish options positioning (High Put volume)"
            elif put_call_ratio < 0.7:
                sentiment_score = 0.5
                sentiment_desc = "Bullish options positioning (High Call volume)"
            else:
                sentiment_desc = "Neutral options positioning"
                
            content = f"Options Put/Call Ratio sits at {put_call_ratio:.2f}. {sentiment_desc}."
            
            db.upsert_social_signal(
                entity_id=entity.get("id"),
                ticker=ticker,
                platform="cboe_options",
                signal_date=date.today().isoformat(),
                mention_count=1,
                avg_sentiment=sentiment_score,
                top_posts=[{
                    "content": content,
                    "author": "CBOE Proxy",
                    "score": 1,
                    "url": url
                }],
                source_provider=self.provider_name
            )
            
            return [ProviderResult(
                provider=self.provider_name,
                data_type="options_sentiment",
                success=True,
                records_stored=1,
            )]
            
        except Exception as exc:
            logger.warning("CBOE fetch failed for %s: %s", ticker, exc)
            return [ProviderResult(provider=self.provider_name, data_type="options_sentiment", success=False, error=str(exc))]
