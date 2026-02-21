"""FINRA provider for specific off-exchange and short volume data."""

from __future__ import annotations

import logging
from typing import Any
from datetime import date

import httpx
from bs4 import BeautifulSoup

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core import db

logger = logging.getLogger(__name__)

class FinraProvider(BaseProvider):
    """Scrapes FINRA proxies for dark pool and short interest data."""
    
    @property
    def provider_name(self) -> str:
        return "finra"

    def is_configured(self) -> bool:
        return True

    def fetch_company_data(self, entity: dict[str, Any]) -> list[ProviderResult]:
        ticker = entity.get("ticker")
        if not ticker:
            return []
            
        # Using a proxy structure to extract Finra short interest sentiment.
        url = f"https://fintel.io/ss/us/{ticker}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        }
        
        try:
            resp = httpx.get(url, headers=headers, timeout=15.0)
            if resp.status_code != 200:
                logger.warning("Finra proxy returned %d for %s", resp.status_code, ticker)
                return [ProviderResult(provider=self.provider_name, data_type="short_interest", success=False, error=str(resp.status_code))]
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Simple text scanning to grab Short Interest
            short_interest_pct = None
            for td in soup.find_all("td"):
                text = td.get_text(" ", strip=True)
                if "Short Interest % Float" in text or "Short Interest Ratio" in text:
                    sibling = td.find_next_sibling("td")
                    if sibling:
                        val = sibling.get_text(strip=True).replace('%', '').replace(',', '')
                        try:
                            short_interest_pct = float(val)
                            break
                        except ValueError:
                            pass
                            
            if short_interest_pct is None:
                import re
                match = re.search(r'Short Interest\s+([\d\.]+)\s*%', soup.text)
                if match:
                    short_interest_pct = float(match.group(1))

            if short_interest_pct is None:
                return [ProviderResult(provider=self.provider_name, data_type="short_interest", success=False, error="No Short Interest found")]
                
            sentiment_score = 0.0
            if short_interest_pct > 20.0:
                sentiment_score = -0.5
                sentiment_desc = "High short interest indicates heavy bearish sentiment (potential for squeeze)."
            elif short_interest_pct < 5.0:
                sentiment_score = 0.5
                sentiment_desc = "Low short interest indicating mostly bullish/neutral sentiment."
            else:
                sentiment_desc = "Moderate short interest."
                
            content = f"FINRA Short Interest reported at {short_interest_pct:.2f}%. {sentiment_desc}"
            
            db.upsert_social_signal(
                entity_id=entity.get("id"),
                ticker=ticker,
                platform="finra_short_interest",
                signal_date=date.today().isoformat(),
                mention_count=1,
                avg_sentiment=sentiment_score,
                top_posts=[{
                    "content": content,
                    "author": "FINRA Proxy",
                    "score": 1,
                    "url": url
                }],
                source_provider=self.provider_name
            )
            
            return [ProviderResult(
                provider=self.provider_name,
                data_type="short_interest",
                success=True,
                records_stored=1,
            )]
            
        except Exception as exc:
            logger.warning("Finra fetch failed for %s: %s", ticker, exc)
            return [ProviderResult(provider=self.provider_name, data_type="short_interest", success=False, error=str(exc))]
