"""Finviz provider for analyst price targets."""

from __future__ import annotations

import logging
from typing import Any
from datetime import date

import httpx
from bs4 import BeautifulSoup

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core import db

logger = logging.getLogger(__name__)

class FinvizProvider(BaseProvider):
    """Scrapes Finviz for analyst price targets and ratings."""
    
    @property
    def provider_name(self) -> str:
        return "finviz"

    def is_configured(self) -> bool:
        # Free web scraper, no key needed
        return True

    def fetch_company_data(self, entity: dict[str, Any]) -> list[ProviderResult]:
        ticker = entity.get("ticker")
        if not ticker:
            return []
            
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        }
        
        try:
            resp = httpx.get(url, headers=headers, timeout=15.0)
            if resp.status_code != 200:
                logger.warning("Finviz returned %d for %s", resp.status_code, ticker)
                return [ProviderResult(provider=self.provider_name, data_type="analyst_targets", success=False, error=str(resp.status_code))]
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 1. Parse Key Metrics (Target Price)
            metrics = {}
            snapshot = soup.find('table', class_='snapshot-table2')
            if snapshot:
                for row in snapshot.find_all('tr'):
                    cols = row.find_all('td')
                    for i in range(0, len(cols), 2):
                        if i+1 < len(cols):
                            key = cols[i].text.strip()
                            val = cols[i+1].text.strip()
                            if key == "Target Price" and val != "-":
                                try:
                                    metrics["analystTargetPrice"] = float(val.replace(',', ''))
                                except ValueError:
                                    pass
            
            if metrics:
                db.upsert_financial_period(
                    entity_id=entity.get("id"),
                    ticker=ticker,
                    period_type="quarterly",
                    period_end_date=date.today().isoformat(),  # Use today for fresh target
                    source_provider=self.provider_name,
                    key_metrics=metrics,
                )
                
            # 2. Parse Analyst Ratings
            top_posts = []
            sentiment_score = 0.0
            for tr in soup.find_all("tr"):
                text = tr.get_text(" ", strip=True)
                if "Upgrade" in text or "Downgrade" in text or "Reiterated" in text or "Initiated" in text:
                    tds = tr.find_all("td")
                    if len(tds) >= 4:
                        date_str = tds[0].text.strip()
                        action = tds[1].text.strip()
                        analyst = tds[2].text.strip()
                        rating = tds[3].text.strip()
                        post_text = f"[{date_str}] {analyst} {action} to {rating}."
                        top_posts.append({
                            "content": post_text,
                            "url": url,
                            "author": analyst,
                            "score": 1,
                        })
                        
                        # extremely basic naive sentiment scoring
                        if "Upgrade" in action or "Buy" in rating or "Overweight" in rating or "Outperform" in rating:
                            sentiment_score += 1.0
                        elif "Downgrade" in action or "Sell" in rating or "Underweight" in rating:
                            sentiment_score -= 1.0

            if top_posts:
                avg_sent = sentiment_score / len(top_posts) if top_posts else 0.0
                db.upsert_social_signal(
                    entity_id=entity.get("id"),
                    ticker=ticker,
                    platform="finviz_analysts",
                    signal_date=date.today().isoformat(),
                    mention_count=len(top_posts),
                    avg_sentiment=avg_sent,
                    top_posts=top_posts[:10],
                    source_provider=self.provider_name
                )
                        
            return [ProviderResult(
                provider=self.provider_name,
                data_type="analyst_targets",
                success=True,
                records_stored=1 + (1 if top_posts else 0),
            )]
            
        except Exception as exc:
            logger.warning("Finviz fetch failed for %s: %s", ticker, exc)
            return [ProviderResult(provider=self.provider_name, data_type="analyst_targets", success=False, error=str(exc))]
