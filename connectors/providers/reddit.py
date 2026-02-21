"""Reddit social signals provider — public JSON feeds (no auth required).

Searches finance-related subreddits for ticker mentions using Reddit's
public `.json` API endpoints. No API keys or PRAW dependency needed.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime, timezone
from typing import Any

import httpx

from connectors.providers.base_provider import BaseProvider, ProviderResult
from core import db

logger = logging.getLogger(__name__)

# Subreddits with highest financial discussion density
_SUBREDDITS = ["wallstreetbets", "stocks", "investing", "stockmarket", "options"]

_HEADERS = {
    "User-Agent": "MarketMind/1.0 (market intelligence platform; educational use)",
}

# Simple keyword-based sentiment (upgrade to Ollama later)
_POSITIVE_WORDS = {
    "bullish", "bull", "buy", "long", "moon", "rocket", "undervalued",
    "breakout", "calls", "growth", "beat", "strong", "rally", "surge",
    "upgrade", "outperform", "profit", "gain", "green", "up",
}
_NEGATIVE_WORDS = {
    "bearish", "bear", "sell", "short", "crash", "overvalued", "dump",
    "puts", "decline", "miss", "weak", "drop", "downgrade", "underperform",
    "loss", "red", "down", "bubble", "risk", "warning",
}


def _simple_sentiment(text: str) -> float:
    """Compute a -1 to 1 sentiment score from keyword counting."""
    words = set(re.findall(r'\w+', text.lower()))
    pos = len(words & _POSITIVE_WORDS)
    neg = len(words & _NEGATIVE_WORDS)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 3)


class RedditProvider(BaseProvider):
    """Reddit social signals provider via public JSON feeds — no API key required."""

    _daily_limit = 0  # Reddit public JSON has informal per-minute limits

    @property
    def provider_name(self) -> str:
        return "reddit"

    def is_configured(self) -> bool:
        # Always available — no auth needed for public JSON feeds
        return True

    def _search_subreddit(self, subreddit_name: str, query: str, limit: int = 25) -> list[dict]:
        """Search a subreddit via its public JSON endpoint."""
        posts: list[dict] = []
        try:
            url = f"https://www.reddit.com/r/{subreddit_name}/search.json"
            params = {
                "q": query,
                "sort": "relevance",
                "t": "week",
                "limit": str(limit),
                "restrict_sr": "on",
            }
            resp = httpx.get(url, params=params, headers=_HEADERS,
                             timeout=15, follow_redirects=True)

            if resp.status_code == 429:
                logger.warning("Reddit rate limited on r/%s — backing off", subreddit_name)
                time.sleep(3)
                return posts

            if resp.status_code != 200:
                logger.warning("Reddit r/%s returned %d", subreddit_name, resp.status_code)
                return posts

            data = resp.json()
            children = data.get("data", {}).get("children", [])

            for child in children:
                post = child.get("data", {})
                if not post:
                    continue
                text = f"{post.get('title', '')} {post.get('selftext', '') or ''}"
                posts.append({
                    "title": (post.get("title", "") or "")[:200],
                    "score": post.get("score", 0),
                    "num_comments": post.get("num_comments", 0),
                    "url": f"https://reddit.com{post.get('permalink', '')}",
                    "created_utc": datetime.fromtimestamp(
                        post.get("created_utc", 0), tz=timezone.utc
                    ).isoformat(),
                    "sentiment": _simple_sentiment(text),
                    "subreddit": subreddit_name,
                    "upvote_ratio": post.get("upvote_ratio", 0),
                })

        except Exception as exc:
            logger.warning("Reddit search failed for r/%s q=%s: %s", subreddit_name, query, exc)
        return posts

    def fetch_company_data(self, entity: dict) -> list[ProviderResult]:
        results: list[ProviderResult] = []
        ticker = entity.get("ticker", "")
        name = entity.get("name", "")
        if not ticker:
            return [ProviderResult(provider=self.provider_name, data_type="social",
                                   success=False, error="No ticker")]

        # Search each subreddit for both ticker and company name
        search_terms = [ticker]
        if name and name.lower() != ticker.lower():
            search_terms.append(name)

        all_posts: list[dict] = []
        for sub_name in _SUBREDDITS:
            for term in search_terms:
                posts = self._search_subreddit(sub_name, term, limit=25)
                all_posts.extend(posts)
                # Be polite between requests
                time.sleep(0.5)

        # Deduplicate by URL
        seen_urls: set[str] = set()
        unique_posts: list[dict] = []
        for p in all_posts:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                unique_posts.append(p)

        if not unique_posts:
            results.append(ProviderResult(provider=self.provider_name, data_type="social",
                                          records_stored=0, success=False,
                                          error="No Reddit posts found"))
            return results

        # Aggregate
        today_str = date.today().isoformat()
        mention_count = len(unique_posts)
        sentiments = [p["sentiment"] for p in unique_posts if p.get("sentiment") is not None]
        avg_sentiment = round(sum(sentiments) / len(sentiments), 4) if sentiments else 0.0

        # Top posts by engagement (score * comments)
        top_posts = sorted(unique_posts,
                           key=lambda x: x.get("score", 0) * max(x.get("num_comments", 1), 1),
                           reverse=True)[:10]

        try:
            db.upsert_social_signal(
                entity_id=entity.get("id"),
                ticker=ticker,
                platform="reddit",
                signal_date=today_str,
                mention_count=mention_count,
                avg_sentiment=avg_sentiment,
                top_posts=top_posts,
                source_provider=self.provider_name,
            )
            results.append(ProviderResult(
                provider=self.provider_name, data_type="social",
                records_stored=1, success=True,
            ))
            sentiment_label = "bullish" if avg_sentiment > 0.15 else (
                "bearish" if avg_sentiment < -0.15 else "neutral")
            logger.info("Reddit: %d posts for %s, sentiment=%.3f (%s)",
                        mention_count, ticker, avg_sentiment, sentiment_label)
        except Exception as exc:
            logger.warning("Reddit store failed: %s", exc)
            results.append(ProviderResult(provider=self.provider_name, data_type="social",
                                          success=False, error=str(exc)))

        return results
