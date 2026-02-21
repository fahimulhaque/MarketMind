"""yfinance analyst consensus and news fetcher.

Fetches recommendations, price targets, and recent news using the yfinance library.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def fetch_analyst_consensus(ticker: str) -> dict[str, Any]:
    """Fetch analyst recommendations and price targets from yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available for analyst consensus")
        return {"available": False}

    try:
        t = yf.Ticker(ticker)
        
        # 1. Price Targets
        targets = {}
        try:
            # yfinance > 0.2.x uses analyst_price_targets
            apt = getattr(t, "analyst_price_targets", None)
            if apt is not None:
                targets = apt
            else:
                # Fallback to info dict
                info = t.info
                targets = {
                    "low": info.get("targetLowPrice"),
                    "mean": info.get("targetMeanPrice"),
                    "high": info.get("targetHighPrice"),
                    "median": info.get("targetMedianPrice"),
                }
        except Exception as exc:
            logger.debug("Failed to extract price targets for %s: %s", ticker, exc)

        # 2. Recommendations summary
        buy = hold = sell = 0
        try:
            rs = getattr(t, "recommendations_summary", None)
            if rs is not None and not rs.empty:
                # rs is a DataFrame with columns like 'period', 'strongBuy', 'buy', 'hold', 'sell', 'strongSell'
                # Usually row 0 is the current period (0m)
                row = rs.iloc[0]
                strong_buy = int(row.get("strongBuy", 0)) if "strongBuy" in row else 0
                r_buy = int(row.get("buy", 0)) if "buy" in row else 0
                buy = strong_buy + r_buy
                hold = int(row.get("hold", 0)) if "hold" in row else 0
                r_sell = int(row.get("sell", 0)) if "sell" in row else 0
                strong_sell = int(row.get("strongSell", 0)) if "strongSell" in row else 0
                sell = r_sell + strong_sell
        except Exception as exc:
            logger.debug("Failed to extract recommendations for %s: %s", ticker, exc)

        analyst_count = buy + hold + sell

        return {
            "available": analyst_count > 0 or targets.get("mean") is not None,
            "buy": buy,
            "hold": hold,
            "sell": sell,
            "analyst_count": analyst_count,
            "target_low": targets.get("low"),
            "target_mean": targets.get("mean", targets.get("current")),
            "target_high": targets.get("high"),
            "target_median": targets.get("median", targets.get("mean")),
        }
    except Exception as exc:
        logger.warning("Failed to fetch analyst consensus for %s: %s", ticker, exc)
        return {"available": False}


def fetch_market_news(ticker: str, limit: int = 5) -> dict[str, Any]:
    """Fetch recent news headlines from yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available for market news")
        return {"available": False, "articles": []}

    try:
        t = yf.Ticker(ticker)
        news_items = getattr(t, "news", [])
        
        articles = []
        for item in news_items[:limit]:
            # yfinance news items now nest data under "content"
            content = item.get("content", item)
            
            # extract string fields safely
            title = content.get("title", "")
            
            provider = content.get("provider", {})
            publisher = provider.get("displayName", "") if isinstance(provider, dict) else ""
            if not publisher:
                publisher = content.get("publisher", "")
                
            links = content.get("clickThroughUrl", {})
            link = links.get("url", "") if isinstance(links, dict) else ""
            if not link:
                link = content.get("link", "")
                
            timestamp = content.get("pubDate", content.get("providerPublishTime", 0))

            articles.append({
                "title": title,
                "publisher": publisher,
                "link": link,
                "timestamp": timestamp,
            })
            
        return {
            "available": len(articles) > 0,
            "articles": articles,
        }
    except Exception as exc:
        logger.warning("Failed to fetch market news for %s: %s", ticker, exc)
        return {"available": False, "articles": []}


def fetch_insider_activity(ticker: str) -> dict[str, Any]:
    """Fetch insider trading activity from yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not available for insider activity")
        return {"available": False, "transactions": []}

    try:
        t = yf.Ticker(ticker)
        ins = getattr(t, "insider_transactions", None)
        if ins is None or (hasattr(ins, "empty") and ins.empty):
            return {"available": False, "transactions": []}

        transactions = []
        buy_count = sell_count = 0

        # We need to handle variations in yfinance dataframe columns 
        # Usually: 'Start Date', 'Shares', 'Value', 'Text', 'Insider', 'Position'
        for _, row in ins.iterrows() if hasattr(ins, "iterrows") else []:
            row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
            
            # Identify columns
            date = str(row_dict.get("Start Date", row_dict.get("Date", "")))[:10]
            name = str(row_dict.get("Insider", row_dict.get("Name", "")))
            title = str(row_dict.get("Position", row_dict.get("Title", "")))
            text = str(row_dict.get("Text", "")).lower()
            shares = float(row_dict.get("Shares", 0))
            value = float(row_dict.get("Value", 0))

            if not name or "sale" in text:
                t_type = "SELL"
                sell_count += 1
            elif "purchase" in text or "buy" in text:
                t_type = "BUY"
                buy_count += 1
            else:
                t_type = "GRANT" # mostly grants or stock options
            
            transactions.append({
                "date": date,
                "name": name,
                "title": title,
                "type": t_type,
                "shares": shares,
                "value": value
            })

            if len(transactions) >= 15:
                break
                
        net_dir = "NET BUYING" if buy_count > sell_count else "NET SELLING" if sell_count > buy_count else "NEUTRAL"
        
        return {
            "available": len(transactions) > 0,
            "transactions": transactions,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "net_direction": net_dir,
        }
    except Exception as exc:
        logger.warning("Failed to fetch insider activity for %s: %s", ticker, exc)
        return {"available": False, "transactions": []}
