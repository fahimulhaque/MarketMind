import feedparser

from connectors.base import BaseConnector


class RssConnector(BaseConnector):
    def fetch(self, source_url: str) -> str:
        feed = feedparser.parse(source_url)
        if feed.bozo and not getattr(feed, "entries", None):
            raise ValueError(f"Unable to parse RSS feed: {source_url}")

        lines: list[str] = []
        for entry in feed.entries[:20]:
            title = getattr(entry, "title", "")
            summary = getattr(entry, "summary", "")
            link = getattr(entry, "link", "")
            lines.append(f"title={title}\nsummary={summary}\nlink={link}")

        if not lines:
            return f"feed_title={getattr(feed.feed, 'title', '')}\nno_entries=true"

        return "\n\n".join(lines)
