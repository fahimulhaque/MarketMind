import httpx

from connectors.base import BaseConnector
from core.config import get_settings


class HttpConnector(BaseConnector):
    def fetch(self, source_url: str) -> str:
        settings = get_settings()
        response = httpx.get(
            source_url,
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": settings.ingest_user_agent},
        )
        response.raise_for_status()
        return response.text
