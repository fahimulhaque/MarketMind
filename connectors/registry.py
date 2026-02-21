from connectors.base import BaseConnector
from connectors.api.rss_connector import RssConnector
from connectors.web.http_connector import HttpConnector


def get_connector(connector_type: str) -> BaseConnector:
    if connector_type == "web":
        return HttpConnector()
    if connector_type == "rss":
        return RssConnector()
    raise ValueError(f"Unsupported connector type: {connector_type}")
