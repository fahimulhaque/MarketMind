from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from core.config import get_settings


@dataclass
class PolicyDecision:
    allowed: bool
    reason: str


import httpx

def _allowed_domains() -> set[str]:
    settings = get_settings()
    raw = settings.ingest_allowed_domains.strip()
    if not raw:
        return set()
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def _domain_allowed(url: str) -> bool:
    domains = _allowed_domains()
    if not domains:
        return True

    host = (urlparse(url).hostname or "").lower()
    return host in domains or any(host.endswith(f".{domain}") for domain in domains)


async def _robots_allowed(url: str, client: httpx.AsyncClient) -> PolicyDecision:
    settings = get_settings()
    if not settings.ingest_policy_require_robots:
        return PolicyDecision(allowed=True, reason="robots_check_disabled")

    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    parser = RobotFileParser()
    try:
        resp = await client.get(robots_url, timeout=5.0)
        if resp.status_code == 200:
            lines = resp.text.splitlines()
            parser.parse(lines)
            allowed = parser.can_fetch(settings.ingest_user_agent, url)
            if allowed:
                return PolicyDecision(allowed=True, reason="robots_allowed")
            return PolicyDecision(allowed=False, reason="robots_disallow")
        return PolicyDecision(allowed=True, reason="robots_missing_allow")
    except Exception:
        if settings.ingest_policy_deny_on_robots_error:
            return PolicyDecision(allowed=False, reason="robots_check_error_deny")
        return PolicyDecision(allowed=True, reason="robots_check_error_allow")


async def validate_source_policy(url: str, client: httpx.AsyncClient | None = None) -> PolicyDecision:
    if not _domain_allowed(url):
        return PolicyDecision(allowed=False, reason="domain_not_allowlisted")

    close_client = False
    if client is None:
        client = httpx.AsyncClient()
        close_client = True
        
    try:
        robots_decision = await _robots_allowed(url, client)
        if not robots_decision.allowed:
            return robots_decision
    finally:
        if close_client:
            await client.aclose()

    return PolicyDecision(allowed=True, reason="policy_pass")
