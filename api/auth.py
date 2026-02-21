from fastapi import Header, HTTPException, status

from core.config import get_settings


def require_write_access(x_api_key: str = Header(default="", alias="X-API-Key")) -> None:
    settings = get_settings()
    if x_api_key != settings.api_write_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key for write access",
        )
