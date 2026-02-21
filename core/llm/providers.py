"""
LLM provider backends and caching layer using LiteLLM.

Supports cloud LLM APIs as the primary backend, with local Ollama as fallback
when no API key is configured.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Generator

import litellm

from core.config import get_settings

logger = logging.getLogger(__name__)

# Configure litellm to be less noisy
litellm.suppress_debug_info = True

# ---------------------------------------------------------------------------
# Redis LLM response cache
# ---------------------------------------------------------------------------

_cache_initialized = False

def _init_cache() -> None:
    """Lazy-init Redis cache for LiteLLM."""
    global _cache_initialized
    if _cache_initialized:
        return
    
    settings = get_settings()
    try:
        import redis
        # Test connection
        rc = redis.Redis(host=settings.redis_host, port=settings.redis_port, db=1, socket_connect_timeout=2)
        rc.ping()
        
        litellm.cache = litellm.Cache(
            type="redis", 
            host=settings.redis_host, 
            port=settings.redis_port, 
            db=1
        )
        _cache_initialized = True
    except Exception:
        logger.debug("Redis unavailable for LLM cache — caching disabled")
        _cache_initialized = True  # don't retry continuously


# ---------------------------------------------------------------------------
# Provider routing
# ---------------------------------------------------------------------------

# Provider defaults: { provider_name: (default_base_url, default_model) }
_PROVIDER_DEFAULTS: dict[str, tuple[str, str]] = {
    "gemini": ("", "gemini/gemini-2.0-flash"),
    "claude": ("", "anthropic/claude-3-5-sonnet-20241022"),
    "anthropic": ("", "anthropic/claude-3-5-sonnet-20241022"),
    "openai": ("", "openai/gpt-4o-mini"),
    "groq": ("", "groq/llama-3.1-8b-instant"),
    "openrouter": ("", "openrouter/meta-llama/llama-3.1-8b-instruct:free"),
    "together": ("", "together_ai/meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo"),
}


def _is_cloud_provider() -> bool:
    settings = get_settings()
    provider = settings.llm_provider.lower()
    has_key = bool(settings.llm_api_key)
    return provider not in ("", "ollama") and has_key


def _get_litellm_kwargs(target_model: str | None = None) -> dict[str, Any]:
    """Resolve litellm parameters from settings."""
    settings = get_settings()
    kwargs: dict[str, Any] = {}

    if _is_cloud_provider():
        provider = settings.llm_provider.lower()
        defaults = _PROVIDER_DEFAULTS.get(provider, ("", ""))
        base_url = settings.llm_api_base_url or defaults[0]
        model = settings.llm_cloud_model or defaults[1]

        kwargs["model"] = model
        kwargs["api_key"] = settings.llm_api_key
        if base_url:
            kwargs["api_base"] = base_url
    else:
        # Fallback to local Ollama
        model = target_model or settings.ollama_generate_model
        kwargs["model"] = f"ollama/{model}"
        kwargs["api_base"] = settings.ollama_host

    # Add litellm fallback/retry settings
    kwargs["num_retries"] = 3
    kwargs["timeout"] = settings.ollama_request_timeout
    return kwargs


# ---------------------------------------------------------------------------
# Async generation with concurrency semaphore
# ---------------------------------------------------------------------------

_semaphore: asyncio.Semaphore | None = None

def _get_semaphore() -> asyncio.Semaphore:
    """Lazy-init semaphore (must be created inside an event loop)."""
    global _semaphore
    if _semaphore is None:
        settings = get_settings()
        _semaphore = asyncio.Semaphore(settings.ollama_max_concurrent)
    return _semaphore


# ---------------------------------------------------------------------------
# Public generation API
# ---------------------------------------------------------------------------

def ollama_generate(
    prompt: str,
    *,
    system: str = "",
    model: str | None = None,
    temperature: float = 0.3,
    max_tokens: int = 1024,
) -> str | None:
    """Generate text via configured LLM provider (synchronous)."""
    _init_cache()
    kwargs = _get_litellm_kwargs(model)
    
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    
    try:
        response = litellm.completion(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            caching=True,
            **kwargs
        )
        return response.choices[0].message.content.strip()
    except Exception as exc:
        logger.warning(f"LLM generate failed: {exc}")
        return None


async def ollama_generate_async(
    prompt: str,
    *,
    system: str = "",
    model: str | None = None,
    temperature: float = 0.3,
    max_tokens: int = 1024,
) -> str | None:
    """Async generation with concurrency semaphore."""
    _init_cache()
    kwargs = _get_litellm_kwargs(model)
    
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    
    sem = _get_semaphore()
    async with sem:
        try:
            response = await litellm.acompletion(
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                caching=True,
                **kwargs
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:
            logger.warning(f"LLM async generate failed: {exc}")
            return None


def ollama_generate_stream(
    prompt: str,
    *,
    system: str = "",
    model: str | None = None,
    temperature: float = 0.3,
    max_tokens: int = 1024,
) -> Generator[str, None, None]:
    """Stream tokens word-by-word."""
    kwargs = _get_litellm_kwargs(model)
    
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    try:
        response = litellm.completion(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
            **kwargs
        )
        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
    except Exception as exc:
        logger.warning(f"LLM streaming generate failed: {exc}")
