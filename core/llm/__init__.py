"""
core.llm — LLM generation layer.

All public functions are re-exported here so existing imports like
``from core.llm import ollama_generate`` continue to work unchanged.
"""

from core.llm.providers import (  # noqa: F401
    ollama_generate,
    ollama_generate_async,
    ollama_generate_stream,
)
from core.llm.generators import (  # noqa: F401
    generate_executive_summary,
    generate_narrative,
    generate_market_narrative,
    generate_scenarios,
    generate_recommendation,
    generate_trend_analysis,
    generate_competitive_landscape,
    generate_parallel_intelligence,
)
from core.llm.streaming import (  # noqa: F401
    generate_executive_summary_stream,
    generate_market_narrative_stream,
    generate_competitive_landscape_stream,
)

__all__ = [
    # providers
    "ollama_generate",
    "ollama_generate_async",
    "ollama_generate_stream",
    # generators
    "generate_executive_summary",
    "generate_narrative",
    "generate_market_narrative",
    "generate_scenarios",
    "generate_recommendation",
    "generate_trend_analysis",
    "generate_competitive_landscape",
    "generate_parallel_intelligence",
    # streaming
    "generate_executive_summary_stream",
    "generate_market_narrative_stream",
    "generate_competitive_landscape_stream",
]
