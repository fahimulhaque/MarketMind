"""
core.pipeline — Market intelligence search pipeline.

Re-exports public functions so ``from core.pipeline import run_market_intelligence_query``
(and the legacy ``from core.search import ...``) continue to work.
"""

from core.pipeline.intelligence import (  # noqa: F401
    run_market_intelligence_query,
)
from core.pipeline.stream import (  # noqa: F401
    run_market_intelligence_query_stream,
)

__all__ = [
    "run_market_intelligence_query",
    "run_market_intelligence_query_stream",
]
