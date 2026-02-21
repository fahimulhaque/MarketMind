"""
core.db — Database access layer.

All public functions are re-exported here so existing imports like
``from core.db import get_connection`` continue to work unchanged.
"""

from core.db.connection import get_connection, init_db  # noqa: F401
from core.db.sources import (  # noqa: F401
    add_source,
    get_source,
    list_sources,
    get_latest_snapshot_hash,
    get_last_ingest_time,
    log_ingest_run,
    log_failed_ingestion,
    insert_snapshot,
    source_exists,
)
from core.db.insights import (  # noqa: F401
    insert_insight,
    count_insights,
    get_latest_insights,
    search_insights_by_query,
)
from core.db.reports import (  # noqa: F401
    insert_report,
    list_reports,
    get_report,
    log_report_run,
    get_observability_metrics,
)
from core.db.search_history import (  # noqa: F401
    save_search_result,
    get_search_history,
)
from core.db.retention import (  # noqa: F401
    list_retention_runs,
    create_deletion_request,
    list_deletion_requests,
    get_deletion_request,
    mark_deletion_request,
    delete_source_records,
    run_retention_purge,
)
from core.db.financials import (  # noqa: F401
    upsert_financial_period,
    get_financial_history,
)
from core.db.macro import (  # noqa: F401
    upsert_macro_indicator,
    get_macro_series,
    get_latest_macro_values,
)
from core.db.social import (  # noqa: F401
    upsert_social_signal,
    get_social_signals,
)
from core.db.filings import (  # noqa: F401
    upsert_entity_filing,
    get_entity_filings,
)
from core.db.coverage import (  # noqa: F401
    update_entity_coverage,
    get_entity_coverage,
)

__all__ = [
    # connection
    "get_connection",
    "init_db",
    # sources
    "add_source",
    "get_source",
    "list_sources",
    "get_latest_snapshot_hash",
    "get_last_ingest_time",
    "log_ingest_run",
    "log_failed_ingestion",
    "insert_snapshot",
    "source_exists",
    # insights
    "insert_insight",
    "count_insights",
    "get_latest_insights",
    "search_insights_by_query",
    # reports
    "insert_report",
    "list_reports",
    "get_report",
    "log_report_run",
    "get_observability_metrics",
    # search history
    "save_search_result",
    "get_search_history",
    # retention
    "list_retention_runs",
    "create_deletion_request",
    "list_deletion_requests",
    "get_deletion_request",
    "mark_deletion_request",
    "delete_source_records",
    "run_retention_purge",
    # financials
    "upsert_financial_period",
    "get_financial_history",
    # macro
    "upsert_macro_indicator",
    "get_macro_series",
    "get_latest_macro_values",
    # social
    "upsert_social_signal",
    "get_social_signals",
    # filings
    "upsert_entity_filing",
    "get_entity_filings",
    # coverage
    "update_entity_coverage",
    "get_entity_coverage",
]
