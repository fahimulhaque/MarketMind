"""
Database connection and schema initialization.

Provides the shared ``get_connection`` helper used by every other module in
this package, plus ``init_db`` which creates all tables and indexes.
"""

from __future__ import annotations

import psycopg2
from psycopg2.extras import RealDictCursor  # noqa: F401 — re-exported for siblings

from core.config import get_settings


def get_connection():
    """Return a new psycopg2 connection using the app settings."""
    settings = get_settings()
    return psycopg2.connect(settings.postgres_dsn)


def init_db() -> None:
    """Create all tables and indexes (idempotent)."""
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS sources (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    connector_type TEXT NOT NULL DEFAULT 'web',
                    deleted_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                ALTER TABLE sources
                ADD COLUMN IF NOT EXISTS connector_type TEXT NOT NULL DEFAULT 'web';
                """
            )
            cursor.execute(
                """
                ALTER TABLE sources
                ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS source_snapshots (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    content_hash TEXT NOT NULL,
                    content_excerpt TEXT,
                    observed_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS insights (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    source_name TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    insight TEXT NOT NULL,
                    threat_level TEXT NOT NULL,
                    recommendation TEXT NOT NULL,
                    evidence_ref TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5,
                    critic_status TEXT NOT NULL DEFAULT 'approved',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                ALTER TABLE insights
                ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5;
                """
            )
            cursor.execute(
                """
                ALTER TABLE insights
                ADD COLUMN IF NOT EXISTS critic_status TEXT NOT NULL DEFAULT 'approved';
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_events (
                    id SERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT,
                    detail TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS ingest_runs (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    status TEXT NOT NULL,
                    detail TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS failed_ingestions (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER,
                    source_url TEXT,
                    error_type TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    retryable BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS reports (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    content_markdown TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS report_runs (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER,
                    report_id INTEGER,
                    status TEXT NOT NULL,
                    duration_ms INTEGER,
                    detail TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS search_queries (
                    id SERIAL PRIMARY KEY,
                    query_text TEXT NOT NULL,
                    answer_summary TEXT NOT NULL,
                    confidence DOUBLE PRECISION NOT NULL,
                    risk_level TEXT NOT NULL,
                    recommendation TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS search_evidence (
                    id SERIAL PRIMARY KEY,
                    search_query_id INTEGER NOT NULL REFERENCES search_queries(id) ON DELETE CASCADE,
                    source_id INTEGER,
                    source_name TEXT,
                    evidence_ref TEXT NOT NULL,
                    insight_excerpt TEXT,
                    confidence DOUBLE PRECISION,
                    recency_score DOUBLE PRECISION,
                    rank_score DOUBLE PRECISION,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS deletion_requests (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    requested_by TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    detail TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    executed_at TIMESTAMPTZ
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS memory_chunks (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    source_name TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    chunk_text TEXT NOT NULL,
                    evidence_ref TEXT NOT NULL,
                    embedding vector(768),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (source_id, content_hash, chunk_index)
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS source_evidence_relations (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    evidence_ref TEXT NOT NULL,
                    threat_level TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (source_id, evidence_ref)
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS retention_runs (
                    id SERIAL PRIMARY KEY,
                    status TEXT NOT NULL,
                    detail TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            # --- Data Provider Tables ---
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS entities (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    ticker TEXT UNIQUE,
                    cik TEXT,
                    sector TEXT,
                    industry TEXT,
                    exchange TEXT,
                    entity_type TEXT NOT NULL DEFAULT 'company',
                    aliases JSONB DEFAULT '[]'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_periods (
                    id SERIAL PRIMARY KEY,
                    entity_id INTEGER REFERENCES entities(id) ON DELETE CASCADE,
                    ticker TEXT NOT NULL,
                    period_type TEXT NOT NULL,
                    period_end_date DATE NOT NULL,
                    fiscal_year INTEGER,
                    fiscal_quarter INTEGER,
                    source_provider TEXT NOT NULL,
                    income_statement JSONB DEFAULT '{}'::jsonb,
                    balance_sheet JSONB DEFAULT '{}'::jsonb,
                    cash_flow JSONB DEFAULT '{}'::jsonb,
                    key_metrics JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (ticker, period_type, period_end_date, source_provider)
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS macro_indicators (
                    id SERIAL PRIMARY KEY,
                    series_id TEXT NOT NULL,
                    series_name TEXT,
                    observation_date DATE NOT NULL,
                    value NUMERIC,
                    source_provider TEXT NOT NULL DEFAULT 'fred',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (series_id, observation_date)
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS social_signals (
                    id SERIAL PRIMARY KEY,
                    entity_id INTEGER REFERENCES entities(id) ON DELETE CASCADE,
                    ticker TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    signal_date DATE NOT NULL,
                    mention_count INTEGER DEFAULT 0,
                    avg_sentiment NUMERIC DEFAULT 0.0,
                    top_posts JSONB DEFAULT '[]'::jsonb,
                    source_provider TEXT NOT NULL DEFAULT 'reddit',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (ticker, platform, signal_date)
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS entity_filings (
                    id SERIAL PRIMARY KEY,
                    entity_id INTEGER REFERENCES entities(id) ON DELETE CASCADE,
                    ticker TEXT NOT NULL,
                    cik TEXT,
                    filing_type TEXT NOT NULL,
                    filing_date DATE NOT NULL,
                    accession_number TEXT UNIQUE,
                    filing_url TEXT,
                    description TEXT,
                    source_provider TEXT NOT NULL DEFAULT 'sec_edgar',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS entity_coverage (
                    id SERIAL PRIMARY KEY,
                    entity_id INTEGER UNIQUE REFERENCES entities(id) ON DELETE CASCADE,
                    ticker TEXT UNIQUE NOT NULL,
                    has_financials BOOLEAN DEFAULT FALSE,
                    financials_quarters INTEGER DEFAULT 0,
                    has_filings BOOLEAN DEFAULT FALSE,
                    filings_count INTEGER DEFAULT 0,
                    has_macro BOOLEAN DEFAULT FALSE,
                    has_social BOOLEAN DEFAULT FALSE,
                    has_news BOOLEAN DEFAULT FALSE,
                    has_price BOOLEAN DEFAULT FALSE,
                    coverage_score NUMERIC DEFAULT 0.0,
                    last_updated TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            # Performance indexes
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_insights_fts
                ON insights USING gin(to_tsvector('english', insight || ' ' || recommendation));
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_memory_chunks_embedding
                ON memory_chunks USING hnsw (embedding vector_cosine_ops);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_financial_periods_ticker
                ON financial_periods (ticker, period_type, period_end_date DESC);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_entity_filings_ticker
                ON entity_filings (ticker, filing_type, filing_date DESC);
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_macro_indicators_series
                ON macro_indicators (series_id, observation_date DESC);
                """
            )
        connection.commit()
