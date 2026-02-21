"""Financial periods CRUD — upsert and query quarterly/annual financial data."""

from __future__ import annotations

import json as _json

from psycopg2.extras import RealDictCursor

from core.db.connection import get_connection


def upsert_financial_period(
    *,
    entity_id: int | None,
    ticker: str,
    period_type: str,
    period_end_date: str,
    fiscal_year: int | None = None,
    fiscal_quarter: int | None = None,
    source_provider: str,
    income_statement: dict | None = None,
    balance_sheet: dict | None = None,
    cash_flow: dict | None = None,
    key_metrics: dict | None = None,
) -> dict:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO financial_periods
                    (entity_id, ticker, period_type, period_end_date, fiscal_year,
                     fiscal_quarter, source_provider, income_statement, balance_sheet,
                     cash_flow, key_metrics)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
                ON CONFLICT (ticker, period_type, period_end_date, source_provider) DO UPDATE SET
                    entity_id = COALESCE(EXCLUDED.entity_id, financial_periods.entity_id),
                    fiscal_year = COALESCE(EXCLUDED.fiscal_year, financial_periods.fiscal_year),
                    fiscal_quarter = COALESCE(EXCLUDED.fiscal_quarter, financial_periods.fiscal_quarter),
                    income_statement = CASE
                        WHEN EXCLUDED.income_statement::text = '{}'
                        THEN financial_periods.income_statement
                        ELSE financial_periods.income_statement || EXCLUDED.income_statement
                    END,
                    balance_sheet = CASE
                        WHEN EXCLUDED.balance_sheet::text = '{}'
                        THEN financial_periods.balance_sheet
                        ELSE financial_periods.balance_sheet || EXCLUDED.balance_sheet
                    END,
                    cash_flow = CASE
                        WHEN EXCLUDED.cash_flow::text = '{}'
                        THEN financial_periods.cash_flow
                        ELSE financial_periods.cash_flow || EXCLUDED.cash_flow
                    END,
                    key_metrics = CASE
                        WHEN EXCLUDED.key_metrics::text = '{}'
                        THEN financial_periods.key_metrics
                        ELSE financial_periods.key_metrics || EXCLUDED.key_metrics
                    END
                RETURNING *;
                """,
                (
                    entity_id, ticker, period_type, period_end_date, fiscal_year,
                    fiscal_quarter, source_provider,
                    _json.dumps(income_statement or {}),
                    _json.dumps(balance_sheet or {}),
                    _json.dumps(cash_flow or {}),
                    _json.dumps(key_metrics or {}),
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else {}


def get_financial_history(ticker: str, period_type: str = "quarterly", limit: int = 20) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM financial_periods
                WHERE ticker = %s AND period_type = %s
                ORDER BY period_end_date DESC
                LIMIT %s
                """,
                (ticker, period_type, limit),
            )
            return [dict(r) for r in cur.fetchall()]
