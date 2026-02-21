'use client'

import type { StreamState } from '../lib/types'
import { fmt } from '../lib/format'

// ---------------------------------------------------------------------------
// MacroZone — macro indicators, historical trends, SEC filings
// ---------------------------------------------------------------------------

export function MacroZone({ s }: { s: StreamState }) {
    const fp = s.financialPerformance as Record<string, Record<string, unknown>> | null
    const macro = s.macroContext as Record<string, Record<string, unknown>> | null
    const hist = s.historicalTrends as Record<string, unknown> | null

    const hasFinancials = fp && Object.keys(fp).length > 0
    const hasMacro = macro && (macro as Record<string, unknown>).available
    const hasHistory = hist && (hist as Record<string, unknown>).available

    if (!hasFinancials && !hasMacro && !hasHistory) return null

    return (
        <div className="zone fade-in">
            <div className="zone-header">MACRO & FUNDAMENTALS</div>

            {/* Macro indicators */}
            {hasMacro ? (
                <div className="fin-section">
                    <div className="fin-title">MACRO INDICATORS</div>
                    <div className="macro-rows">
                        {Object.entries(macro.indicators || {})
                            .slice(0, 8)
                            .map(([id, ind]) => {
                                const d = ind as Record<string, unknown>
                                return (
                                    <div key={id} className="macro-row">
                                        <span className="macro-key">{String(d.name || id)}</span>
                                        <span className="macro-val">
                                            {d.value != null ? String(d.value) : '—'}
                                        </span>
                                    </div>
                                )
                            })}
                    </div>
                </div>
            ) : null}

            {/* Historical trend summary */}
            {hasHistory ? (
                <div className="fin-section">
                    <div className="fin-title">HISTORICAL TREND</div>
                    <div className="macro-rows">
                        <div className="macro-row">
                            <span className="macro-key">DIRECTION</span>
                            <span className="macro-val">
                                {String(
                                    (hist as Record<string, unknown>).trend_direction || 'STABLE',
                                ).toUpperCase()}
                            </span>
                        </div>
                        <div className="macro-row">
                            <span className="macro-key">QUARTERS</span>
                            <span className="macro-val">
                                {String((hist as Record<string, unknown>).quarters_available || 0)}
                            </span>
                        </div>
                    </div>
                    {s.trendAnalysis ? (
                        <div className="ai-generated-section">
                            <span className="ai-badge">🤖 AI GENERATED</span>
                            <div style={{ marginTop: 6, fontSize: 11, color: '#999', lineHeight: 1.5 }}>
                                {s.trendAnalysis}
                            </div>
                        </div>
                    ) : null}

                    {/* Quarterly data table */}
                    {(
                        (hist as Record<string, unknown>).quarters as Array<
                            Record<string, unknown>
                        > || []
                    ).length > 0 ? (
                        <details>
                            <summary>Quarterly Data</summary>
                            <table className="data-table">
                                <thead>
                                    <tr>
                                        <th>PERIOD</th>
                                        <th>REVENUE</th>
                                        <th>NET INC</th>
                                        <th>EPS</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {(
                                        (hist as Record<string, unknown>).quarters as Array<
                                            Record<string, unknown>
                                        > || []
                                    ).map((q, i) => (
                                        <tr key={i}>
                                            <td>{String(q.period_end ?? '')}</td>
                                            <td>{fmt(q.revenue)}</td>
                                            <td>{fmt(q.net_income)}</td>
                                            <td>{q.eps != null ? String(q.eps) : '—'}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </details>
                    ) : null}
                </div>
            ) : null}

            {/* SEC Filings */}
            {s.filings && (s.filings as Record<string, unknown>).available ? (
                <div className="fin-section">
                    <div className="fin-title">
                        SEC FILINGS ({String((s.filings as Record<string, unknown>).count || 0)})
                    </div>
                    <div className="macro-rows">
                        {(
                            (s.filings as Record<string, unknown>).filings as Array<
                                Record<string, unknown>
                            > || []
                        )
                            .slice(0, 5)
                            .map((f, i) => (
                                <div key={i} className="macro-row">
                                    <span className="macro-key">{String(f.type)}</span>
                                    <span className="macro-val">
                                        {String(f.date)}
                                        {f.url ? (
                                            <>
                                                {' '}
                                                —{' '}
                                                <a
                                                    href={String(f.url)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    style={{ color: '#00FF41' }}
                                                >
                                                    VIEW
                                                </a>
                                            </>
                                        ) : null}
                                    </span>
                                </div>
                            ))}
                    </div>
                </div>
            ) : null}
        </div>
    )
}
