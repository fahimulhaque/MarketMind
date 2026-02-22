'use client'

import type { StreamState } from '../lib/types'
import { GaugeBar } from './GaugeBar'
import { AnalystConsensus } from './AnalystConsensus'
import { InsiderActivity } from './InsiderActivity'
import { getCurrencySymbol } from '../lib/format'

// ---------------------------------------------------------------------------
// SignalDash — sentiment gauges, verdict, risk, signals, contradictions
// ---------------------------------------------------------------------------

export function SignalDash({ s }: { s: StreamState }) {
    const decision = s.decision
    const sentiment = s.socialSentiment as Record<string, unknown> | null
    const coverage = s.coverage as Record<string, unknown> | null

    // Get dynamic currency symbol from financial performance
    const fp = s.financialPerformance as Record<string, unknown> | null
    const currencyCode = String(fp?.currency || 'USD')
    const currencySymbol = getCurrencySymbol(currencyCode)

    // Derive gauge values
    const confidencePct = decision ? Math.round(decision.confidence * 100) : 0
    const coveragePct =
        coverage && (coverage as Record<string, unknown>).available
            ? Math.round(Number((coverage as Record<string, unknown>).score || 0) * 100)
            : 0

    // Simple sentiment extraction
    let sentimentPct = 50
    let sentimentLabel = 'NEUTRAL'
    if (sentiment && (sentiment as Record<string, unknown>).available) {
        const summary = String(
            (sentiment as Record<string, unknown>).summary || '',
        ).toLowerCase()
        if (summary.includes('bullish') || summary.includes('positive')) {
            sentimentPct = 80
            sentimentLabel = 'BULLISH'
        } else if (summary.includes('bearish') || summary.includes('negative')) {
            sentimentPct = 20
            sentimentLabel = 'BEARISH'
        } else if (summary.includes('mixed')) {
            sentimentPct = 50
            sentimentLabel = 'MIXED'
        }
    }

    // Risk as volatility proxy
    const riskLevel = decision?.risk_level || 'unknown'
    let volPct = 50
    if (riskLevel === 'high') volPct = 80
    else if (riskLevel === 'medium') volPct = 50
    else if (riskLevel === 'low') volPct = 20

    const hasAny =
        decision ||
        (sentiment && (sentiment as Record<string, unknown>).available) ||
        (coverage && (coverage as Record<string, unknown>).available)
    if (!hasAny) return null

    return (
        <div className="zone fade-in">
            <div className="zone-header">SIGNAL DASHBOARD</div>
            <div className="gauge-list">
                {decision ? (
                    <>
                        <GaugeBar
                            label={`SENTIMENT: ${sentimentLabel}`}
                            value={sentimentPct}
                            color={sentimentPct >= 60 ? 'green' : sentimentPct <= 40 ? 'red' : 'yellow'}
                        />
                        <GaugeBar
                            label="VOLATILITY"
                            value={volPct}
                            color={volPct >= 60 ? 'red' : volPct >= 40 ? 'orange' : 'green'}
                        />
                        <GaugeBar
                            label="CONFIDENCE"
                            value={confidencePct}
                            color={confidencePct >= 70 ? 'green' : confidencePct >= 40 ? 'yellow' : 'red'}
                        />
                        <GaugeBar
                            label="DATA COVERAGE"
                            value={coveragePct}
                            color={coveragePct >= 70 ? 'green' : coveragePct >= 30 ? 'yellow' : 'red'}
                        />
                    </>
                ) : null}

                {/* Decision cards */}
                {decision
                    ? (() => {
                        const recText = decision.recommendation || ''
                        const actionWords = [
                            'BUY',
                            'SELL',
                            'HOLD',
                            'ACCUMULATE',
                            'REDUCE',
                            'MONITOR',
                            'INVESTIGATE',
                        ]
                        const firstWord = recText.trim().split(/[\s,.]+/)[0].toUpperCase()
                        const action = actionWords.includes(firstWord) ? firstWord : 'MONITOR'

                        return (
                            <>
                                <div className="signal-card">
                                    <div className="signal-card-label">VERDICT</div>
                                    <div
                                        className={`signal-card-val ${action === 'BUY' || action === 'ACCUMULATE'
                                            ? 'bullish'
                                            : action === 'SELL' || action === 'REDUCE'
                                                ? 'bearish'
                                                : 'neutral'
                                            }`}
                                    >
                                        {action}
                                    </div>
                                </div>
                                <div className="signal-card">
                                    <div className="signal-card-label">RISK LEVEL</div>
                                    <div className={`signal-card-val ${riskLevel}`}>
                                        {riskLevel.toUpperCase()}
                                    </div>
                                </div>
                                {/* Full recommendation text */}
                                <div className="ai-generated-section">
                                    <span className="ai-badge">🤖 AI GENERATED</span>
                                    <div style={{ marginTop: 8, fontSize: 11, lineHeight: 1.6, color: '#ccc' }}>
                                        {recText}
                                    </div>
                                </div>
                            </>
                        )
                    })()
                    : null}

                {/* Analyst Consensus & Insider Activity */}
                {s.analystConsensus && <AnalystConsensus data={s.analystConsensus} isStreaming={s.analystStreaming} currencySymbol={currencySymbol} />}
                {s.insiderActivity && <InsiderActivity data={s.insiderActivity} isStreaming={s.insiderStreaming} />}

                {/* Signal shifts */}
                {s.signalShifts && s.signalShifts.length > 0 ? (
                    <div style={{ marginTop: 8 }}>
                        <div className="fin-title">SIGNALS</div>
                        <ul className="narrative-list">
                            {s.signalShifts.map((shift, i) => (
                                <li key={i}>{shift}</li>
                            ))}
                        </ul>
                    </div>
                ) : null}

                {/* Contradictions */}
                {s.contradictions && s.contradictions.length > 0 ? (
                    <div style={{ marginTop: 8 }}>
                        <div className="fin-title">CONTRADICTIONS</div>
                        <ul className="narrative-list">
                            {s.contradictions.map((c, i) => (
                                <li key={i} className="nar-alert">
                                    {String(c.type)}: {String(c.detail)}
                                </li>
                            ))}
                        </ul>
                    </div>
                ) : null}
            </div>
        </div>
    )
}
