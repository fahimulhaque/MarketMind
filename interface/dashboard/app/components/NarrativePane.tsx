'use client'

import ReactMarkdown from 'react-markdown'
import type { StreamState } from '../lib/types'
import { MarketNews } from './MarketNews'

// ---------------------------------------------------------------------------
// NarrativePane — executive summary, market narrative, competitive landscape,
//                 scenario analysis, citations, connected entities
// ---------------------------------------------------------------------------

export function NarrativePane({ s }: { s: StreamState }) {
    const hasDecision = s.decision || s.decisionStreaming
    const hasNarrative = s.narrative || s.narrativeStreaming
    const hasCompetitive = s.competitiveLandscape || s.competitiveStreaming
    const hasScenarios = s.scenarios && s.scenarios.length > 0

    if (!hasDecision && !hasNarrative && !hasCompetitive && !hasScenarios) return null

    return (
        <div className="zone fade-in">
            <div className="zone-header">THE NARRATIVE</div>

            {/* Executive Summary */}
            {s.decision ? (
                <div className="ai-generated-section">
                    <span className="ai-badge">🤖 AI GENERATED</span>
                    <div className="md-content">
                        <ReactMarkdown>{s.decision.executive_summary}</ReactMarkdown>
                    </div>
                </div>
            ) : s.decisionStreaming ? (
                <div className="ai-generated-section">
                    <span className="ai-badge">🤖 AI GENERATED</span>
                    <div className="md-content">
                        <ReactMarkdown>{s.decisionStreaming}</ReactMarkdown>
                        <span className="stream-cursor" />
                    </div>
                </div>
            ) : null}

            {/* Market Narrative */}
            {s.narrative ? (
                <div className="ai-generated-section">
                    <div className="nar-section-title">
                        MARKET NARRATIVE <span className="ai-badge">🤖 AI GENERATED</span>
                    </div>
                    <div className="md-content">
                        <ReactMarkdown>{s.narrative}</ReactMarkdown>
                    </div>
                </div>
            ) : s.narrativeStreaming ? (
                <div className="ai-generated-section">
                    <div className="nar-section-title">
                        MARKET NARRATIVE <span className="ai-badge">🤖 AI GENERATED</span>
                    </div>
                    <div className="md-content">
                        <ReactMarkdown>{s.narrativeStreaming}</ReactMarkdown>
                        <span className="stream-cursor" />
                    </div>
                </div>
            ) : null}

            {/* Competitive Landscape */}
            {s.competitiveLandscape ? (
                <div className="ai-generated-section">
                    <div className="nar-section-title">
                        COMPETITIVE LANDSCAPE <span className="ai-badge">🤖 AI GENERATED</span>
                    </div>
                    <div className="md-content">
                        <ReactMarkdown>{s.competitiveLandscape}</ReactMarkdown>
                    </div>
                </div>
            ) : s.competitiveStreaming ? (
                <div className="ai-generated-section">
                    <div className="nar-section-title">
                        COMPETITIVE LANDSCAPE <span className="ai-badge">🤖 AI GENERATED</span>
                    </div>
                    <div className="md-content">
                        <ReactMarkdown>{s.competitiveStreaming}</ReactMarkdown>
                        <span className="stream-cursor" />
                    </div>
                </div>
            ) : null}

            {/* Scenarios */}
            {hasScenarios ? (
                <div className="ai-generated-section">
                    <div className="nar-section-title">
                        SCENARIO ANALYSIS <span className="ai-badge">🤖 AI GENERATED</span>
                    </div>
                    <div className="scenarios-strip">
                        {s.scenarios!.map((sc) => {
                            const name = String(sc.name || '').toLowerCase()
                            const cls = name.includes('bull')
                                ? 'bull'
                                : name.includes('bear')
                                    ? 'bear'
                                    : 'base'
                            return (
                                <div key={String(sc.name)} className="scenario-cell">
                                    <div className={`scenario-name ${cls}`}>
                                        {String(sc.name).toUpperCase()}
                                    </div>
                                    <div className="scenario-prob">
                                        {((Number(sc.probability) || 0) * 100).toFixed(0)}%
                                    </div>
                                    <div className="scenario-detail">
                                        {sc.assumption ? (
                                            <>
                                                <strong>IF:</strong> {String(sc.assumption)}
                                                <br />
                                            </>
                                        ) : null}
                                        {sc.impact ? (
                                            <>
                                                <strong>THEN:</strong> {String(sc.impact)}
                                            </>
                                        ) : null}
                                    </div>
                                </div>
                            )
                        })}
                    </div>
                </div>
            ) : null}

            {/* Citations / Evidence */}
            {s.citations && s.citations.length > 0 ? (
                <div style={{ marginTop: 12 }}>
                    <div className="nar-section-title">KEY EVIDENCE</div>
                    <ul className="evidence-list">
                        {s.citations.slice(0, 5).map((c, i) => (
                            <li key={i}>
                                <strong>{String(c.source)}</strong>
                                {c.confidence
                                    ? ` — ${(Number(c.confidence) * 100).toFixed(0)}%`
                                    : ''}
                            </li>
                        ))}
                    </ul>
                </div>
            ) : null}

            {/* Connected Entities */}
            {s.connectedEntities && s.connectedEntities.length > 0 ? (
                <div style={{ marginTop: 8 }}>
                    <div className="nar-section-title">CONNECTED ENTITIES</div>
                    <div className="entity-list">
                        {s.connectedEntities.map((e, i) => (
                            <span key={i} className="entity-chip">
                                {String(e.name || e.source_name || '')}
                            </span>
                        ))}
                    </div>
                </div>
            ) : null}

            {/* Market News */}
            {s.marketNews && <MarketNews data={s.marketNews} isStreaming={s.newsStreaming} />}
        </div>
    )
}
