'use client'

import { FormEvent, useEffect, useRef, useState } from 'react'

// --- Shared types & helpers ---
import type { StageEvent, StreamState } from './lib/types'
import { emptyStreamState } from './lib/types'
import { API_BASE } from './constants'

// --- Hooks ---
import { useClocks } from './hooks/useClocks'

// --- Components ---
import { CommandBar } from './components/CommandBar'
import { BootSequence } from './components/BootSequence'
import { AssetProfile } from './components/AssetProfile'
import { MacroZone } from './components/MacroZone'
import { SignalDash } from './components/SignalDash'
import { NarrativePane } from './components/NarrativePane'
import { StatusFooter } from './components/StatusFooter'
import { AiDisclaimer } from './components/AiDisclaimer'

// ---------------------------------------------------------------------------
// DashboardPage — Main orchestrator
//
// Manages application state and the SSE stream from the backend.
// All visual rendering is delegated to extracted components.
// ---------------------------------------------------------------------------

export default function DashboardPage() {
  // ── State ──────────────────────────────────────────────────────────────
  const [query, setQuery] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [bootMsg, setBootMsg] = useState('')
  const [stream, setStream] = useState<StreamState | null>(null)
  const [complete, setComplete] = useState(false)
  const [latency, setLatency] = useState(0)

  const abortRef = useRef<AbortController | null>(null)
  const startTimeRef = useRef<number>(0)

  const clocks = useClocks()

  // ── Latency timer ──────────────────────────────────────────────────────
  useEffect(() => {
    if (!loading) return
    const id = setInterval(() => {
      setLatency(Date.now() - startTimeRef.current)
    }, 100)
    return () => clearInterval(id)
  }, [loading])

  // ── SSE event handler ──────────────────────────────────────────────────
  function processEvent(evt: StageEvent) {
    if (evt.message) setBootMsg(evt.message)
    const data = evt.data || {}

    switch (evt.stage) {
      case 'query_parsed':
        setStream((prev) => (prev ? { ...prev, queryContext: data } : prev))
        break
      case 'retrieval_complete':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              evidenceCount: Number(data.postgres_hits || 0),
              semanticMatches: Number(data.semantic_hits || 0),
              graphRelated: Number(data.graph_hits || 0),
            }
            : prev,
        )
        break
      case 'ranking_complete':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              evidenceCount: Number(data.evidence_count || prev.evidenceCount),
            }
            : prev,
        )
        break
      case 'financial_snapshot':
        setStream((prev) => (prev ? { ...prev, financialPerformance: data } : prev))
        break
      case 'historical_trends':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              historicalTrends:
                (data.trends as Record<string, unknown>) || null,
              trendAnalysis: (data.trend_analysis as string) || null,
            }
            : prev,
        )
        break
      case 'macro_context':
        setStream((prev) => (prev ? { ...prev, macroContext: data } : prev))
        break
      case 'social_sentiment':
        setStream((prev) => (prev ? { ...prev, socialSentiment: data } : prev))
        break
      case 'coverage':
        setStream((prev) => (prev ? { ...prev, coverage: data } : prev))
        break
      case 'filings':
        setStream((prev) => (prev ? { ...prev, filings: data } : prev))
        break
      case 'analyst_consensus':
        setStream((prev) => (prev ? { ...prev, analystConsensus: data } : prev))
        break
      case 'insider_activity':
        setStream((prev) => (prev ? { ...prev, insiderActivity: data } : prev))
        break
      case 'market_news':
        setStream((prev) => (prev ? { ...prev, marketNews: data } : prev))
        break
      case 'decision_ready':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              decisionStreaming: '',
              decision: {
                executive_summary: String(data.executive_summary || ''),
                recommendation: String(data.recommendation || ''),
                confidence: Number(data.confidence || 0),
                risk_level: String(data.risk_level || 'low'),
              },
            }
            : prev,
        )
        break
      case 'decision_token':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              decisionStreaming:
                prev.decisionStreaming + String(data.token || ''),
            }
            : prev,
        )
        break
      case 'narrative_ready':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              narrativeStreaming: '',
              narrative: String(data.market_narrative || ''),
            }
            : prev,
        )
        break
      case 'narrative_token':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              narrativeStreaming:
                prev.narrativeStreaming + String(data.token || ''),
            }
            : prev,
        )
        break
      case 'scenarios_ready':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              scenarios:
                (data.scenarios as Array<Record<string, unknown>>) || null,
              contradictions:
                (data.contradictions as Array<Record<string, unknown>>) ||
                null,
              signalShifts: (data.signal_shifts as string[]) || null,
            }
            : prev,
        )
        break
      case 'competitive_landscape':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              competitiveStreaming: '',
              competitiveLandscape: String(
                data.competitive_landscape || '',
              ),
            }
            : prev,
        )
        break
      case 'competitive_token':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              competitiveStreaming:
                prev.competitiveStreaming + String(data.token || ''),
            }
            : prev,
        )
        break
      case 'price_history':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              priceHistory:
                (data as unknown as StreamState['priceHistory']) || null,
            }
            : prev,
        )
        break
      case 'complete':
        setStream((prev) =>
          prev
            ? {
              ...prev,
              searchId: Number(data.search_id || 0),
              relatedEntities:
                (data.related_entities as Array<Record<string, unknown>>) ||
                null,
              connectedEntities:
                (data.connected_entities as Array<Record<string, unknown>>) ||
                null,
              citations:
                (data.citations as Array<Record<string, unknown>>) || null,
              enrichment:
                (data.enrichment as Record<string, unknown>) || null,
              evidenceCount: Number(
                data.evidence_count || prev.evidenceCount || 0,
              ),
              semanticMatches: Number(
                data.semantic_matches || prev.semanticMatches || 0,
              ),
              graphRelated: Number(
                data.graph_related_sources || prev.graphRelated || 0,
              ),
            }
            : prev,
        )
        setComplete(true)
        break
      case 'error':
        setError(evt.message || 'Pipeline error')
        break
    }
  }

  // ── Search execution ───────────────────────────────────────────────────
  async function runSearch(event: FormEvent) {
    event.preventDefault()
    if (abortRef.current) abortRef.current.abort()
    const controller = new AbortController()
    abortRef.current = controller

    setError('')
    setLoading(true)
    setComplete(false)
    setBootMsg('CONNECTING TO DATA FEEDS...')
    startTimeRef.current = Date.now()
    setLatency(0)
    setStream(emptyStreamState())

    try {
      const response = await fetch(`${API_BASE}/search/stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, limit: 12 }),
        signal: controller.signal,
      })

      if (!response.ok) {
        const text = await response.text()
        throw new Error(`${response.status}: ${text}`)
      }

      const reader = response.body?.getReader()
      if (!reader) throw new Error('No stream body')
      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          try {
            processEvent(JSON.parse(line.slice(6)))
          } catch {
            /* skip malformed events */
          }
        }
      }

      // Process any remaining buffered data
      if (buffer.startsWith('data: ')) {
        try {
          processEvent(JSON.parse(buffer.slice(6)))
        } catch {
          /* skip */
        }
      }
    } catch (err) {
      if ((err as Error).name !== 'AbortError') {
        setError(err instanceof Error ? err.message : 'Stream failed.')
      }
    } finally {
      setLoading(false)
      setLatency(Date.now() - startTimeRef.current)
    }
  }

  // ── Render ─────────────────────────────────────────────────────────────
  const s = stream
  const hasData =
    s && (s.financialPerformance || s.decision || s.narrative || s.priceHistory)
  const isBooting = loading && !hasData

  return (
    <div className="terminal-shell">
      {/* ═══ COMMAND BAR ═══ */}
      <CommandBar
        query={query}
        loading={loading}
        clocks={clocks}
        onQueryChange={setQuery}
        onSubmit={runSearch}
      />

      {/* ═══ MAIN BODY ═══ */}
      <div className="terminal-body">
        {error ? <div className="error-bar">[ERR] {error}</div> : null}

        {/* AI Disclaimer — visible when AI data is present */}
        {hasData ? <AiDisclaimer /> : null}

        {/* Boot sequence while loading with no data */}
        {isBooting ? (
          <BootSequence stage={bootMsg || 'INITIALIZING...'} />
        ) : null}

        {/* Bento Grid — shows when data arrives */}
        {hasData ? (
          <div className="bento-grid">
            {/* Zone A: Asset Profile (top-left) */}
            <AssetProfile s={s} />

            {/* Zone B: Macro & Fundamentals (top-right) */}
            <MacroZone s={s} />

            {/* Zone C: Signal Dashboard (bottom-left) */}
            <SignalDash s={s} />

            {/* Zone D: Narrative (bottom-right) */}
            <NarrativePane s={s} />
          </div>
        ) : null}
      </div>

      {/* ═══ STATUS FOOTER ═══ */}
      <StatusFooter
        stream={s}
        complete={complete}
        loading={loading}
        latency={latency}
      />
    </div>
  )
}
