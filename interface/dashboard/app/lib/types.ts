// ---------------------------------------------------------------------------
// MarketMind Dashboard — Shared TypeScript Types
// ---------------------------------------------------------------------------

/** Server-Sent Event payload pushed by the streaming search endpoint. */
export type StageEvent = {
    stage: string
    progress: number
    data?: Record<string, unknown>
    message?: string
}

/** A single autocomplete result from `/search/autocomplete`. */
export type AutocompleteSuggestion = {
    ticker: string
    name: string
    exchange: string
    type: string
}

/** Price history snapshot returned by the `price_history` stage. */
export type PriceHistory = {
    available: boolean
    current?: number
    fifty_two_week_high?: number
    fifty_two_week_low?: number
    ytd_return?: number
    one_month_return?: number
    three_month_return?: number
    range_position?: number
}

/** Accumulated state built up as SSE events arrive from the backend. */
export type StreamState = {
    queryContext: Record<string, unknown> | null
    financialPerformance: Record<string, unknown> | null
    historicalTrends: Record<string, unknown> | null
    trendAnalysis: string | null
    macroContext: Record<string, unknown> | null
    socialSentiment: Record<string, unknown> | null
    coverage: Record<string, unknown> | null
    filings: Record<string, unknown> | null
    decision: {
        executive_summary: string
        recommendation: string
        confidence: number
        risk_level: string
    } | null
    decisionStreaming: string
    narrative: string | null
    narrativeStreaming: string
    scenarios: Array<Record<string, unknown>> | null
    contradictions: Array<Record<string, unknown>> | null
    signalShifts: string[] | null
    competitiveLandscape: string | null
    competitiveStreaming: string
    priceHistory: PriceHistory | null
    relatedEntities: Array<Record<string, unknown>> | null
    connectedEntities: Array<Record<string, unknown>> | null
    citations: Array<Record<string, unknown>> | null
    enrichment: Record<string, unknown> | null
    searchId: number | null
    evidenceCount: number
    semanticMatches: number
    graphRelated: number
    analystConsensus: Record<string, unknown> | null
    analystStreaming: boolean
    insiderActivity: Record<string, unknown> | null
    insiderStreaming: boolean
    marketNews: Record<string, unknown> | null
    newsStreaming: boolean
}

/** Factory for creating a blank StreamState. */
export function emptyStreamState(): StreamState {
    return {
        queryContext: null,
        financialPerformance: null,
        historicalTrends: null,
        trendAnalysis: null,
        macroContext: null,
        socialSentiment: null,
        coverage: null,
        filings: null,
        decision: null,
        decisionStreaming: '',
        narrative: null,
        narrativeStreaming: '',
        scenarios: null,
        contradictions: null,
        signalShifts: null,
        competitiveLandscape: null,
        competitiveStreaming: '',
        priceHistory: null,
        relatedEntities: null,
        connectedEntities: null,
        citations: null,
        enrichment: null,
        searchId: null,
        evidenceCount: 0,
        semanticMatches: 0,
        graphRelated: 0,
        analystConsensus: null,
        analystStreaming: false,
        insiderActivity: null,
        insiderStreaming: false,
        marketNews: null,
        newsStreaming: false,
    }
}
