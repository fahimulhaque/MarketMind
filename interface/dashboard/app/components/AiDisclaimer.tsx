'use client'

// ---------------------------------------------------------------------------
// AiDisclaimer — persistent warning banner about AI-generated content
// ---------------------------------------------------------------------------

export function AiDisclaimer() {
    return (
        <div className="ai-disclaimer" role="alert">
            <span className="ai-disclaimer-icon">⚠</span>
            <span className="ai-disclaimer-text">
                <strong>AI-Generated Analysis</strong> — Insights, narratives, and
                recommendations are produced by AI models and may contain inaccuracies.
                Always verify with official sources before making investment decisions.
            </span>
        </div>
    )
}
