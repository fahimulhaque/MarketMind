'use client'

import type { StreamState } from '../lib/types'

// ---------------------------------------------------------------------------
// StatusFooter — bottom bar with data integrity, latency, source count
// ---------------------------------------------------------------------------

type Props = {
    stream: StreamState | null
    complete: boolean
    loading: boolean
    latency: number
}

export function StatusFooter({ stream: s, complete, loading, latency }: Props) {
    return (
        <div className="status-footer">
            <div className="status-left">
                <span className="status-item">
                    DATA INTEGRITY:{' '}
                    <span>{complete ? '100%' : loading ? 'STREAMING' : 'IDLE'}</span>
                </span>
                <span className="status-item">
                    LATENCY:{' '}
                    <span>{latency > 0 ? `${(latency / 1000).toFixed(1)}s` : '—'}</span>
                </span>
            </div>
            <div className="status-right">
                {s?.evidenceCount ? (
                    <span className="status-item">
                        SOURCES: <span>{s.evidenceCount}</span>
                    </span>
                ) : null}
                {s?.searchId ? (
                    <span className="status-item">
                        SEARCH: <span>#{s.searchId}</span>
                    </span>
                ) : null}
                <span className="status-item">TICKERAGENT</span>
            </div>
        </div>
    )
}
