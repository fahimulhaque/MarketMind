import React from 'react';
import '../styles/analyst-consensus.css';

export function AnalystConsensus({ data, isStreaming = false, currencySymbol = '$' }: { data: any, isStreaming?: boolean, currencySymbol?: string }) {
    if (!data || !data.analyst_count) return null;

    const total = data.analyst_count;
    const buyPct = Math.round((data.buy / total) * 100) || 0;
    const holdPct = Math.round((data.hold / total) * 100) || 0;
    const sellPct = Math.round((data.sell / total) * 100) || 0;

    return (
        <div className={`terminal-section analyst-consensus ${isStreaming ? 'streaming' : ''}`}>
            <div className="section-header">ANALYST CONSENSUS [{total} RATINGS]</div>
            <div className="consensus-bar">
                {buyPct > 0 && <div className="bar-segment buy" style={{ width: `${buyPct}%` }}>{buyPct}% BUY</div>}
                {holdPct > 0 && <div className="bar-segment hold" style={{ width: `${holdPct}%` }}>{holdPct}% HLD</div>}
                {sellPct > 0 && <div className="bar-segment sell" style={{ width: `${sellPct}%` }}>{sellPct}% SEL</div>}
            </div>
            <div className="target-prices">
                <div>LOW: {currencySymbol}{data.target_low?.toFixed(2) || 'N/A'}</div>
                <div>AVG: {currencySymbol}{data.target_mean?.toFixed(2) || 'N/A'}</div>
                <div>HIGH: {currencySymbol}{data.target_high?.toFixed(2) || 'N/A'}</div>
            </div>
        </div>
    );
}
