'use client'

import type { StreamState } from '../lib/types'
import { fmt, pct, signClass, getCurrencySymbol } from '../lib/format'

// ---------------------------------------------------------------------------
// AssetProfile — ticker, price, key financial metrics, 52-week range
// ---------------------------------------------------------------------------

export function AssetProfile({ s }: { s: StreamState }) {
    const fp = s.financialPerformance as Record<string, Record<string, unknown>> | null
    const ph = s.priceHistory
    if (!fp && !ph) return null

    const price = ph?.current ?? (fp as Record<string, unknown>)?.price
    const symbol = (s.queryContext as Record<string, unknown>)?.ticker
    const change = ph?.one_month_return
    const currencyCode = String((fp as Record<string, unknown>)?.currency || 'USD')
    const currencySymbol = getCurrencySymbol(currencyCode)
    const val = fp?.valuation || {}
    const grow = fp?.growth || {}
    const prof = fp?.profitability || {}
    const liq = fp?.liquidity || {}

    return (
        <div className="zone fade-in">
            <div className="zone-header">ASSET PROFILE</div>
            {symbol ? <div className="asset-ticker">{String(symbol)}</div> : null}

            <div className="asset-price-row">
                {price != null ? (
                    <>
                        <span className="asset-price">
                            {currencySymbol}{Number(price).toLocaleString(undefined, { maximumFractionDigits: 2 })}
                        </span>
                        <span className="asset-currency">
                            {currencyCode}
                        </span>
                    </>
                ) : (
                    <span className="asset-price">—</span>
                )}
                {change != null ? (
                    <span className={`asset-change ${Number(change) >= 0 ? 'up' : 'down'}`}>
                        {Number(change) > 0 ? '+' : ''}
                        {Number(change).toFixed(2)}%
                    </span>
                ) : null}
            </div>

            <div className="asset-metrics">
                <div className="a-metric">
                    <div className="a-metric-label">MKT CAP</div>
                    <div className="a-metric-val">{fmt((fp as Record<string, unknown>)?.market_cap)}</div>
                </div>
                <div className="a-metric">
                    <div className="a-metric-label">P/E TTM</div>
                    <div className="a-metric-val">{fmt(val.trailing_pe)}</div>
                </div>
                <div className="a-metric">
                    <div className="a-metric-label">BETA</div>
                    <div className="a-metric-val">{fmt((fp as Record<string, unknown>)?.beta)}</div>
                </div>
                <div className="a-metric">
                    <div className="a-metric-label">REV GROWTH</div>
                    <div className={`a-metric-val ${signClass(grow.revenue_growth ?? grow.revenue_growth_yoy)}`}>
                        {pct(grow.revenue_growth ?? grow.revenue_growth_yoy)}
                    </div>
                </div>
                <div className="a-metric">
                    <div className="a-metric-label">NET MARGIN</div>
                    <div className={`a-metric-val ${signClass(prof.net_margin ?? prof.profit_margins)}`}>
                        {pct(prof.net_margin ?? prof.profit_margins)}
                    </div>
                </div>
                <div className="a-metric">
                    <div className="a-metric-label">D/E</div>
                    <div className="a-metric-val">{fmt(liq.debt_to_equity)}</div>
                </div>
            </div>

            {/* Additional metrics */}
            <details>
                <summary>Full Metrics</summary>
                <div className="asset-metrics" style={{ marginTop: 4 }}>
                    <div className="a-metric">
                        <div className="a-metric-label">P/E FWD</div>
                        <div className="a-metric-val">{fmt(val.forward_pe)}</div>
                    </div>
                    <div className="a-metric">
                        <div className="a-metric-label">PEG</div>
                        <div className="a-metric-val">{fmt(val.peg_ratio)}</div>
                    </div>
                    <div className="a-metric">
                        <div className="a-metric-label">EARN GROWTH</div>
                        <div className={`a-metric-val ${signClass(grow.earnings_growth ?? grow.earnings_growth_yoy)}`}>
                            {pct(grow.earnings_growth ?? grow.earnings_growth_yoy)}
                        </div>
                    </div>
                    <div className="a-metric">
                        <div className="a-metric-label">GROSS MARGIN</div>
                        <div className={`a-metric-val ${signClass(prof.gross_margin ?? prof.gross_margins)}`}>
                            {pct(prof.gross_margin ?? prof.gross_margins)}
                        </div>
                    </div>
                    <div className="a-metric">
                        <div className="a-metric-label">OP MARGIN</div>
                        <div className={`a-metric-val ${signClass(prof.operating_margin ?? prof.operating_margins)}`}>
                            {pct(prof.operating_margin ?? prof.operating_margins)}
                        </div>
                    </div>
                    <div className="a-metric">
                        <div className="a-metric-label">CURR RATIO</div>
                        <div className="a-metric-val">{fmt(liq.current_ratio)}</div>
                    </div>
                </div>
            </details>

            {/* 52-Week Range */}
            {ph && ph.available && ph.fifty_two_week_low != null && ph.fifty_two_week_high != null ? (
                <div className="range-bar-wrap">
                    <div className="range-label">52-WEEK RANGE</div>
                    <div className="range-bar">
                        <span className="range-val">{currencySymbol}{ph.fifty_two_week_low}</span>
                        <div className="range-track">
                            <div className="range-marker" style={{ left: `${ph.range_position ?? 50}%` }} />
                        </div>
                        <span className="range-val">{currencySymbol}{ph.fifty_two_week_high}</span>
                    </div>
                </div>
            ) : null}
        </div>
    )
}
