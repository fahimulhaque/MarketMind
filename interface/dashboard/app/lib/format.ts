// ---------------------------------------------------------------------------
// TickerAgent Dashboard — Number / Display Formatting Helpers
// ---------------------------------------------------------------------------

/**
 * Smart-format a numeric value — handles trillions, billions, millions,
 * and small decimals that should render as percentages.
 */
export function fmt(value: unknown): string {
    if (value == null) return '—'
    const n = Number(value)
    if (isNaN(n)) return String(value)
    const abs = Math.abs(n)
    if (abs >= 1e12) return `${(n / 1e12).toFixed(2)}T`
    if (abs >= 1e9) return `${(n / 1e9).toFixed(2)}B`
    if (abs >= 1e6) return `${(n / 1e6).toFixed(2)}M`
    if (abs < 1 && abs > 0) return `${(n * 100).toFixed(1)}%`
    return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

/**
 * Get the currency symbol for a given currency code. Defaults to '$' for 'USD'.
 */
export function getCurrencySymbol(currency: string = 'USD'): string {
    try {
        return (0).toLocaleString(undefined, {
            style: 'currency',
            currency,
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
        }).replace(/\d/g, '').trim()
    } catch {
        return '$' // Fallback for invalid currency codes
    }
}

/** Format a value explicitly as a percentage. */
export function pct(value: unknown): string {
    if (value == null) return '—'
    const n = Number(value)
    if (isNaN(n)) return String(value)
    if (Math.abs(n) < 1) return `${(n * 100).toFixed(1)}%`
    return `${n.toFixed(1)}%`
}

/** Returns a CSS class name based on the numeric sign: 'pos', 'neg', or ''. */
export function signClass(value: unknown): string {
    if (value == null) return ''
    const n = Number(value)
    if (isNaN(n)) return ''
    return n > 0 ? 'pos' : n < 0 ? 'neg' : ''
}

/** Build ASCII gauge strings — `on` chars and `off` chars. */
export function gaugeAscii(
    value: number,
    total: number = 10,
): { on: string; off: string } {
    const filled = Math.round((value / 100) * total)
    return {
        on: '|'.repeat(Math.max(0, filled)),
        off: '-'.repeat(Math.max(0, total - filled)),
    }
}
