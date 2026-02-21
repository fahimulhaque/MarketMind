'use client'

import { gaugeAscii } from '../lib/format'

// ---------------------------------------------------------------------------
// GaugeBar — ASCII + visual gauge indicator for signal metrics
// ---------------------------------------------------------------------------

export function GaugeBar({
    label,
    value,
    color,
}: {
    label: string
    value: number
    color: string
}) {
    const g = gaugeAscii(value)

    return (
        <div className="gauge-item">
            <div className="gauge-label">
                <span>{label}</span>
                <span className="gauge-value">{value}%</span>
            </div>
            <div className="gauge-ascii">
                [<span className="gauge-on">{g.on}</span>
                <span className="gauge-off">{g.off}</span>]
            </div>
            <div className="gauge-track">
                <div className={`gauge-fill ${color}`} style={{ width: `${value}%` }} />
            </div>
        </div>
    )
}
