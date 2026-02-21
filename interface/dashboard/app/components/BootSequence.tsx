'use client'

// ---------------------------------------------------------------------------
// BootSequence — animated startup lines shown while connecting to data feeds
// ---------------------------------------------------------------------------

export function BootSequence({ stage }: { stage: string }) {
    const lines = [
        { text: 'MARKETMIND TERMINAL v2.0', cls: 'hl' },
        { text: '> Initializing data providers...', cls: '' },
        { text: `> ${stage}`, cls: '' },
        { text: '> Connecting to market feeds...', cls: 'dim' },
    ]

    return (
        <div className="boot-seq">
            {lines.map((l, i) => (
                <div
                    key={i}
                    className={`boot-line ${l.cls}`}
                    style={{ animationDelay: `${i * 0.15}s` }}
                >
                    {l.text}
                </div>
            ))}
            <span className="boot-cursor">█</span>
        </div>
    )
}
