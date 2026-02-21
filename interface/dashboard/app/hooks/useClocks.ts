'use client'

import { useEffect, useState } from 'react'

// ---------------------------------------------------------------------------
// useClocks — live UTC / EST / SGT clock strings, updating every 10 s
// ---------------------------------------------------------------------------

export type Clocks = { utc: string; est: string; sgt: string }

export function useClocks(): Clocks {
    const [clocks, setClocks] = useState<Clocks>({ utc: '', est: '', sgt: '' })

    useEffect(() => {
        function tick() {
            const now = new Date()
            setClocks({
                utc: now.toLocaleTimeString('en-US', {
                    timeZone: 'UTC',
                    hour12: false,
                    hour: '2-digit',
                    minute: '2-digit',
                }),
                est: now.toLocaleTimeString('en-US', {
                    timeZone: 'America/New_York',
                    hour12: false,
                    hour: '2-digit',
                    minute: '2-digit',
                }),
                sgt: now.toLocaleTimeString('en-US', {
                    timeZone: 'Asia/Singapore',
                    hour12: false,
                    hour: '2-digit',
                    minute: '2-digit',
                }),
            })
        }
        tick()
        const id = setInterval(tick, 10_000)
        return () => clearInterval(id)
    }, [])

    return clocks
}
