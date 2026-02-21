'use client'

import {
    ChangeEvent,
    FormEvent,
    useCallback,
    useEffect,
    useRef,
    useState,
} from 'react'
import type { AutocompleteSuggestion } from '../lib/types'
import type { Clocks } from '../hooks/useClocks'
import { API_BASE } from '../constants'

// ---------------------------------------------------------------------------
// CommandBar — search input, autocomplete dropdown, execute button, clocks
// ---------------------------------------------------------------------------

type Props = {
    query: string
    loading: boolean
    clocks: Clocks
    onQueryChange: (value: string) => void
    onSubmit: (e: FormEvent) => void
}

export function CommandBar({ query, loading, clocks, onQueryChange, onSubmit }: Props) {
    const [suggestions, setSuggestions] = useState<AutocompleteSuggestion[]>([])
    const [selectedIdx, setSelectedIdx] = useState(-1)
    const [showDropdown, setShowDropdown] = useState(false)
    const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    const inputRef = useRef<HTMLInputElement>(null)

    const fetchSuggestions = useCallback(async (q: string) => {
        if (q.length < 1) {
            setSuggestions([])
            setShowDropdown(false)
            return
        }
        try {
            const res = await fetch(
                `${API_BASE}/search/autocomplete?q=${encodeURIComponent(q)}`,
            )
            if (res.ok) {
                const data: AutocompleteSuggestion[] = await res.json()
                setSuggestions(data)
                setShowDropdown(data.length > 0)
                setSelectedIdx(-1)
            }
        } catch {
            /* silent */
        }
    }, [])

    function onInput(event: ChangeEvent<HTMLInputElement>) {
        const value = event.target.value
        onQueryChange(value)
        if (debounceRef.current) clearTimeout(debounceRef.current)
        debounceRef.current = setTimeout(() => fetchSuggestions(value), 300)
    }

    function selectSuggestion(s: AutocompleteSuggestion) {
        onQueryChange(`${s.name} (${s.ticker})`)
        setSuggestions([])
        setShowDropdown(false)
        inputRef.current?.focus()
    }

    function onKeyDown(e: React.KeyboardEvent) {
        if (!showDropdown || suggestions.length === 0) {
            if (e.key === 'Enter') return // let form submit
            return
        }
        if (e.key === 'ArrowDown') {
            e.preventDefault()
            setSelectedIdx((prev) => (prev + 1) % suggestions.length)
        } else if (e.key === 'ArrowUp') {
            e.preventDefault()
            setSelectedIdx(
                (prev) => (prev - 1 + suggestions.length) % suggestions.length,
            )
        } else if (e.key === 'Enter' && selectedIdx >= 0) {
            e.preventDefault()
            selectSuggestion(suggestions[selectedIdx])
        } else if (e.key === 'Escape') {
            setShowDropdown(false)
        }
    }

    // Close dropdown on outside click
    useEffect(() => {
        function handleClick(e: MouseEvent) {
            if (
                inputRef.current &&
                !inputRef.current.parentElement?.contains(e.target as Node)
            ) {
                setShowDropdown(false)
            }
        }
        document.addEventListener('mousedown', handleClick)
        return () => document.removeEventListener('mousedown', handleClick)
    }, [])

    return (
        <form className="command-bar" onSubmit={onSubmit}>
            <span className="command-prefix">&gt; QUERY:</span>
            <div className="command-input-wrap">
                <input
                    ref={inputRef}
                    className="command-input"
                    value={query}
                    onChange={onInput}
                    onKeyDown={onKeyDown}
                    onFocus={() => suggestions.length > 0 && setShowDropdown(true)}
                    placeholder="MSFT, Apple, Nvidia..."
                    aria-label="Search company or ticker"
                    autoComplete="off"
                />
                <span className="block-cursor">█</span>
                {showDropdown && suggestions.length > 0 ? (
                    <div className="ac-dropdown">
                        {suggestions.map((sg, i) => (
                            <button
                                key={`${sg.ticker}-${i}`}
                                type="button"
                                className={`ac-item ${i === selectedIdx ? 'sel' : ''}`}
                                onMouseDown={() => selectSuggestion(sg)}
                                onMouseEnter={() => setSelectedIdx(i)}
                            >
                                <span className="ac-tk">{sg.ticker}</span>
                                <span className="ac-nm">{sg.name}</span>
                                <span className="ac-ex">{sg.exchange}</span>
                            </button>
                        ))}
                    </div>
                ) : null}
            </div>
            <button
                type="submit"
                className="cmd-btn"
                disabled={loading || query.trim().length < 2}
            >
                {loading ? 'EXEC...' : 'EXECUTE'}
            </button>
            <div className="clock-strip">
                <div className="clock-item">
                    UTC <span>{clocks.utc}</span>
                </div>
                <div className="clock-item">
                    EST <span>{clocks.est}</span>
                </div>
                <div className="clock-item">
                    SGT <span>{clocks.sgt}</span>
                </div>
                <span className="conn-dot" />
            </div>
        </form>
    )
}
