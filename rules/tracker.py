"""agents.tracker — Content change detection."""

from core.processing import build_excerpt, hash_content


def evaluate_change(current_content: str, previous_hash: str | None) -> dict:
    current_hash = hash_content(current_content)
    has_changed = previous_hash is None or previous_hash != current_hash

    if previous_hash is None:
        insight = "Initial baseline snapshot created for competitor source."
        threat_level = "low"
        recommendation = "Continue monitoring for subsequent deltas."
    elif has_changed:
        insight = "Competitor source content changed since last observation."
        threat_level = "medium"
        recommendation = "Review delta and validate business impact."
    else:
        insight = "No content delta detected in latest observation window."
        threat_level = "low"
        recommendation = "No immediate action required."

    return {
        "content_hash": current_hash,
        "has_changed": has_changed,
        "insight": insight,
        "threat_level": threat_level,
        "recommendation": recommendation,
        "excerpt": build_excerpt(current_content),
    }
