"""agents.critic — Evidence quality review."""


def review_analysis(analysis: dict) -> dict:
    confidence = float(analysis.get("confidence", 0.0))
    evidence_ref = analysis.get("evidence_ref", "")
    evidence_excerpt = analysis.get("evidence_excerpt", "")

    has_evidence = bool(evidence_ref) and bool(evidence_excerpt)
    critic_status = "approved"

    if confidence < 0.55 or not has_evidence:
        critic_status = "flagged"
    elif analysis.get("threat_level") == "high" and confidence < 0.75:
        critic_status = "flagged"

    return {
        "critic_status": critic_status,
        "confidence": confidence,
        "has_evidence": has_evidence,
    }
