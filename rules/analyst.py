"""agents.analyst — Market change analysis."""


def build_analysis(
    source_name: str,
    source_url: str,
    has_changed: bool,
    excerpt: str,
) -> dict:
    if has_changed:
        threat_level = "medium"
        confidence = 0.72
        insight = f"Change detected for {source_name}; extracted update requires review."
        recommendation = "Compare latest change against prior messaging and assess strategic impact."
    else:
        threat_level = "low"
        confidence = 0.61
        insight = f"No meaningful change detected for {source_name} in this cycle."
        recommendation = "Continue scheduled monitoring and aggregate with trend signals."

    return {
        "insight": insight,
        "threat_level": threat_level,
        "recommendation": recommendation,
        "confidence": confidence,
        "evidence_ref": source_url,
        "evidence_excerpt": excerpt,
    }
