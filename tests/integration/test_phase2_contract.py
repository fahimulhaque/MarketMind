from rules.analyst import build_analysis
from rules.critic import review_analysis


def test_analysis_and_critic_contract() -> None:
    analysis = build_analysis(
        source_name="Example",
        source_url="https://example.com",
        has_changed=True,
        excerpt="some evidence",
    )

    assert set(["insight", "threat_level", "recommendation", "confidence", "evidence_ref", "evidence_excerpt"]).issubset(analysis)

    review = review_analysis(analysis)
    assert set(["critic_status", "confidence", "has_evidence"]).issubset(review)
    assert review["critic_status"] in {"approved", "flagged"}
