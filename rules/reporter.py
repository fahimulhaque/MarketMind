"""agents.reporter — Markdown report generation."""

from datetime import datetime, timezone


def build_markdown_report(source_name: str, source_url: str, insights: list[dict]) -> tuple[str, str]:
    title = f"MarketMind Report - {source_name} - {datetime.now(timezone.utc).date().isoformat()}"

    header = [
        f"# {title}",
        "",
        f"Source: {source_name}",
        f"URL: {source_url}",
        "",
        "## Findings",
    ]

    body: list[str] = []
    if not insights:
        body.extend(["- No insights available for this period."])
    else:
        for index, insight in enumerate(insights, start=1):
            body.extend(
                [
                    f"### {index}. {insight['insight']}",
                    f"- Threat Level: {insight['threat_level']}",
                    f"- Recommendation: {insight['recommendation']}",
                    f"- Confidence: {insight.get('confidence', 0.0)}",
                    f"- Critic Status: {insight.get('critic_status', 'unknown')}",
                    f"- Evidence: {insight['evidence_ref']}",
                    "",
                ]
            )

    markdown = "\n".join(header + [""] + body).strip() + "\n"
    return title, markdown
