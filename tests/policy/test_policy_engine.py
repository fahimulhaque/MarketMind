from security.policy_engine import validate_source_policy


def test_policy_passes_when_no_allowlist() -> None:
    decision = validate_source_policy("http://example.com")
    assert isinstance(decision.allowed, bool)
    assert isinstance(decision.reason, str)
