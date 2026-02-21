from security.pii import redact_pii


def test_redact_pii_masks_email_and_phone() -> None:
    sample = "Contact jane.doe@example.com or +1 (555) 123-9876"
    redacted = redact_pii(sample)

    assert "jane.doe@example.com" not in redacted
    assert "+1 (555) 123-9876" not in redacted
    assert "[REDACTED_EMAIL]" in redacted
    assert "[REDACTED_PHONE]" in redacted
