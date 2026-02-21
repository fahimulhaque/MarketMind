import re

EMAIL_PATTERN = re.compile(r"(?P<email>[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)")
PHONE_PATTERN = re.compile(r"(?P<phone>\+?\d{1,4}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9})")
SSN_PATTERN = re.compile(r"(?P<ssn>\b\d{3}[- ]?\d{2}[- ]?\d{4}\b)")
CREDIT_CARD_PATTERN = re.compile(r"(?P<cc>\b(?:\d[ -]*?){13,16}\b)")


def redact_pii(text: str) -> str:
    """Redact common PII using robust regular expressions."""
    if not text:
        return text
    redacted = EMAIL_PATTERN.sub("[REDACTED_EMAIL]", text)
    redacted = PHONE_PATTERN.sub("[REDACTED_PHONE]", redacted)
    redacted = SSN_PATTERN.sub("[REDACTED_SSN]", redacted)
    redacted = CREDIT_CARD_PATTERN.sub("[REDACTED_CC]", redacted)
    return redacted

