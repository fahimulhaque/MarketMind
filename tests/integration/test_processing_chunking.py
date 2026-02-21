from core.processing import chunk_text, normalize_content


def test_html_normalization_and_chunking() -> None:
    html = "<html><body><h1>Hello</h1><p>World</p></body></html>"
    normalized = normalize_content(html, "http://example.com")
    assert "Hello" in normalized and "World" in normalized

    chunks = chunk_text(normalized * 100, chunk_size=120, overlap=20)
    assert len(chunks) > 1
    assert all(chunks)
