import hashlib
from bs4 import BeautifulSoup


def hash_content(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def build_excerpt(content: str, max_length: int = 500) -> str:
    normalized = " ".join(content.split())
    return normalized[:max_length]


def normalize_content(raw_content: str, source_url: str) -> str:
    if "<html" in raw_content.lower() or "</" in raw_content:
        soup = BeautifulSoup(raw_content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        return " ".join(text.split())
    return " ".join(raw_content.split())


def chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> list[str]:
    if not text:
        return []

    safe_chunk_size = max(chunk_size, 100)
    safe_overlap = min(max(overlap, 0), safe_chunk_size - 1)

    chunks: list[str] = []
    start = 0
    length = len(text)

    while start < length:
        end = min(start + safe_chunk_size, length)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end == length:
            break
        start = end - safe_overlap

    return chunks
