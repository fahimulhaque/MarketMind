from __future__ import annotations

import logging
from hashlib import sha256
import httpx

from core.config import get_settings
from core.db.connection import get_connection

logger = logging.getLogger(__name__)

def _fallback_vector(text: str, size: int) -> list[float]:
    digest = sha256(text.encode("utf-8")).digest()
    values: list[float] = []
    for index in range(size):
        byte_val = digest[index % len(digest)]
        values.append((byte_val / 255.0) * 2 - 1)
    return values

def _embed_with_ollama(text: str) -> list[float] | None:
    settings = get_settings()
    try:
        response = httpx.post(
            f"{settings.ollama_host}/api/embeddings",
            json={"model": settings.ollama_embed_model, "prompt": text},
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        embedding = data.get("embedding")
        if isinstance(embedding, list) and embedding:
            return [float(item) for item in embedding]
    except Exception:
        return None
    return None

def _embed_batch_with_ollama(texts: list[str]) -> list[list[float] | None]:
    if not texts:
        return []
    settings = get_settings()
    try:
        response = httpx.post(
            f"{settings.ollama_host}/api/embed",
            json={"model": settings.ollama_embed_model, "input": texts},
            timeout=60.0,
        )
        response.raise_for_status()
        data = response.json()
        embeddings = data.get("embeddings", [])
        if isinstance(embeddings, list) and len(embeddings) == len(texts):
            return [[float(v) for v in emb] if emb else None for emb in embeddings]
    except Exception:
        logger.debug("Batch embed endpoint unavailable — falling back to sequential")
    results: list[list[float] | None] = []
    for text in texts:
        results.append(_embed_with_ollama(text))
    return results

def _vector_for_text(text: str) -> list[float]:
    settings = get_settings()
    return _vector_for_text_with_size(text, settings.embedding_vector_size)

def _vector_for_text_with_size(text: str, target_size: int) -> list[float]:
    settings = get_settings()
    embedding = _embed_with_ollama(text)
    if embedding:
        if len(embedding) >= target_size:
            return embedding[:target_size]
        return embedding + [0.0] * (target_size - len(embedding))
    return _fallback_vector(text, target_size)

def _vectors_for_texts_with_size(texts: list[str], target_size: int) -> list[list[float]]:
    embeddings = _embed_batch_with_ollama(texts)
    results: list[list[float]] = []
    for i, emb in enumerate(embeddings):
        if emb:
            if len(emb) >= target_size:
                results.append(emb[:target_size])
            else:
                results.append(emb + [0.0] * (target_size - len(emb)))
        else:
            results.append(_fallback_vector(texts[i], target_size))
    return results

def _format_pgvector(vec: list[float]) -> str:
    return "[" + ",".join(str(v) for v in vec) + "]"

def upsert_document_memory(
    source_id: int,
    source_name: str,
    source_url: str,
    content_hash: str,
    chunks: list[str],
    evidence_ref: str,
) -> None:
    settings = get_settings()
    selected_chunks = chunks[:10] if chunks else [content_hash]
    vectors = _vectors_for_texts_with_size(selected_chunks, settings.embedding_vector_size)

    with get_connection() as conn:
        with conn.cursor() as cursor:
            for index, chunk in enumerate(selected_chunks):
                vec_str = _format_pgvector(vectors[index])
                cursor.execute(
                    """
                    INSERT INTO memory_chunks 
                    (source_id, source_name, source_url, content_hash, chunk_index, chunk_text, evidence_ref, embedding)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_id, content_hash, chunk_index) DO UPDATE
                    SET embedding = EXCLUDED.embedding,
                        chunk_text = EXCLUDED.chunk_text,
                        evidence_ref = EXCLUDED.evidence_ref;
                    """,
                    (source_id, source_name, source_url, content_hash, index, chunk, evidence_ref, vec_str)
                )
        conn.commit()

def upsert_graph_relationship(
    source_id: int,
    source_name: str,
    source_url: str,
    threat_level: str,
    evidence_ref: str,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO source_evidence_relations (source_id, evidence_ref, threat_level)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_id, evidence_ref) DO UPDATE
                SET threat_level = EXCLUDED.threat_level;
                """,
                (source_id, evidence_ref, threat_level)
            )
        conn.commit()


def delete_source_memory(source_id: int) -> dict:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM memory_chunks WHERE source_id = %s", (source_id,))
            qdrant_deleted = cursor.rowcount
            cursor.execute("DELETE FROM source_evidence_relations WHERE source_id = %s", (source_id,))
            neo4j_deleted = cursor.rowcount
        conn.commit()
    return {
        "qdrant_deleted": qdrant_deleted, # Kept same keys for compatibility
        "neo4j_deleted": neo4j_deleted,
    }


def semantic_search(query_text: str, limit: int = 20) -> list[dict]:
    settings = get_settings()
    query_vector = _vector_for_text_with_size(query_text, settings.embedding_vector_size)
    vec_str = _format_pgvector(query_vector)
    
    with get_connection() as conn:
        with conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT source_id, source_name, source_url, chunk_text as chunk, evidence_ref,
                       1 - (embedding <=> %s::vector) AS similarity_score
                FROM memory_chunks
                ORDER BY embedding <=> %s::vector
                LIMIT %s;
                """,
                (vec_str, vec_str, limit)
            )
            items = cursor.fetchall()
            return [dict(i) for i in items]


def graph_search_related_sources(entity_name: str, limit: int = 10) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT s.name AS source_name, s.url AS source_url, 
                       r.threat_level AS threat_level, r.evidence_ref
                FROM source_evidence_relations r
                JOIN sources s ON r.source_id = s.id
                WHERE s.name ILIKE %s
                ORDER BY 
                   CASE r.threat_level WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END DESC
                LIMIT %s;
                """,
                (f"%{entity_name}%", limit)
            )
            items = cursor.fetchall()
            return [dict(i) for i in items]

def graph_find_connected_entities(entity_name: str, limit: int = 10) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=__import__('psycopg2').extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT s2.name AS related_source, s2.url AS url, count(r2.evidence_ref) AS shared_evidence_count
                FROM source_evidence_relations r1
                JOIN sources s1 ON r1.source_id = s1.id
                JOIN source_evidence_relations r2 ON r1.evidence_ref = r2.evidence_ref
                JOIN sources s2 ON r2.source_id = s2.id
                WHERE s1.name ILIKE %s AND s1.id != s2.id
                GROUP BY s2.id, s2.name, s2.url
                ORDER BY shared_evidence_count DESC
                LIMIT %s;
                """,
                (f"%{entity_name}%", limit)
            )
            items = cursor.fetchall()
            return [dict(i) for i in items]
