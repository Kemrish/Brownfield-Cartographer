"""Shared embedding API (OpenAI/OpenRouter) for embedding-based domain clustering and semantic search."""

import os
from typing import Optional

import httpx

OPENROUTER_BASE = "https://openrouter.ai/api/v1"
OPENAI_BASE = "https://api.openai.com/v1"
EMBEDDING_MODEL = "text-embedding-3-small"


def get_embedding(
    text: str,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: str = EMBEDDING_MODEL,
) -> Optional[list[float]]:
    """Return embedding vector for text, or None on failure. Uses OpenAI-compatible /embeddings endpoint."""
    key = api_key or os.environ.get("OPENROUTER_API_KEY", "").strip() or os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        return None
    url_base = base_url or (OPENROUTER_BASE if os.environ.get("OPENROUTER_API_KEY") else OPENAI_BASE)
    url = url_base.rstrip("/") + "/embeddings"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {"model": model, "input": text[:8000]}
    try:
        with httpx.Client(timeout=20.0) as client:
            r = client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
            emb = data.get("data", [{}])[0].get("embedding")
            return emb if isinstance(emb, list) else None
    except Exception:
        return None


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)
