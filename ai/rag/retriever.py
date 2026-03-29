"""
RAG retriever — loads the persisted FAISS index and answers similarity queries.

This is the read-side counterpart to indexer.py.  The indexer builds the
FAISS index from policy documents; the retriever loads it and finds the
most relevant chunks for a given query.

Re-ranking:
  We do a simple two-stage retrieval:
    1. FAISS similarity search to get top-k candidates (k=20 by default)
    2. Score-threshold filtering to drop low-relevance noise
    3. Sort by score descending

  We considered adding a cross-encoder re-ranker (like ms-marco-MiniLM)
  but the latency hit wasn't worth it for our use case — the FAISS scores
  from text-embedding-3-small are already pretty good for policy docs,
  and users expect sub-second responses from the chatbot.

  TODO: revisit cross-encoder if retrieval quality degrades with more docs.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import structlog
from openai import OpenAI

logger = structlog.get_logger(__name__)

INDEX_DIR: Path = Path(__file__).resolve().parent / "index"

# score threshold for filtering — tuned on a handful of test queries.
# text-embedding-3-small cosine scores tend to be in the 0.3-0.8 range
# for relevant policy docs.  0.35 drops obvious garbage without being
# so aggressive that we miss partial matches.
DEFAULT_SCORE_THRESHOLD: float = 0.35
DEFAULT_TOP_K: int = 5
CANDIDATE_MULTIPLIER: int = 4  # fetch 4x top_k from FAISS, then filter


@dataclass
class RetrievalResult:
    """A single retrieved chunk with metadata and similarity score."""

    text: str
    score: float
    source: str = ""
    chunk_index: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class PolicyRetriever:
    """Retrieves relevant policy document chunks for a given query.

    Usage::

        retriever = PolicyRetriever(api_key="sk-...")
        results = retriever.search("What are Title I funding requirements?")
        for r in results:
            print(f"[{r.score:.3f}] {r.source}: {r.text[:100]}...")
    """

    def __init__(
        self,
        api_key: str | None = None,
        index_dir: Path = INDEX_DIR,
        embedding_model: str = "text-embedding-3-small",
        score_threshold: float = DEFAULT_SCORE_THRESHOLD,
        top_k: int = DEFAULT_TOP_K,
    ) -> None:
        self._index_dir: Path = index_dir
        self._embedding_model: str = embedding_model
        self._score_threshold: float = score_threshold
        self._top_k: int = top_k

        resolved_key: str = api_key or os.environ.get("OPENAI_API_KEY", "")
        if not resolved_key:
            raise ValueError("No OpenAI API key — set OPENAI_API_KEY or pass api_key")

        self._client: OpenAI = OpenAI(api_key=resolved_key, timeout=30.0)

        # lazy-loaded on first search
        self._index: Any | None = None
        self._chunks: list[str] = []
        self._metadata: list[dict[str, Any]] = []
        self._use_faiss: bool = True

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        top_k: int | None = None,
        score_threshold: float | None = None,
    ) -> list[RetrievalResult]:
        """Find the most relevant document chunks for a query.

        Returns up to top_k results, filtered by score_threshold.
        Results are sorted by relevance (highest score first).
        """
        k: int = top_k or self._top_k
        threshold: float = score_threshold or self._score_threshold

        self._load_index()

        if not self._chunks:
            logger.warning("empty_index")
            return []

        query_embedding: np.ndarray = self._embed_query(query)

        # fetch more candidates than we need, then filter
        n_candidates: int = min(k * CANDIDATE_MULTIPLIER, len(self._chunks))

        if self._use_faiss and self._index is not None:
            scores, indices = self._index.search(
                query_embedding.reshape(1, -1), n_candidates
            )
            scores = scores[0]
            indices = indices[0]
        else:
            # numpy fallback — brute force cosine similarity
            scores, indices = self._numpy_search(query_embedding, n_candidates)

        results: list[RetrievalResult] = []
        for score, idx in zip(scores, indices):
            if idx < 0 or idx >= len(self._chunks):
                continue
            if float(score) < threshold:
                continue

            meta: dict[str, Any] = (
                self._metadata[idx] if idx < len(self._metadata) else {}
            )

            results.append(RetrievalResult(
                text=self._chunks[idx],
                score=float(score),
                source=meta.get("source", "unknown"),
                chunk_index=meta.get("chunk_index", 0),
                metadata=meta,
            ))

        results = self._rerank(results)
        return results[:k]

    # ------------------------------------------------------------------
    # index loading
    # ------------------------------------------------------------------

    def _load_index(self) -> None:
        """Load the FAISS index and associated metadata from disk.

        Tries FAISS first, falls back to numpy if faiss-cpu isn't installed.
        This runs once and caches everything in memory.
        """
        if self._chunks:
            return

        chunks_path: Path = self._index_dir / "chunks.json"
        metadata_path: Path = self._index_dir / "metadata.json"
        faiss_path: Path = self._index_dir / "wellnest_docs.index"
        numpy_path: Path = self._index_dir / "embeddings.npy"

        if not chunks_path.exists():
            logger.error("index_not_found", dir=str(self._index_dir))
            return

        with open(chunks_path) as f:
            self._chunks = json.load(f)

        if metadata_path.exists():
            with open(metadata_path) as f:
                self._metadata = json.load(f)

        # try FAISS index first
        if faiss_path.exists():
            try:
                import faiss

                self._index = faiss.read_index(str(faiss_path))
                self._use_faiss = True
                logger.info(
                    "faiss_index_loaded",
                    vectors=self._index.ntotal,
                    chunks=len(self._chunks),
                )
                return
            except ImportError:
                logger.warning("faiss_not_installed, using numpy fallback")

        # numpy fallback
        if numpy_path.exists():
            self._index = np.load(str(numpy_path))
            self._use_faiss = False
            logger.info("numpy_index_loaded", shape=self._index.shape)
        else:
            logger.error("no_index_file_found")

    # ------------------------------------------------------------------
    # embedding + search helpers
    # ------------------------------------------------------------------

    def _embed_query(self, query: str) -> np.ndarray:
        """Embed a single query string."""
        response = self._client.embeddings.create(
            model=self._embedding_model,
            input=[query],
        )
        vec: np.ndarray = np.array(response.data[0].embedding, dtype=np.float32)

        # normalize for cosine similarity (FAISS IndexFlatIP expects normalized vectors)
        norm: float = float(np.linalg.norm(vec))
        if norm > 0:
            vec = vec / norm

        return vec

    def _numpy_search(
        self,
        query_embedding: np.ndarray,
        n_candidates: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Brute-force cosine similarity when FAISS isn't available.

        Slower than FAISS but works everywhere.  Fine for <10K chunks.
        """
        embeddings: np.ndarray = self._index  # numpy array

        # normalize the stored embeddings (should already be done but just in case)
        norms: np.ndarray = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        normalized: np.ndarray = embeddings / norms

        similarities: np.ndarray = normalized @ query_embedding

        top_indices: np.ndarray = np.argsort(similarities)[::-1][:n_candidates]
        top_scores: np.ndarray = similarities[top_indices]

        return top_scores, top_indices

    # ------------------------------------------------------------------
    # re-ranking
    # ------------------------------------------------------------------

    def _rerank(self, results: list[RetrievalResult]) -> list[RetrievalResult]:
        """Re-rank results by score with source diversity bonus.

        Simple heuristic: if we have results from multiple source documents,
        lightly boost results from underrepresented sources.  This prevents
        the top-k from being dominated by one long document.

        The boost is small (5%) so it only breaks ties — a genuinely more
        relevant chunk from the same document will still rank higher.
        """
        if len(results) <= 1:
            return results

        source_counts: dict[str, int] = {}
        for r in results:
            source_counts[r.source] = source_counts.get(r.source, 0) + 1

        max_count: int = max(source_counts.values())

        for r in results:
            count: int = source_counts.get(r.source, 1)
            if count < max_count:
                # slight boost for underrepresented sources
                r.score *= 1.05

        results.sort(key=lambda r: r.score, reverse=True)
        return results
