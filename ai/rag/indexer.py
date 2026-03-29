"""
RAG document indexer — loads PDFs, chunks them, and builds a FAISS index.

The pipeline:
  1. Scan ai/documents/ for PDFs (ESSA guidelines, Title I docs, etc.)
  2. Extract text with PyPDF (via langchain's loader)
  3. Chunk with RecursiveCharacterTextSplitter (512 tokens, 50 overlap)
  4. Embed chunks with text-embedding-3-small
  5. Build a FAISS IndexFlatIP (inner product on normalized vectors)
  6. Persist index + metadata to ai/rag/index/

The index is small enough (~10-50 documents) that flat search is fine.
We considered HNSW for larger indexes but it's overkill for our doc count
and the build time difference is negligible at this scale.

Embedding costs:
  text-embedding-3-small is $0.02/1M tokens.  A typical 50-page PDF is
  ~25K tokens after chunking.  Even with 100 documents we're looking at
  ~2.5M tokens = $0.05.  Basically free.

TODO: add incremental indexing so we don't re-embed unchanged documents.
Right now we rebuild the whole index every time, which is fine for <100
docs but will get annoying if someone dumps a whole library in there.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import numpy as np
import structlog
from openai import OpenAI

logger = structlog.get_logger(__name__)

DOCUMENTS_DIR: Path = Path(__file__).resolve().parent.parent / "documents"
INDEX_DIR: Path = Path(__file__).resolve().parent / "index"

CHUNK_SIZE: int = 512
CHUNK_OVERLAP: int = 50
EMBEDDING_MODEL: str = "text-embedding-3-small"
EMBEDDING_BATCH_SIZE: int = 512  # OpenAI embedding API limit is 2048


class DocumentIndexer:
    """Loads, chunks, embeds, and indexes policy documents for RAG.

    Usage::

        indexer = DocumentIndexer(api_key="sk-...")
        stats = indexer.index_all()
        print(f"Indexed {stats['chunks']} chunks from {stats['documents']} documents")
    """

    def __init__(
        self,
        api_key: str | None = None,
        documents_dir: Path = DOCUMENTS_DIR,
        index_dir: Path = INDEX_DIR,
        embedding_model: str = EMBEDDING_MODEL,
    ) -> None:
        self._documents_dir: Path = documents_dir
        self._index_dir: Path = index_dir
        self._embedding_model: str = embedding_model

        resolved_key: str = api_key or os.environ.get("OPENAI_API_KEY", "")
        if not resolved_key:
            raise ValueError("No OpenAI API key — set OPENAI_API_KEY or pass api_key")

        self._client: OpenAI = OpenAI(api_key=resolved_key, timeout=60.0)

        self._chunks: list[str] = []
        self._metadata: list[dict[str, Any]] = []
        self._total_tokens: int = 0

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def index_all(self) -> dict[str, Any]:
        """Full pipeline: load -> chunk -> embed -> persist.

        Returns stats dict with document_count, chunk_count, token_count, etc.
        """
        raw_docs: list[dict[str, Any]] = self._load_documents()
        if not raw_docs:
            logger.warning("no_documents_found", dir=str(self._documents_dir))
            return {"documents": 0, "chunks": 0, "tokens": 0}

        self._chunks = []
        self._metadata = []

        for doc in raw_docs:
            doc_chunks, doc_meta = self._chunk(doc)
            self._chunks.extend(doc_chunks)
            self._metadata.extend(doc_meta)

        logger.info(
            "chunking_complete",
            documents=len(raw_docs),
            chunks=len(self._chunks),
        )

        if not self._chunks:
            logger.warning("no_chunks_produced")
            return {"documents": len(raw_docs), "chunks": 0, "tokens": 0}

        embeddings: np.ndarray = self._embed_and_store()

        self.save_index(embeddings)

        return {
            "documents": len(raw_docs),
            "chunks": len(self._chunks),
            "tokens": self._total_tokens,
            "index_path": str(self._index_dir),
        }

    # ------------------------------------------------------------------
    # document loading
    # ------------------------------------------------------------------

    def _load_documents(self) -> list[dict[str, Any]]:
        """Load all PDFs from the documents directory.

        Falls back to a simple text extraction if langchain's PDF loader
        isn't available (it depends on pypdf which is an optional dep).
        """
        self._documents_dir.mkdir(parents=True, exist_ok=True)
        pdf_files: list[Path] = sorted(self._documents_dir.glob("*.pdf"))

        if not pdf_files:
            logger.info("no_pdfs", dir=str(self._documents_dir))
            return []

        documents: list[dict[str, Any]] = []

        for pdf_path in pdf_files:
            try:
                pages: list[str] = self._extract_pdf_text(pdf_path)
                full_text: str = "\n\n".join(pages)

                if len(full_text.strip()) < 50:
                    logger.warning("pdf_too_short", file=pdf_path.name)
                    continue

                doc_hash: str = hashlib.md5(full_text.encode()).hexdigest()[:12]

                documents.append({
                    "filename": pdf_path.name,
                    "path": str(pdf_path),
                    "text": full_text,
                    "page_count": len(pages),
                    "hash": doc_hash,
                })

                logger.info("pdf_loaded", file=pdf_path.name, pages=len(pages))

            except Exception:
                logger.exception("pdf_load_failed", file=pdf_path.name)

        return documents

    def _extract_pdf_text(self, pdf_path: Path) -> list[str]:
        """Extract text from a PDF file, one string per page."""
        try:
            from langchain_community.document_loaders import PyPDFLoader

            loader = PyPDFLoader(str(pdf_path))
            pages = loader.load()
            return [page.page_content for page in pages]
        except ImportError:
            # langchain not available — try pypdf directly
            from pypdf import PdfReader

            reader = PdfReader(str(pdf_path))
            return [page.extract_text() or "" for page in reader.pages]

    # ------------------------------------------------------------------
    # chunking
    # ------------------------------------------------------------------

    def _chunk(self, doc: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
        """Split a document into overlapping chunks using langchain's splitter.

        We use RecursiveCharacterTextSplitter which tries to split on paragraph
        boundaries first, then sentences, then words.  This produces much
        better chunks than a naive fixed-size split — you don't end up with
        chunks that start mid-sentence.

        The 512-token chunk size was chosen empirically.  Smaller chunks (256)
        gave better precision but worse recall on policy questions.  Larger
        chunks (1024) retrieved too much noise and inflated prompt costs.
        """
        try:
            from langchain_text_splitters import RecursiveCharacterTextSplitter
        except ImportError:
            from langchain.text_splitter import RecursiveCharacterTextSplitter

        splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE * 4,  # roughly 4 chars per token
            chunk_overlap=CHUNK_OVERLAP * 4,
            length_function=len,
            separators=["\n\n", "\n", ". ", " ", ""],
        )

        text: str = doc["text"]
        raw_chunks: list[str] = splitter.split_text(text)

        chunks: list[str] = []
        metadata: list[dict[str, Any]] = []

        for i, chunk_text in enumerate(raw_chunks):
            chunk_text = chunk_text.strip()
            if len(chunk_text) < 20:
                continue

            chunks.append(chunk_text)
            metadata.append({
                "source": doc["filename"],
                "chunk_index": i,
                "doc_hash": doc["hash"],
                "page_count": doc["page_count"],
            })

        return chunks, metadata

    # ------------------------------------------------------------------
    # embedding
    # ------------------------------------------------------------------

    def _embed_and_store(self) -> np.ndarray:
        """Embed all chunks and return the embedding matrix.

        Batches requests to stay under the OpenAI embedding API's per-request
        limit (2048 inputs).  In practice we hit it rarely since most document
        sets produce <1000 chunks, but better safe than sorry.
        """
        all_embeddings: list[list[float]] = []

        for i in range(0, len(self._chunks), EMBEDDING_BATCH_SIZE):
            batch: list[str] = self._chunks[i : i + EMBEDDING_BATCH_SIZE]

            response = self._client.embeddings.create(
                model=self._embedding_model,
                input=batch,
            )

            batch_embeddings: list[list[float]] = [
                item.embedding for item in response.data
            ]
            all_embeddings.extend(batch_embeddings)

            if response.usage:
                self._total_tokens += response.usage.total_tokens

            batch_num: int = i // EMBEDDING_BATCH_SIZE + 1
            total_batches: int = (len(self._chunks) - 1) // EMBEDDING_BATCH_SIZE + 1
            logger.info(
                "embedding_batch",
                batch=f"{batch_num}/{total_batches}",
                chunks=len(batch),
            )

            # small sleep between batches to be polite to the API
            if i + EMBEDDING_BATCH_SIZE < len(self._chunks):
                time.sleep(0.5)

        return np.array(all_embeddings, dtype=np.float32)

    # ------------------------------------------------------------------
    # index persistence
    # ------------------------------------------------------------------

    def save_index(self, embeddings: np.ndarray) -> Path:
        """Build FAISS index and write everything to disk.

        We store three files:
          - wellnest_docs.index  — the FAISS binary index
          - chunks.json          — the raw text chunks (for retrieval)
          - metadata.json        — source file, chunk index, etc.

        If faiss-cpu isn't installed we fall back to saving the raw numpy
        array.  The retriever handles both formats.
        """
        self._index_dir.mkdir(parents=True, exist_ok=True)

        try:
            import faiss

            dim: int = embeddings.shape[1]
            index = faiss.IndexFlatIP(dim)

            # normalize for cosine similarity via inner product
            faiss.normalize_L2(embeddings)
            index.add(embeddings)

            index_path: Path = self._index_dir / "wellnest_docs.index"
            faiss.write_index(index, str(index_path))

            logger.info("faiss_index_saved", vectors=index.ntotal, dim=dim)

        except ImportError:
            logger.warning("faiss_not_available, saving raw embeddings")
            np.save(str(self._index_dir / "embeddings.npy"), embeddings)

        # always save the text and metadata — retriever needs these
        with open(self._index_dir / "chunks.json", "w") as f:
            json.dump(self._chunks, f, ensure_ascii=False)

        with open(self._index_dir / "metadata.json", "w") as f:
            json.dump(self._metadata, f, ensure_ascii=False)

        logger.info(
            "index_saved",
            path=str(self._index_dir),
            chunks=len(self._chunks),
            total_tokens=self._total_tokens,
        )

        return self._index_dir
