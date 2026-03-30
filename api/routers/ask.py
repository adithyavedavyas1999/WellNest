"""
RAG endpoint — POST /api/ask

Takes a natural language question, retrieves relevant chunks from our
FAISS index of federal education/health policy docs, and generates an
answer with GPT-4o-mini via LangChain.

If the RAG chain isn't loaded (missing FAISS index or no OpenAI key),
we return a 503 instead of crashing. The dashboard falls back to
showing a "RAG unavailable" message.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from api.config import Settings, get_settings

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ask"])

# we lazy-load the chain to avoid importing langchain at module level
# (it's heavy and not every API worker needs it)
_rag_chain = None
_chain_loaded = False


class AskRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=1000,
        description="Natural language question about child wellbeing, education policy, etc.",
    )
    max_sources: int = Field(default=3, ge=1, le=10)


class Source(BaseModel):
    document: str
    page: int | None = None
    chunk_text: str = Field(..., description="Relevant excerpt from the source document")
    relevance_score: float | None = None


class AskResponse(BaseModel):
    question: str
    answer: str
    sources: list[Source] = []
    model: str
    responded_at: datetime


def _load_rag_chain(settings: Settings):
    """
    Lazy-load the LangChain RAG chain. We keep this separate so the import
    cost only hits the first /ask request, not every worker startup.
    """
    global _rag_chain, _chain_loaded

    if _chain_loaded:
        return _rag_chain

    if not settings.openai_api_key:
        logger.warning("OPENAI_API_KEY not set — RAG endpoint will be unavailable")
        _chain_loaded = True
        return None

    try:
        from langchain.chains import RetrievalQA
        from langchain_community.vectorstores import FAISS
        from langchain_openai import ChatOpenAI, OpenAIEmbeddings

        embeddings = OpenAIEmbeddings(
            model=settings.openai_embedding_model,
            openai_api_key=settings.openai_api_key,
        )

        # FAISS index lives on disk — built by ai/rag/indexer.py
        faiss_path = "ai/rag/faiss_index"
        vectorstore = FAISS.load_local(
            faiss_path,
            embeddings,
            allow_dangerous_deserialization=True,
        )

        llm = ChatOpenAI(
            model=settings.openai_model,
            openai_api_key=settings.openai_api_key,
            temperature=0.2,
        )

        _rag_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=vectorstore.as_retriever(search_kwargs={"k": 5}),
            return_source_documents=True,
        )
        logger.info("RAG chain loaded successfully (model=%s)", settings.openai_model)

    except FileNotFoundError:
        logger.warning("FAISS index not found at ai/rag/faiss_index — run indexer first")
        _rag_chain = None
    except Exception:
        logger.exception("Failed to initialize RAG chain")
        _rag_chain = None

    _chain_loaded = True
    return _rag_chain


@router.post("/ask", response_model=AskResponse)
def ask_wellnest(
    body: AskRequest,
    settings: Settings = Depends(get_settings),
) -> AskResponse:
    chain = _load_rag_chain(settings)

    if chain is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="RAG pipeline is not available. Check that OPENAI_API_KEY is set and the FAISS index exists.",
        )

    result = chain.invoke({"query": body.question})

    answer_text = result.get("result", "")
    source_docs = result.get("source_documents", [])

    sources = []
    for doc in source_docs[: body.max_sources]:
        meta = doc.metadata or {}
        sources.append(
            Source(
                document=meta.get("source", "unknown"),
                page=meta.get("page"),
                chunk_text=doc.page_content[:500],
                relevance_score=meta.get("score"),
            )
        )

    return AskResponse(
        question=body.question,
        answer=answer_text,
        sources=sources,
        model=settings.openai_model,
        responded_at=datetime.now(UTC),
    )
