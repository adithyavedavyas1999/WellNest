"""
LangChain QA chain for the WellNest community chatbot.

Wraps PolicyRetriever in a RetrievalQA-style chain that:
  1. Retrieves relevant policy document chunks
  2. Builds a grounded prompt with source citations
  3. Sends to GPT-4o-mini for answer generation
  4. Returns the answer with source references

We're not using LangChain's RetrievalQA directly because it doesn't give
us enough control over the prompt format and source citation style.
Instead we use the retriever for context and build the chain manually.
Less magic, more debuggable.

Token budget per query:
  - System prompt: ~200 tokens
  - Retrieved context (5 chunks × ~200 tokens): ~1,000 tokens
  - User question: ~50 tokens
  - Response: ~300 tokens
  Total: ~1,550 tokens per query ≈ $0.0002 at gpt-4o-mini pricing

  At 1,000 queries/day that's about $6/month.  Very manageable.
"""

from __future__ import annotations

import os
from typing import Any

import structlog
from openai import OpenAI

from ai.rag.retriever import PolicyRetriever, RetrievalResult

logger = structlog.get_logger(__name__)

QA_SYSTEM_PROMPT: str = (
    "You are WellNest Assistant, an AI that answers questions about education "
    "policy, school health programs, and child wellbeing in the United States.  "
    "You work for the Chicago Education & Analytics Collaborative (ChiEAC).\n\n"
    "Rules:\n"
    "- Only answer based on the provided context.  If the context doesn't "
    "contain enough information, say so.\n"
    "- Cite your sources using [Source: filename] notation.\n"
    "- Be specific — include numbers, percentages, and page references when "
    "available.\n"
    "- If the question is about a specific school or county, and you don't "
    "have data for it, say so rather than guessing.\n"
    "- Keep answers under 300 words unless the question requires more detail."
)

# this gets filled in with retrieved chunks at query time
QA_CONTEXT_TEMPLATE: str = """\
Use the following policy documents to answer the user's question.  \
Cite sources using [Source: filename] when referencing specific information.

{context}

---
If the above context doesn't contain the answer, say "I don't have enough \
information in the policy documents to answer that question."  Do not make \
up information.\
"""


class WellNestQA:
    """Question-answering chain backed by policy document retrieval.

    Usage::

        qa = WellNestQA(api_key="sk-...")
        answer = qa.ask("What are the Title I funding requirements?")
        print(answer["response"])
        for src in answer["sources"]:
            print(f"  - {src}")
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "gpt-4o-mini",
        retriever: PolicyRetriever | None = None,
        top_k: int = 5,
        temperature: float = 0.2,
    ) -> None:
        resolved_key: str = api_key or os.environ.get("OPENAI_API_KEY", "")
        if not resolved_key:
            raise ValueError("No OpenAI API key — set OPENAI_API_KEY or pass api_key")

        self._model: str = model
        self._top_k: int = top_k
        self._temperature: float = temperature

        self._client: OpenAI = OpenAI(
            api_key=resolved_key,
            timeout=45.0,
            max_retries=2,
        )

        self._retriever: PolicyRetriever = retriever or PolicyRetriever(
            api_key=resolved_key
        )

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def ask(self, question: str) -> dict[str, Any]:
        """Ask a question and get a grounded answer with sources.

        Returns:
            {
                "question": original question,
                "response": LLM-generated answer,
                "sources": list of source filenames,
                "chunks_used": number of context chunks,
                "usage": {"prompt_tokens": ..., "completion_tokens": ...},
            }
        """
        retrieved: list[RetrievalResult] = self._retriever.search(
            question, top_k=self._top_k
        )

        messages: list[dict[str, str]] = self._build_chain(question, retrieved)

        response = self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=self._temperature,
            max_tokens=800,
        )

        answer_text: str = response.choices[0].message.content or ""

        sources: list[str] = list({r.source for r in retrieved if r.source != "unknown"})

        usage_info: dict[str, int] = {}
        if response.usage:
            usage_info = {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
            }

        logger.info(
            "qa_response",
            question=question[:80],
            sources=len(sources),
            chunks=len(retrieved),
            **usage_info,
        )

        return {
            "question": question,
            "response": answer_text,
            "sources": sources,
            "chunks_used": len(retrieved),
            "usage": usage_info,
        }

    # ------------------------------------------------------------------
    # chain construction
    # ------------------------------------------------------------------

    def _build_chain(
        self,
        question: str,
        context_chunks: list[RetrievalResult],
    ) -> list[dict[str, str]]:
        """Assemble the message list for the chat completion.

        We format each context chunk with its source and relevance score
        so the LLM can weight them appropriately.  In practice GPT-4o-mini
        is pretty good at ignoring low-relevance context without us having
        to explicitly tell it to.
        """
        context_parts: list[str] = []
        for i, chunk in enumerate(context_chunks, 1):
            header: str = f"[Source: {chunk.source} | Relevance: {chunk.score:.2f}]"
            context_parts.append(f"{header}\n{chunk.text}")

        context_block: str = "\n\n---\n\n".join(context_parts) if context_parts else (
            "No relevant policy documents found."
        )

        context_message: str = QA_CONTEXT_TEMPLATE.format(context=context_block)

        return [
            {"role": "system", "content": QA_SYSTEM_PROMPT},
            {"role": "user", "content": f"{context_message}\n\nQuestion: {question}"},
        ]
