"""
WellNest AI module — RAG, community briefs, and LLM-based data quality.

Three sub-packages:

  ai.briefs    - GPT-4o-mini generated county-level community briefs.
                 Batch generates ~200-word summaries for NGO grant proposals.
                 Costs about $15 for all 3,200 counties as of 2025-01.

  ai.rag       - Retrieval-augmented generation over policy documents and
                 gold-layer data.  FAISS index + LangChain RetrievalQA chain.
                 Used by the community Q&A chatbot in the dashboard.

  ai.quality   - LLM-as-judge for suspicious data records.  Samples outliers
                 from the gold layer and asks GPT-4o-mini whether the values
                 are plausible.  Complement to the statistical checks in soda.

The documents/ subdirectory holds PDFs that get indexed for RAG — ESSA
guidelines, Title I guidance, CDC school health docs, etc.

All three sub-packages are orchestrated as Dagster assets (see
orchestration/assets/ai_assets.py) but can also be run standalone for
development and debugging.
"""
