# ADR-004: FAISS over ChromaDB for Vector Search

**Status:** Accepted
**Date:** 2024-09
**Author:** Engineering

## Context

WellNest's RAG pipeline needs a vector store to hold embeddings of federal education and health policy documents. The workflow:

1. Index: Chunk ~50 PDF documents (ESSA guidelines, Title I guidance, CDC school health resources, FEMA safety guides) into ~5,000 chunks of ~512 tokens each
2. Embed: Generate embeddings using OpenAI's `text-embedding-3-small` (1536 dimensions)
3. Store: Persist the vectors for retrieval
4. Query: At request time (`POST /api/ask`), embed the user's question and find the k nearest chunks
5. Generate: Feed the retrieved chunks to GPT-4o-mini for answer generation

The vector store needs to handle ~5,000 vectors of 1536 dimensions. Queries happen infrequently -- maybe a few dozen per day from the "Ask WellNest" dashboard page. Latency should be under 200ms for the retrieval step (the LLM generation takes 2-5 seconds anyway, so retrieval isn't the bottleneck).

## Decision

We chose FAISS (Facebook AI Similarity Search) via the `faiss-cpu` package, persisted to disk as a single file.

## What We Considered

### ChromaDB

ChromaDB is the most popular embedded vector database for LangChain projects. It's purpose-built for the RAG use case: embed, store, query, with a simple Python API.

**What we liked:** Dead-simple API. `chroma.add()` and `chroma.query()` are about as straightforward as it gets. Supports metadata filtering (e.g., "only search documents from 2023"). Has a built-in embedding function, so you can skip the explicit embedding step. The LangChain integration is seamless.

**Why we rejected it:**

1. **Operational overhead for our scale.** ChromaDB runs as a service (or an embedded SQLite-backed store). For 5,000 vectors, running a separate database service feels like overkill. It's another process to start, another thing to health-check, another dependency in Docker Compose. FAISS is a library, not a service -- it loads an index file into memory and you're done.

2. **Persistence story is changing.** ChromaDB's persistence has gone through several iterations (DuckDB backend, SQLite backend, the client-server split). I've been burned by persistence format changes in pre-1.0 libraries before. FAISS's binary format has been stable for years.

3. **We don't need metadata filtering.** ChromaDB's killer feature over FAISS is metadata filtering -- "find similar vectors WHERE document_type = 'essa_guidance'." We don't use this. Our document set is small enough that we retrieve the top-k and let the LLM figure out relevance. If we ever need filtering, we can add it at the application layer.

4. **ChromaDB adds ~200MB of dependencies.** The `chromadb` package pulls in hnswlib, posthog (telemetry), tokenizers, and various other packages. For a library that we use for exactly one feature (RAG), this is a lot of dependency surface area. `faiss-cpu` is a single package with minimal dependencies.

### Pinecone

Pinecone is a managed vector database. You get an API endpoint, upload vectors, and query them over the network.

**What we liked:** Zero operational burden. Generous free tier (1 index, 100K vectors, ~1GB).

**Why we rejected it:**

1. **Network dependency for retrieval.** Every RAG query would make a round-trip to Pinecone's servers. At our query volume (dozens/day), this isn't a performance concern. But it is a reliability concern -- if Pinecone is down, our RAG feature is down. With FAISS, the index is local, and retrieval works even if the internet is out.

2. **Vendor lock-in.** The Pinecone API isn't an industry standard. If they change pricing, go down, or sunset the free tier, we'd need to migrate. FAISS indices can be loaded by any program that links the FAISS library.

3. **We don't need managed scale.** Pinecone makes sense when you have millions of vectors, need low-latency retrieval at high QPS, and don't want to manage infrastructure. We have 5,000 vectors and a few queries per day. It's like renting a warehouse for a shoebox.

4. **Cost trajectory.** Free tier is fine now, but if we grow the document corpus (which we plan to), we'll hit the limit quickly. FAISS on disk costs nothing regardless of scale.

### pgvector (PostgreSQL extension)

pgvector adds vector similarity search to PostgreSQL. Since we're already running Postgres, this would add zero operational overhead.

**What we liked:** No new service. Vectors live in the same database as everything else. Can join vector search results with relational data in a single query.

**Why we rejected it:**

1. **Supabase free tier vector limits.** Supabase does support pgvector, but the free tier has limited compute for vector operations. HNSW index building on 5,000 vectors would be fine, but query latency can spike on the paused free-tier instance.

2. **LangChain integration is less mature.** LangChain's pgvector integration works but has more configuration knobs than FAISS (connection pooling, table names, index types). For our simple use case, FAISS is simpler to set up.

3. **Mixing concerns.** Putting vector embeddings in the same database as application data means backup, restore, and migration operations affect both. It also means the database needs enough memory for vector operations on top of normal query workloads. Not a problem at our scale, but it feels like unnecessary coupling.

We might revisit pgvector in the future. The "everything in one database" story is compelling, and pgvector has been improving rapidly. But for now, FAISS is simpler.

### Weaviate / Qdrant / Milvus

Full-featured vector databases. Overkill for our use case. These are designed for millions-to-billions of vectors with complex filtering, multi-tenancy, and horizontal scaling. We have 5,000 vectors and a single user.

## Why FAISS Won

### 1. Our vector search volume is tiny

Let me be blunt about the scale: 5,000 vectors of 1536 dimensions, queried a few dozen times per day. This is a toy workload for any vector search solution. FAISS loads the entire index into memory (~45MB) and does a brute-force exact nearest neighbor search in <5ms. There's no need for approximate nearest neighbor algorithms, no need for sharding, no need for a separate service.

The FAISS index is built once by `ai/rag/indexer.py` and saved to disk as `ai/rag/faiss_index/`. The API loads it lazily on the first `/ask` request. Total memory overhead: ~45MB. That's it.

### 2. Operational simplicity

FAISS is a library, not a service. There's no daemon to start, no port to configure, no health check to write, no Docker service to add. The index is a file on disk. You `FAISS.load_local()` and you're ready to query.

Compare this to ChromaDB (SQLite files + a server process) or Pinecone (API key + network calls + vendor account). For a feature that amounts to "search 5,000 text chunks," the simplest solution wins.

### 3. Performance is more than adequate

On an M2 MacBook Air, searching 5,000 vectors with FAISS `IndexFlatL2` (brute-force exact search):

| Metric | Value |
|--------|-------|
| Index load time | 120ms |
| Query time (k=5) | 3ms |
| Memory footprint | ~45MB |
| Index file size on disk | ~35MB |

For comparison, the LLM completion step takes 2-5 seconds. Retrieval latency is irrelevant -- it could be 100x slower and still not matter.

If we ever scale to 100K+ vectors (unlikely with our current document corpus), we'd switch from `IndexFlatL2` to `IndexIVFFlat` or `IndexHNSW` for approximate search. FAISS supports these out of the box -- it's a configuration change, not a migration.

### 4. LangChain integration is solid

LangChain's FAISS integration is one of the most mature vector store integrations. The code is simple:

```python
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings

embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
vectorstore = FAISS.load_local("ai/rag/faiss_index", embeddings)
docs = vectorstore.similarity_search("What is Title I?", k=5)
```

Three lines to load and query. The `allow_dangerous_deserialization=True` flag is needed because FAISS indices are pickle files (a known security concern if you're loading untrusted indices, but ours are self-generated).

### 5. Cost

$0. The `faiss-cpu` package is free, the index file is stored on local disk, and queries use only CPU cycles. No API calls, no managed service fees, no bandwidth costs.

We specifically chose `faiss-cpu` over `faiss-gpu` because:
- GPU builds are a pain on CI (need CUDA drivers)
- CPU is more than fast enough for 5,000 vectors
- The CPU package is a simple pip install

## Consequences

### Positive

- Zero operational overhead. No service to run, no configuration to manage.
- Sub-5ms query latency on our corpus.
- ~45MB memory footprint.
- Stable binary format -- the same FAISS index works across versions.
- $0 cost, now and at any scale we'll realistically reach.
- Dead-simple LangChain integration.

### Negative

- **No metadata filtering.** If we need "search only ESSA documents" or "search only documents from 2023," we'd need to implement this at the application layer (pre-filter before FAISS search, or post-filter results). For now we don't need this.

- **Single-file persistence.** The FAISS index is one binary file. If it gets corrupted, we rebuild from scratch (re-embed all documents). This takes about 5 minutes and costs ~$0.50 in OpenAI embedding API calls. Not a big deal, but there's no incremental update -- adding a new document means rebuilding the entire index.

- **No built-in versioning.** ChromaDB and Pinecone track collection versions. FAISS is just a file. We version it by naming convention (`faiss_index/`) and rebuilding monthly alongside the AI brief pipeline.

- **Memory-resident.** The entire index must fit in memory. At 5,000 vectors x 1536 dimensions x 4 bytes = ~30MB, this is trivial. But if we grew to 1M vectors (~6GB), we'd need to use FAISS's disk-based options or switch to a dedicated vector database.

- **pickle security.** FAISS indices are serialized with pickle, which has known deserialization vulnerabilities. This is fine because we only load indices we built ourselves. But don't load FAISS indices from untrusted sources.

### Revisiting This Decision

We should revisit if:
- Document corpus grows beyond ~50,000 chunks (unlikely near-term)
- We need metadata filtering for document-type-specific retrieval
- We need real-time index updates (currently rebuilding monthly is fine)
- We want to offer per-user or per-organization document collections (multi-tenancy)

The migration path is straightforward: ChromaDB and pgvector both accept the same LangChain retriever interface. Switching the vector store backend is a configuration change in `ai/rag/retriever.py`, not an architecture change.
