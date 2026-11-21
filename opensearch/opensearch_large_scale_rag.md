# Large-Scale RAG on OpenSearch: Top 5 Issues & Data Model Design

When OpenSearch serves as both **vector/hybrid retriever** and **chunk store** at tens or hundreds of millions of chunks, five architectural bottlenecks dominate production systems. Below are those issues and how to design the OpenSearch data model to solve them.

---

## Top 5 Issues & Data Model Solutions

### 1. HNSW RAM Explosion & `_source` Storage Bloat
- **Issue**: Full `float32` HNSW graphs live in off-heap memory (~4–6 GB RAM per 1M `1024`-dim vectors before replicas). Keeping embeddings inside `_source` duplicates vectors on disk, slows Lucene segment merges, and inflates network payloads on every search hit.
- **Data Model Fix**:
  - Enable **disk-based / quantized vectors** (`mode: "on_disk"` with `compression_level: "32x"` binary quantization + rescoring, or `fp16` / `int8`).
  - Exclude vectors from `_source`: `"_source": { "excludes": ["chunk_embedding"] }`.
  - Disable indexing on display-only fields (`"index": false` on `chunk_text` and `citation_url`).

### 2. Context Loss & Single-Document Flooding ("Lost in Chunking")
- **Issue**: Small chunks match embeddings well but strip away document titles/section hierarchy, or `top-k` gets flooded by 8 chunks from the same long manual. Using `nested` chunk arrays inside one parent document causes severe write amplification (updating 1 chunk re-indexes all sibling chunks and vectors).
- **Data Model Fix**:
  - Use a **flat `1-chunk = 1-document` schema** with **denormalized parent context** (`doc_id`, `doc_title`, `heading_path`, `chunk_index`).
  - Index a contextualized `search_text` (`"{doc_title} > {heading_path}\n{chunk_text}"`) for BM25 and embedding generation, while storing clean `chunk_text` (`index: false`) for LLM prompts.
  - Deduplicate single-doc flooding at query time with `collapse: { "field": "doc_id" }`, and fetch surrounding window chunks (`chunk_index - 1`, `chunk_index + 1`) via fast `_mget` on deterministic IDs (`{doc_id}#{chunk_index}`).

### 3. Multi-Tenant Scatter-Gather Latency vs. Oversharding
- **Issue**: Creating one index per customer produces thousands of tiny shards that overwhelm Cluster Manager state; using one unrouted shared index forces every query to scan HNSW graphs across *all* shards even for a single small tenant.
- **Data Model Fix**:
  - **Tiered Multi-Tenancy with `_routing`**: Put small/medium tenants in a shared index with `"_routing": { "required": true }` set to `tenant_id` (routing writes and queries to 1 shard instead of the whole cluster); isolate only giant "whale" tenants into dedicated indices.
  - **Strict `keyword` Pre-Filters**: Map `tenant_id`, `source_type`, and `acl_principals` as `keyword` (never `text`) so Lucene/Faiss applies fast bitset pre-filtering *during* HNSW traversal without starving `top-k`.

### 4. Orphaned "Ghost" Chunks & ACL Churn Re-Indexing Storms
- **Issue**: Editing a 10-chunk Confluence page down to 6 chunks via naive upserts leaves chunks `7–10` orphaned in the index, causing stale hallucinations. Furthermore, storing user emails per chunk forces massive re-indexing/re-embedding whenever team membership changes (especially since excluding vectors from `_source` prevents partial `_update` without re-sending vectors).
- **Data Model Fix**:
  - **Deterministic `_id` + Version Sweep**: Use `_id = "{doc_id}#{chunk_index}"` alongside `doc_version` and `content_hash`. Skip unchanged hashes, upsert updated chunks, then run `delete_by_query` where `doc_id == X AND doc_version < new_version` to remove trailing ghost chunks.
  - **Group/Principal Tokens over User Lists**: Store stable group/role tokens in `acl_principals: keyword` (e.g., `["group:eng", "space:infra"]`). Resolve `user -> groups` at query time via a `terms` filter or DLS role so user membership changes require zero OpenSearch writes.

### 5. Embedding Model Lock-In & Re-Indexing Downtime
- **Issue**: Upgrading embedding models (e.g., `768` -> `1024` dims) or changing chunking rules cannot alter an existing `knn_vector` mapping in place, risking search downtime or expensive re-crawls of rate-limited upstream APIs (Confluence, Slack, Drive).
- **Data Model Fix**:
  - Route applications strictly through **Read/Write Index Aliases** (`rag_chunks_read`, `rag_chunks_write`) pointing to versioned physical indices (`rag_chunks_v1_bge_1024`).
  - Persist `chunk_text`, `embedding_model`, and `chunker_version` in `_source` (or an object-store `blob_uri`) so you can build `rag_chunks_v2` via `_reindex` + an Ingest Pipeline and atomically swap aliases with zero downtime.

---

## Reference Production Index Mapping

```json
PUT rag_chunks_v1_bge_1024
{
  "aliases": {
    "rag_chunks_read": {},
    "rag_chunks_write": {}
  },
  "settings": {
    "index": {
      "knn": true,
      "number_of_shards": 12,
      "number_of_replicas": 1,
      "refresh_interval": "5s"
    }
  },
  "mappings": {
    "dynamic": "strict",
    "_routing": { "required": true },
    "_source": {
      "excludes": ["chunk_embedding"]
    },
    "properties": {
      "tenant_id":       { "type": "keyword" },
      "doc_id":          { "type": "keyword" },
      "chunk_index":     { "type": "integer" },
      "doc_version":     { "type": "long" },
      "content_hash":    { "type": "keyword" },

      "source_type":     { "type": "keyword" },
      "acl_principals":  { "type": "keyword" },
      "custom_labels":   { "type": "keyword" },
      "updated_at":      { "type": "date" },

      "doc_title":       { "type": "text", "fields": { "raw": { "type": "keyword" } } },
      "heading_path":    { "type": "text" },
      "search_text":     { "type": "text", "analyzer": "standard" },
      "chunk_text":      { "type": "text", "index": false },
      "citation_url":    { "type": "keyword", "index": false },

      "embedding_model": { "type": "keyword" },
      "chunker_version": { "type": "keyword" },
      "chunk_embedding": {
        "type": "knn_vector",
        "dimension": 1024,
        "method": {
          "name": "hnsw",
          "engine": "faiss",
          "space_type": "innerproduct"
        },
        "mode": "on_disk",
        "compression_level": "32x"
      }
    }
  }
}
```

## Example Indexed Document & Query Pattern

```json
// PUT rag_chunks_write/_doc/conf_9921%232?routing=acme
{
  "tenant_id": "acme",
  "doc_id": "conf_9921",
  "chunk_index": 2,
  "doc_version": 17,
  "content_hash": "sha256:8f14e45f",
  "source_type": "confluence",
  "acl_principals": ["group:sre", "space:prod-runbooks"],
  "custom_labels": ["env:prod", "service:payments"],
  "updated_at": "2026-11-21T10:00:00Z",
  "doc_title": "Payments DB Failover Runbook",
  "heading_path": "Failover Steps > Promote Read Replica",
  "search_text": "Payments DB Failover Runbook > Failover Steps > Promote Read Replica\nRun pg_ctl promote on replica-02...",
  "chunk_text": "Run pg_ctl promote on replica-02...",
  "citation_url": "https://wiki.acme.internal/pages/9921",
  "embedding_model": "bge-large-en-v1.5",
  "chunker_version": "v2_markdown_512t",
  "chunk_embedding": [0.021, -0.048, "... 1024 floats ..."]
}
```

### Routed, Pre-Filtered Hybrid Query with Document Collapse

```json
// POST rag_chunks_read/_search?routing=acme&search_pipeline=hybrid_rrf_rerank
{
  "size": 10,
  "_source": ["doc_id", "chunk_index", "doc_title", "heading_path", "chunk_text", "citation_url"],
  "collapse": { "field": "doc_id" },
  "query": {
    "hybrid": {
      "queries": [
        {
          "bool": {
            "must": [{ "match": { "search_text": "promote postgres replica failover" } }],
            "filter": [
              { "term":  { "tenant_id": "acme" } },
              { "terms": { "acl_principals": ["group:sre", "group:all-eng"] } }
            ]
          }
        },
        {
          "knn": {
            "chunk_embedding": {
              "vector": [0.019, -0.051, "..."],
              "k": 30,
              "filter": {
                "bool": {
                  "filter": [
                    { "term":  { "tenant_id": "acme" } },
                    { "terms": { "acl_principals": ["group:sre", "group:all-eng"] } }
                  ]
                }
              }
            }
          }
        }
      ]
    }
  }
}
```
