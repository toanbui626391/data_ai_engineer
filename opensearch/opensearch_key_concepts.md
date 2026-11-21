# OpenSearch Key Concepts

OpenSearch organizes data as JSON documents distributed across a cluster of nodes and backed by Apache Lucene shards.

## 1. Cluster & Nodes

- **Cluster**: A collection of nodes working together under one cluster name to store data and serve queries.
- **Node**: A single OpenSearch instance (server/container). Common node roles:
  - **Cluster Manager** (formerly Master): Manages cluster state, index creation/deletion, and shard allocation.
  - **Data**: Holds shards and executes CRUD, search, and aggregation operations.
  - **Ingest**: Runs pre-indexing transformation pipelines (parsing, chunking, enriching).
  - **ML**: Runs machine learning workloads (embeddings, rerankers) isolated from data nodes.
  - **Coordinating**: Routes client requests, scatters queries to shards, and gathers final results.

## 2. Data Model

- **Document**: The basic unit of data—a JSON object stored in an index with metadata (`_index`, `_id`, `_source`, `_routing`).
- **Index**: A logical namespace grouping related documents with a shared schema (roughly analogous to a database table).
- **Mapping**: The schema definition for an index specifying field types and how they are indexed:
  - `text`: Analyzed into tokens for full-text search (e.g., body text, titles).
  - `keyword`: Stored verbatim for exact filtering, sorting, and aggregations (e.g., IDs, tags, status).
  - `knn_vector`: Stores numerical embeddings for approximate or exact nearest-neighbor search.
- **Alias**: A virtual pointer to one or more physical indices, enabling zero-downtime re-indexing and swaps.

## 3. Shards, Segments & Routing

- **Shard**: A self-contained Apache Lucene instance holding a horizontal partition of an index's data.
  - **Primary Shard**: Accepts writes first and replicates changes; primary shard count is fixed at index creation.
  - **Replica Shard**: Read-only copy of a primary shard placed on a different node for high availability and read throughput.
- **Segment**: Immutable internal storage files inside a Lucene shard. New writes buffer in memory, become searchable after a **refresh** (default `1s`, making OpenSearch *near real-time*), and are periodically compacted via background **merges**.
- **Routing**: Formula (`hash(_routing) % num_primary_shards`, defaulting to `_id`) that determines which shard stores or queries a document.

## 4. Indexing Structures & Text Analysis

- **Inverted Index**: Maps each unique term to the list of documents containing it (`term -> [doc_1, doc_8, ...]`), enabling millisecond full-text lookups.
- **Analyzer**: Pipeline that converts `text` strings into searchable tokens during indexing and querying:
  1. **Character Filters** (strip HTML, replace symbols) -> 2. **Tokenizer** (split into words) -> 3. **Token Filters** (lowercase, stemming, synonyms, stopwords).
- **Doc Values**: Columnar on-disk structure built for `keyword`, numeric, and date fields to power fast sorting, filtering, and aggregations.
- **Vector Index (HNSW / IVF)**: Graph- or cluster-based structures (via Faiss or Lucene) for fast approximate k-Nearest Neighbor (`k-NN`) semantic search.

## 5. Querying & Pipelines

- **Query Context vs. Filter Context**:
  - **Query Context**: Calculates a relevance `_score` (e.g., `BM25` for text, cosine/inner-product for vectors).
  - **Filter Context**: Binary match (`yes/no`) using cached bitsets with zero scoring overhead—always preferred for exact constraints (`tenant_id`, dates, ACLs).
- **Aggregations**: Real-time analytics (metrics, buckets, histograms, facets) computed over query results.
- **Ingest & Search Pipelines**: Modular processor chains that transform documents before indexing (e.g., chunking, embedding) or modify queries/results at search time (e.g., hybrid score normalization, reranking).
- **Index State Management (ISM) & Snapshots**: Policies for automating index rollover/retention (hot -> warm -> cold -> delete) and incremental backups to object storage (S3/GCS).
