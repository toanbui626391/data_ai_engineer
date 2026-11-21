# OpenSearch

OpenSearch is an open-source (Apache 2.0) distributed search and analytics engine, forked from Elasticsearch 7.10 and governed by the OpenSearch Software Foundation (Linux Foundation).

## Issues It Solves

- **Slow full-text search** over large datasets: inverted indexes return results in milliseconds, where SQL `LIKE` queries scan whole tables.
- **Log and event analytics**: it ingests and queries high-volume logs, metrics and traces for observability and security.
- **Semantic and AI search**: its vector search finds results by meaning, which supports RAG and recommendations.
- **Scaling and availability**: it shards and replicates data across nodes, so there is no single point of failure.
- **Vendor lock-in and licensing**: it is fully open source, with no restrictive license and with security features included free.

## Key Features

| Area | Features |
|---|---|
| Search | Full-text search (BM25), fuzzy matching, highlighting, aggregations, Query DSL, SQL and PPL |
| Vector / AI | k-NN (HNSW, IVF, Faiss, Lucene, nmslib), neural search, hybrid search (lexical + vector), ML Commons, and a conversational search/RAG pipeline |
| Scalability | Sharding, replicas, auto-recovery, cross-cluster replication, snapshots, and searchable snapshots |
| Observability | Log analytics, trace analytics, alerting, and anomaly detection |
| Security | TLS, RBAC, fine-grained (index/document/field-level) access control, audit logs, SAML/OIDC |
| Data management | Index State Management (rollover, tiering, retention), ingest pipelines, and index templates |
| Tooling | OpenSearch Dashboards (visualization), Data Prepper, language clients, and a REST/JSON API |
| Deployment | Self-hosted, Kubernetes operator, Amazon OpenSearch Service, and Serverless |

## Typical Use Cases

Site and app search, log analytics (ELK-style), SIEM and security analytics, RAG and vector retrieval, and e-commerce recommendations.
