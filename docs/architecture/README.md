# Enterprise AI & Data Architecture Reference Specifications

This directory contains master reference architecture blueprints, solution design documents (SADD), and production failure-mode specifications for enterprise AI platforms, real-time ingestion lakehouses, and agent systems.

## Architecture Specifications Index

| Specification Document | Scope & System Focus | Target Scale & Constraints | Level |
| :--- | :--- | :--- | :---: |
| [enterprise_rag_system_architecture.md](file:///Users/toanbui/dev/data_ai_engineer/docs/architecture/enterprise_rag_system_architecture.md) | **Enterprise Production RAG System: End-to-End Solution Architecture**<br/>*(Medallion Lakehouse, Parent-Child Chunking, Hybrid Search, Zero-Trust ACLs, DiskANN FinOps, Blue/Green Migration)* | 50M+ documents, 150M chunks, p95 < 150ms, 60s sync SLA, Entra ID zero-trust | **L6 / L7 (Principal)** |
| [chunking_tokenizing_indexing_strategy.md](file:///Users/toanbui/dev/data_ai_engineer/docs/architecture/chunking_tokenizing_indexing_strategy.md) | **Enterprise RAG Strategy: Chunking, Tokenizing & Hybrid Indexing**<br/>*(Parent-Child Small-to-Big, Tightest-Ceiling Budgeting, Table Linearization, SQ8 HNSW + BM25, Filtered HNSW)* | 50M docs, 150M chunks, zero silent truncation, 75% RAM reduction, hybrid fusion | **L6 / L7 (Principal)** |
| [ingestion_pipeline.md](file:///Users/toanbui/dev/data_ai_engineer/docs/architecture/ingestion_pipeline.md) | **Real-Time Ingestion Pipeline Architecture**<br/>*(Flink flow control, bad-data quarantine, contract gates, zero-waste deduplication)* | 15,000 CDC events/sec, sub-second vector index freshness, automated quarantine | **L6 (Staff)** |
| [agent_memory_and_checkpointing.md](file:///Users/toanbui/dev/data_ai_engineer/docs/architecture/agent_memory_and_checkpointing.md) | **Agent Memory & Checkpointing Architecture**<br/>*(4-tier memory hierarchy, durable state machine, dynamic windowing, distributed cache)* | Multi-agent coordination, sub-second working memory, cross-session episodic persistence | **L6 (Staff)** |

---

## Key Design Principles

1. **Decoupled Medallion Lakehouses**: S3 Bronze (Raw Binaries) $\to$ S3 Silver (Clean Normalized Markdown AST) $\to$ S3 Gold (Tokenized Chunks).
2. **FinOps & Scale-Conscious Quantization**: Scalar Quantization (SQ8) and NVMe-backed DiskANN layouts reducing vector RAM footprints by 75–85%.
3. **Zero-Trust Security Trimming**: Item-level Entra ID ACL inheritance via payload-aware Filtered HNSW and threshold switching to prevent graph disconnection.
4. **Resiliency & Circuit Breaking**: Non-blocking poison-pill quarantine side-outputs and automated regression eval gates.
