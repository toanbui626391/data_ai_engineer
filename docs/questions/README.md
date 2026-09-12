# Enterprise RAG & Data Systems Interview Frameworks

This directory contains technical assessment guides for vetting Senior and Principal/Staff Engineers on enterprise-scale AI, Vector Lakehouse, and RAG architectures.

## Assessments

- [01_enterprise_rag_senior_principal_assessment.md](file:///Users/toanbui/dev/data_ai_engineer/docs/questions/01_enterprise_rag_senior_principal_assessment.md): Comprehensive assessment guide covering:
  - **Candidate Leveling Matrix ("The Reality Filter")**: Differentiating tutorial builders (L4) from Senior (L5) and Principal/Staff (L6/L7) architects.
  - **Build vs. Buy Architectural Boundaries**: Why custom RAG data pipelines are required over off-the-shelf managed services (Bedrock KB, Azure AI Search).
  - **Thematic Deep-Dive Probes**:
    - Layout-aware parsing, table linearization, and Confluence XHTML macro stripping.
    - Parent-Child (Small-to-Big) chunking, semantic chunking, and tokenizer vocabulary alignment.
    - Hybrid retrieval (Dense HNSW + Sparse BM25 via Reciprocal Rank Fusion) and Cross-Encoder re-ranking latency budgeting.
    - The **HNSW Graph Disconnection Problem** during zero-trust security trimming (Entra ID ACL pre-filtering vs post-filtering recall collapse).
    - 100M-vector RAM mathematics, HNSW index graph overhead, and FinOps optimization via Scalar Quantization (SQ8) & NVMe DiskANN.
    - Zero-downtime embedding model migration via CDC dual-writing and shadow index backfills.
  - **Live Whiteboard System Design Challenge**: 50M-document multi-tenant enterprise RAG platform with <250ms p95 latency and 60s GDPR tombstone purging.
  - **Distributed Coding Challenge**: Production-grade PySpark/Python `HierarchicalDocumentChunker` with strict BPE tokenizer alignment and ACL inheritance.
  - **100-Point Weighted Interviewer Scorecard & Evaluation Rubric**.
