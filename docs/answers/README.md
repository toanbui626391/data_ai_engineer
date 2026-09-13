# Enterprise AI & Data Engineer Interview: Model Answers & Reference Architectures

This directory contains master reference answers and production architectures for Staff and Principal (L6 / L7) technical interview questions.

## Directory Index

| File | Topic & Target Question | Level | Key Architecture Patterns |
| :--- | :--- | :---: | :--- |
| [01_enterprise_document_parsing_and_sanitization_l7.md](file:///Users/toanbui/dev/data_ai_engineer/docs/answers/01_enterprise_document_parsing_and_sanitization_l7.md) | **Enterprise Document Parsing & Noise-Free Sanitization for RAG**<br/>*(SharePoint multi-column PDFs, financial spreadsheets, Confluence macros, scanned OCR)* | **L6 / L7 (Principal)** | • Three-Tier Medallion Document Compiler<br/>• Confluence AST XML Macro Sanitizer (`lxml`)<br/>• Multi-Column XY-Cut Reading Order & Header/Footer Pruning<br/>• Table Linearization & Semantic Markdown Injection<br/>• Neural Vision OCR & Chart Multimodal Captioning<br/>• Non-blocking Quarantine Side-Output<br/>• FinOps Token Reduction (-34%) & Ragas Metrics Lift |
| [02_hierarchical_chunking_and_tokenizer_fidelity_l7.md](file:///Users/toanbui/dev/data_ai_engineer/docs/answers/02_hierarchical_chunking_and_tokenizer_fidelity_l7.md) | **Hierarchical Chunking & Multi-Model Tokenizer Fidelity for Enterprise RAG**<br/>*(Parent-child topologies, vector dilution, silent truncation, and tightest-ceiling budgeting)* | **L6 / L7 (Principal)** | • Hierarchical Parent-Child (Small-to-Big) Topology<br/>• Contextual Breadcrumb Lineage Injection<br/>• Tri-Model Stack Vocabulary Disconnect Resolution<br/>• Tightest-Ceiling Token Budgeting Equation<br/>• Character Offset Boundary Snapping (`return_offsets_mapping`)<br/>• Pre-Upsert Assertion Circuit Breakers & DLQ Quarantine |
