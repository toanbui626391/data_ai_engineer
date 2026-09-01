# Enterprise AI & Data Engineer Interview Question Bank

This directory contains a comprehensive, production-grade interview framework and question bank designed to evaluate and calibrate **Senior (L4/L5)** and **Staff (L6+) Data Engineers** for **Enterprise AI Agents Projects**.

---

## 📂 Question Bank Structure

| File | Focus Area | Description |
| :--- | :--- | :--- |
| [01_system_design_and_architecture.md](file:///Users/toanbui/dev/data_ai_engineer/questions/01_system_design_and_architecture.md) | **Architecture & System Design** | 4 enterprise-scale system design scenarios (Vector Lakehouses, Multi-Agent Memory, Text-to-SQL Semantic Layers, Streaming CDC). |
| [02_deep_dive_technical_and_failure_modes.md](file:///Users/toanbui/dev/data_ai_engineer/questions/02_deep_dive_technical_and_failure_modes.md) | **Deep-Dive Technical & Failure Modes** | In-depth technical questions covering HNSW/DiskANN memory math, backpressure, schema drift, embedding migration, and ACLs. |
| [03_practical_coding_and_debugging.md](file:///Users/toanbui/dev/data_ai_engineer/questions/03_practical_coding_and_debugging.md) | **Coding, Pipelines & Debugging** | Practical coding and debugging challenges (Distributed Semantic Chunking, AST SQL Guardrails, Streaming Backpressure). |
| [04_interviewer_evaluation_rubric_and_scorecard.md](file:///Users/toanbui/dev/data_ai_engineer/questions/04_interviewer_evaluation_rubric_and_scorecard.md) | **Scoring & Leveling Rubric** | Senior vs. Staff behavioral/technical calibration, green flags, red flags, and interview scorecard template. |
| [05_leveling_deep_dive_ingestion_pipeline.md](file:///Users/toanbui/dev/data_ai_engineer/questions/05_leveling_deep_dive_ingestion_pipeline.md) | **Leveling Deep-Dive (L4/L5/L6)** | Visual architecture comparison with Mermaid diagrams and FinOps cost models for Scenario 1 ingestion. |
| [06_custom_connectors_sharepoint_confluence_assessment.md](file:///Users/toanbui/dev/data_ai_engineer/questions/06_custom_connectors_sharepoint_confluence_assessment.md) | **Custom Knowledge Connectors Assessment** | Complete vetting guide for custom SharePoint & Confluence ingestion (Graph Delta queries, Zero-RAM streaming, Entra ID ACLs, 429 jitter, ADF/XHTML parsing). |

---

## 🎯 Candidate Leveling Quick Reference

| Level | Profile & Focus Area |
| :--- | :--- |
| **Junior / Mid (L3/L4)** | • Relies on tutorial defaults (LangChain, ChromaDB)<br/>• Unaware of distributed data bottlenecks and RAM limits |
| **Senior (L4/L5)** | • Strong execution in PySpark / Python streaming<br/>• Deterministic component pipelines, error handling & retries<br/>• Understands hybrid search, batching, and schema types |
| **Staff (L6+)** | • End-to-end distributed system architecture & trade-offs<br/>• Zero-downtime model migrations & memory hierarchies<br/>• FinOps cost modeling (RAM vs. NVMe) & zero-trust ACLs<br/>• Defines data contracts and leads cross-team architecture |
