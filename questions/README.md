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

---

## 🎯 Candidate Leveling Quick Reference

```
+----------------------------------------------------------------------------------------------------+
|                                    CANDIDATE LEVELING SPECTRUM                                     |
+----------------------------------------------------------------------------------------------------+
|  Junior / Mid-Level (L3/L4)  |  - Relies on default tutorial stacks (LangChain defaults, ChromaDB) |
|                              |  - Unaware of distributed data bottlenecks and vector RAM overhead |
+------------------------------+---------------------------------------------------------------------+
|  Senior Engineer (L4 / L5)   |  - Strong execution, clean PySpark/Flink/Python streaming code     |
|                              |  - Component-level design, deterministic pipelines, error handling  |
|                              |  - Understands hybrid search, embedding batching, and schema typing |
+------------------------------+---------------------------------------------------------------------+
|  Staff Engineer (L6+)        |  - End-to-end distributed system architecture and trade-off mastery |
|                              |  - Zero-downtime model migrations, multi-agent memory hierarchies    |
|                              |  - FinOps cost modeling (RAM vs NVMe), zero-trust security & ACLs   |
|                              |  - Defines enterprise data contracts and mentors team leads         |
+----------------------------------------------------------------------------------------------------+
```
