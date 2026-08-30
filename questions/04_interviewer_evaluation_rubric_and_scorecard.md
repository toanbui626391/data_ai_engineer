# Interviewer Evaluation Rubric & Scorecard

This guide provides interviewers with concrete criteria to evaluate, score, and calibrate candidates across **Senior (L4/L5)** and **Staff (L6+)** levels for Data Engineering roles in enterprise AI Agent projects.

---

## 1. Competency Scoring Dimensions

Each candidate is evaluated on a 1–5 scale across 5 core competencies:

```
[1. Distributed Systems & Scale]   - Streaming CDC, Kafka/Flink backpressure, Lakehouse (Iceberg/Delta)
[2. Vector & Context Operations]    - Vector indexing (HNSW/DiskANN), memory math, hybrid retrieval, RRF
[3. Reliability & Failure Modes]    - Idempotency, crash recovery, state checkpointing, drift detection
[4. Security, Governance & FinOps]  - ACLs, PII redaction token vaults, cloud RAM cost optimization
[5. Code Quality & Problem Solving] - AST parsing, type safety, async batching, debugging production bugs
```

---

## 2. Senior vs. Staff Leveling Matrix

| Dimension | Senior Data Engineer (L4 / L5) | Staff Data Engineer (L6+) |
| :--- | :--- | :--- |
| **System Scope** | **Component & Pipeline Level**<br>Takes a well-defined vector sync or memory pipeline and delivers robust, production-grade implementation. | **Platform & Distributed System Level**<br>Designs cross-system architectures, defines data contracts, manages multi-tenant isolation, and sets technical standards. |
| **Engineering Rigor** | Handles known edge cases: retries, dead-letter queues, partition skew, schema validation. | Anticipates non-obvious failure cascades: HNSW recall collapse on pre-filtered ACLs, dual-index migration divergence, split-brain agent states. |
| **Trade-Off Articulation** | Explains *how* a tool works (e.g., how Kafka consumer groups work). | Explains *why* one architectural choice was selected over alternatives, with clear trade-offs between latency, throughput, compute cost, and engineering complexity. |
| **Cost & FinOps Awareness** | Selects sensible instance sizes and cleans up unused resources. | Calculates exact RAM/GPU cost envelopes (e.g., $6\text{TB}$ RAM for in-memory HNSW vs. NVMe DiskANN) and designs for 70%+ cost reduction. |
| **Security & Governance** | Applies input sanitization, masks PII using standard libraries, uses read-only DB roles. | Architects zero-trust data pipelines, deterministic reversible token vaults, and cryptographically verified tool execution boundaries. |

---

## 3. Red Flags vs. Green Flags

### 🚩 Red Flags (Signals of Gaps in Experience)
* **Tutorial-Only Knowledge**: Suggests using in-memory ChromaDB/FAISS or naive LangChain scripts for a 200M document enterprise workload.
* **Synchronous Bottlenecks**: Places synchronous, unbatched LLM embedding HTTP calls inside streaming consumer loops without backpressure or circuit breakers.
* **Ignoring Memory Math**: Unable to calculate or estimate the RAM requirements of 100M+ vector embeddings and HNSW graph overhead.
* **Naive ACL Filtering**: Suggests applying naive post-search filtering over vector results without recognizing that the top-$k$ nearest neighbors might all be filtered out.
* **Over-Engineering without Justification**: Introduces overly complex distributed frameworks (e.g., full Ray cluster or custom Raft consensus) where a simple Redis cluster or SQS/Kafka queue would suffice.

### 🟢 Green Flags (Signals of Exceptional Talent)
* **First-Principles Thinking**: Instantly breaks down vector data into dimension count $\times$ byte size $\times$ graph overhead to model memory costs before recommending an engine.
* **Content-Hash Change Detection**: Proactively identifies that 80%+ of database CDC updates are non-semantic (e.g., updated timestamps) and avoids wasted GPU embedding costs.
* **Dual-Writing & Zero-Downtime Migration**: Naturally thinks of blue/green shadow indexing when upgrading embedding models.
* **AST & Data Contracts**: Prefers deterministic SQL AST parsers and schema contracts over trusting LLM string outputs directly.

---

## 4. Interviewer Scorecard Template

Copy and fill out the template below after completing the interview:

```markdown
# Candidate Evaluation Scorecard

- **Candidate Name**: [Candidate Name]
- **Target Role**: Data Engineer (Enterprise AI Agents)
- **Assessed Level**: [ Senior (L4/L5) | Staff (L6+) | No Hire ]
- **Interviewer**: [Interviewer Name]
- **Date**: [Date]

---

### Quantitative Scores (1 = Unsatisfactory, 3 = Meets Bar, 5 = Exceptional)

| Competency Area | Score (1-5) | Key Observations / Evidence |
| :--- | :---: | :--- |
| **1. Distributed Systems & Scale** | | |
| **2. Vector & Context Operations** | | |
| **3. Reliability & Failure Modes** | | |
| **4. Security, Governance & FinOps** | | |
| **5. Practical Coding & Debugging** | | |

---

### Leveling Recommendation & Qualitative Summary

- **Recommendation**: [ Strong Hire (Staff) | Strong Hire (Senior) | Lean Hire | No Hire ]
- **Summary Justification**:
  [Provide 2-3 paragraphs detailing the candidate's strongest architectural decisions, trade-offs made, and any identified gaps.]

- **Key Strengths**:
  - 
  - 

- **Areas for Growth / Follow-Up**:
  - 
  - 
```
