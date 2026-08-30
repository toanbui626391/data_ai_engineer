# Deep-Dive Technical Questions & Failure Modes

This module contains targeted technical deep-dive questions focusing on real-world edge cases, distributed bottlenecks, memory trade-offs, and failure modes in enterprise AI data pipelines.

---

## 1. Vector Storage, Indexing & Memory Math

### Q1: Vector Index Memory Calculation & Storage Architecture
> **Question**: "You need to store and index **500 million embeddings** (1,536 dimensions, `float32`) for an enterprise agent search system.
> 1. Calculate the raw vector memory footprint.
> 2. Calculate the approximate RAM overhead if using an **HNSW** in-memory graph index.
> 3. Compare **HNSW**, **IVF-PQ (Inverted File with Product Quantization)**, and **DiskANN/LanceDB** in terms of RAM requirements, QPS, build time, and recall accuracy. How would you design this cost-effectively?"

#### Leveling Calibration:
* **Senior Engineer (L4/L5)**:
  * Computes raw size correctly: $500\text{M} \times 1,536 \times 4\text{ bytes} \approx 3.072\text{ TB}$.
  * Knows HNSW requires extra memory for the proximity graph (typically $1.5\times$ to $2\times$ raw size $\approx 4.5\text{ TB} - 6\text{ TB}$ RAM).
  * Explains that PQ compresses vectors into compact 8-bit codes to reduce RAM, but sacrifices precision/recall.
* **Staff Engineer (L6+)**:
  * Walks through precise memory math (e.g., $M=16$ to $64$ connections per node $\to$ additional pointer overhead per vector).
  * Analyzes cloud economics: Running $6\text{ TB}$ of RAM across high-memory cloud instances (e.g., AWS `r6i.32xlarge`) costs $\$25,000+/month$.
  * **Architectural Solution**: Proposes hybrid memory-disk architectures (e.g., **DiskANN** or **LanceDB** with NVMe SSDs and memory-mapped files) where compressed vectors or HNSW graph nodes live in RAM while full vectors reside on fast NVMe SSDs, slashing cloud infrastructure costs by 70–80% with $<10\text{ms}$ latency impact.

---

## 2. Streaming Ingestion, CDC & Embedding Backpressure

### Q2: Managing GPU Embedding Bottlenecks under Burst CDC Traffic
> **Question**: "We are streaming 40,000 document change-events per second from an operational Postgres database into Kafka via Debezium. Our downstream embedding cluster (Triton / vLLM on 8x H100 GPUs) can process a maximum of 8,000 embedding requests per second. 
> 
> If a batch database migration suddenly generates a 10x traffic spike, how do you prevent Kafka consumer lag from ballooning, avoid GPU endpoint 429/503 crashes, and ensure urgent customer queries are not blocked?"

#### Leveling Calibration:
* **Senior Engineer (L4/L5)**:
  * Implements exponential backoff with jitter and Dead-Letter Queues (DLQ).
  * Batches text payloads into chunks of 128/256 to maximize GPU tensor core utilization.
  * Scales up consumer worker pods using Kubernetes HPA based on Kafka consumer lag metrics.
* **Staff Engineer (L6+)**:
  * **Content-Hash Deduplication Gate**: Hashes the text payload (`SHA-256`) before sending to GPU; checks a fast Redis cache or Bloom filter. If the text has not changed (e.g., only a metadata timestamp was updated in Postgres), skips GPU embedding entirely.
  * **Prioritized Tiered Queues**: Separates real-time high-priority interactive agent streams from background batch synchronization topics.
  * **Stream Backpressure & Rate Limiting**: Implements token-bucket rate-limiting directly inside the Flink streaming graph with credit-based flow control to dynamically throttle consumption rather than crashing downstream inference endpoints.

---

## 3. Data Governance, Document ACLs & The "Filter-First" Dilemma

### Q3: Document-Level Permissions & Vector Search Degradation
> **Question**: "An enterprise AI agent searches across a vector database where each document has granular Access Control Lists (ACLs) containing lists of allowed user IDs and group IDs. 
> 
> Why does applying strict metadata filtering (`WHERE user_id IN allowed_users`) on a high-cardinality dataset cause **catastrophic recall drop and latency spikes** in traditional HNSW vector graphs? How do you architect a solution around this?"

#### Leveling Calibration:
* **Senior Engineer (L4/L5)**:
  * Explains the difference between **Pre-filtering** (filtering candidates before ANN search) and **Post-filtering** (searching top-1000 vectors then filtering by ACL).
  * Identifies that post-filtering can return zero valid results if the top-$k$ nearest neighbors all belong to restricted documents.
* **Staff Engineer (L6+)**:
  * **Deep HNSW Mechanics**: Explains that in Pre-filtering with HNSW, if 99% of documents are filtered out, the search graph becomes disconnected; the traversal gets trapped in local minima, causing recall to collapse to near zero.
  * **Architectural Solutions**:
    1. *Namespace/Collection Partitioning*: For tenant-level isolation, create dedicated vector namespaces.
    2. *Iterative Deepening & Graph Traversals with Filter-Aware Routing*: Uses vector engines with native Acorn/Vamana filter-aware indexing (e.g., Qdrant's payload indexes or Milvus partition keys).
    3. *Hierarchical ABAC Pruning*: Compresses permissions into bitsets/role hierarchies to execute hardware-accelerated SIMD bitwise filtering prior to vector distance calculations.

---

## 4. Reversible PII Masking & Data Sanitization in Context Pipelines

### Q4: Enterprise Data Sanitization & Deterministic Reversible Tokenization
> **Question**: "When feeding unstructured customer service transcripts into an AI agent data pipeline, we must redact sensitive PII (Social Security Numbers, Credit Cards, Patient Names) before sending data to third-party LLMs. However, the agent's tool-execution step must ultimately update the *correct* customer record in the backend database.
> 
> How do you design a high-throughput, deterministic, reversible PII redaction and token vault pipeline?"

#### Leveling Calibration:
* **Senior Engineer (L4/L5)**:
  * Integrates Microsoft Presidio or spaCy NER in the data pipeline to detect PII entities.
  * Replaces PII with generic tags (e.g., `<PERSON_NAME>`, `<SSN>`) before storing in the vector database or sending to the LLM.
* **Staff Engineer (L6+)**:
  * **Deterministic Token Vault Architecture**:
    * Implements format-preserving surrogate tokens (e.g., `PII_TOKEN_A8F9X`) mapped to encrypted values in an ephemeral, hardware-isolated Token Vault (e.g., HashiCorp Vault / AWS KMS).
    * Embeddings are generated using the masked representation to ensure zero PII leakage into vector databases.
    * When the agent generates a tool call (e.g., `update_record(customer_id="PII_TOKEN_A8F9X", ...)`), an authorized API Gateway intercepts the tool call, securely detokenizes the payload, and executes against the enterprise backend.
    * Implements strict TTLs and cryptographic audit logging on all token-resolution events.

---

## 5. Schema Drift & Contract Enforcement in LLM-Generated Data

### Q5: Managing Schema Drift in LLM-to-Lakehouse Pipelines
> **Question**: "We have autonomous agents that parse unstructured medical records and write structured analytical tables into our Apache Iceberg data lake. Over time, prompt changes or model updates cause subtle schema drifts (e.g., nested JSON keys changing from `diagnosis_code` to `icd10_codes`, or string numbers becoming floats).
> 
> How do you implement robust Data Contracts and automated schema evolution without corrupting downstream analytics tables?"

#### Leveling Calibration:
* **Senior Engineer (L4/L5)**:
  * Uses Pydantic or JSONSchema to validate agent outputs before writing.
  * Employs Iceberg's native schema evolution capabilities (e.g., `ALTER TABLE ADD COLUMN`).
  * Sends invalid records to a quarantine/dead-letter table.
* **Staff Engineer (L6+)**:
  * **End-to-End Data Contract Framework**:
    * Implements versioned data contracts (e.g., using Great Expectations, Soda, or Protobuf schemas).
    * Implements a **Shadow Pipeline & Canary Ingestion**: New agent prompt/model versions write to a shadow Iceberg branch/namespace; automated statistical diffing compares schema consistency and data distribution against production.
    * **Automated Remediation Engine**: Employs structural schema matching algorithms that map drifted keys to the canonical contract, raising alerts only when semantic ambiguity exceeds confidence thresholds.
