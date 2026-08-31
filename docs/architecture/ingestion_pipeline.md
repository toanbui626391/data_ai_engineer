# Real-Time Ingestion Pipeline Architecture

> **Revision:** v2.0 — Validated & corrected from v1.0.  
> **Level:** L6 Staff Engineer Reference Architecture  
> **Throughput Target:** 15,000 CDC document-change events/sec  
> **Objective:** Sub-second vector index freshness with minimal GPU compute cost

---

## Architecture Diagram

```mermaid
flowchart TD
    %% ── Styles ──────────────────────────────────────────────────────────────
    style K_VIP     fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc
    style K_Bulk    fill:#1e293b,stroke:#64748b,stroke-width:1.5px,color:#94a3b8
    style Normalize fill:#1e3a5f,stroke:#60a5fa,stroke-width:2px,color:#eff6ff
    style HashCalc  fill:#3b0764,stroke:#c084fc,stroke-width:2px,color:#faf5ff
    style StateLookup fill:#3b0764,stroke:#c084fc,stroke-width:2px,color:#faf5ff
    style Skip      fill:#451a03,stroke:#fbbf24,stroke-width:2px,color:#fffbeb
    style Controller fill:#064e3b,stroke:#34d399,stroke-width:2px,color:#ecfdf5
    style Triton    fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc
    style V_Active  fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc
    style V_Shadow  fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc
    style Iceberg   fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc
    style Retry     fill:#451a03,stroke:#fbbf24,stroke-width:2px,color:#fffbeb
    style DLQ       fill:#450a0a,stroke:#f87171,stroke-width:2px,color:#fef2f2
    style RocksDB   fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc
    style Metrics   fill:#0c4a6e,stroke:#38bdf8,stroke-width:1.5px,color:#e0f2fe

    %% ── Tier 1: Dual-Priority Kafka Ingestion ───────────────────────────────
    subgraph Ingestion ["Kafka CDC Ingestion Tier (15,000 events/sec)"]
        K_VIP["🔴 High-Priority VIP Stream<br/>(Live Agent Chats / Jira Tickets)"]
        K_Bulk["⚪ Bulk Background Stream<br/>(Batch DB Ingestion / Confluence)"]
    end

    %% ── Tier 2: Deduplication Gate ──────────────────────────────────────────
    subgraph Deduplication ["3-Tier Deduplication Gate (Zero-Waste)"]
        Normalize["① Normalize Text<br/>(Unicode NFC · strip whitespace · decode HTML)"]
        HashCalc["② Compute xxHash64<br/>(model_id + canonical_text)"]
        StateLookup{"③ Hash matches<br/>Flink RocksDB Keyed State?<br/>Key: (model_id, doc_id)"}
        Skip[("✅ Skip GPU Inference<br/>Update Metadata Only")]
    end

    %% ── Tier 3: Stream Processing & Flow Control ─────────────────────────────
    subgraph StreamProcessing ["Stream Processing & Flow Control"]
        Controller["Apache Flink Flow Controller<br/>(Credit-Based Backpressure Engine)<br/>Monitors GPU Queue Depth"]
    end

    %% ── Tier 4: GPU Inference ────────────────────────────────────────────────
    subgraph Inference ["High-Density GPU Inference"]
        Triton["Dynamic Tensor Batching Engine<br/>(Triton / vLLM · H100 / L40S Pool)<br/>Micro-batches: 128–256 docs"]
    end

    %% ── Tier 5: Storage ──────────────────────────────────────────────────────
    subgraph Storage ["Storage & Indexing Tier"]
        V_Active[("Primary Active Vector Index<br/>(Qdrant / Milvus / LanceDB)")]
        V_Shadow[("Shadow Index v2<br/>(Dual-Write for Zero-Downtime Upgrades)")]
        Iceberg[("Apache Iceberg<br/>Bronze / Silver Lakehouse")]
    end

    %% ── Resilience Layer ─────────────────────────────────────────────────────
    subgraph Reliability ["Resilience Layer"]
        Retry["Exponential Backoff + Jitter"]
        DLQ["Kafka Dead-Letter Queue (DLQ)"]
        RocksDB[("RocksDB State Store<br/>Checkpointed → S3 / GCS<br/>Covers: Dedup State + Flow State")]
    end

    %% ── Observability ────────────────────────────────────────────────────────
    subgraph Observability ["Observability & FinOps"]
        Metrics["Prometheus / Grafana<br/>· Consumer Lag per Partition<br/>· Dedup Hit Rate (saved GPU %)<br/>· GPU Queue Depth<br/>· DLQ Depth Alert"]
    end

    %% ── Connections ──────────────────────────────────────────────────────────

    %% Sources → Priority Topics
    Jira[(Jira)]    -.->|"CDC"| K_VIP
    Conf[(Confluence)] -.->|"CDC"| K_Bulk
    ERP[(ERP)]      -.->|"CDC"| K_Bulk

    %% Ingestion → Deduplication
    K_VIP  --> Normalize
    K_Bulk --> Normalize
    Normalize --> HashCalc
    HashCalc --> StateLookup

    %% Dedup gate decision
    StateLookup -->|"✅ Hash Unchanged"| Skip
    StateLookup -->|"❌ Hash Modified / New Doc"| Controller

    %% Flow Control → GPU → Storage
    Controller --> Triton
    Triton --> V_Active
    Triton --> V_Shadow

    %% Unchanged events → Lakehouse (metadata audit trail)
    Skip --> Iceberg

    %% Resilience paths
    Triton  -.->|"429 / 503 Failures"| Retry
    Retry   -.->|"Max Retries Exceeded"| DLQ

    %% State checkpointing covers BOTH dedup gate AND flow controller
    StateLookup -.->|"Checkpoint Dedup State"| RocksDB
    Controller  -.->|"Checkpoint Flow State"| RocksDB

    %% Observability taps
    K_VIP       -.->|"Lag Metrics"| Metrics
    K_Bulk      -.->|"Lag Metrics"| Metrics
    StateLookup -.->|"Hit Rate Counter"| Metrics
    Triton      -.->|"Queue Depth"| Metrics
    DLQ         -.->|"Depth Alert"| Metrics
```

---

## Key Design Decisions

### 1. Dual Priority Kafka Topics
Separates real-time VIP traffic (live agent queries, Jira ticket updates) from bulk background ingestion (full Confluence re-index). This ensures interactive agent flows are **never queued behind bulk sync jobs**, regardless of throughput spikes.

### 2. 3-Step Deduplication Gate

| Step | What It Does | Why |
|------|-------------|-----|
| **① Normalize** | Unicode NFC, strip `\r\n`/trailing whitespace, decode HTML entities | Prevents false cache misses from trivial formatting differences (Windows vs. Unix editors, Confluence autosave artifacts) |
| **② xxHash64** | Hash `(model_id + canonical_text)` | xxHash64 runs at ~10–15 GB/s/core vs. SHA-256 at ~400 MB/s/core — **25x faster** with negligible collision risk for change detection |
| **③ RocksDB State Lookup** | Key: `(model_id, doc_id)` → `latest_hash` | Exact KV lookup, no false positives. Embedding model version encoded in key ensures all docs are re-embedded on model upgrades |

> **Why NOT a Redis Bloom Filter:**  
> Bloom filters have **false positives** — incorrectly marking a changed document as unchanged, silently leaving the Vector DB stale. They also **cannot update or delete keys**, making them unsuitable for tracking evolving document content.

> **Why NOT SHA-256:**  
> SHA-256 is a cryptographic hash providing collision resistance against adversarial inputs — overkill for accidental change detection and ~25x slower than xxHash64 at this throughput.

### 3. RocksDB Checkpointing Covers Both States
Both the **deduplication gate state** (`doc_id → hash`) and the **flow controller state** checkpoint to RocksDB, which is flushed to durable object storage (S3/GCS). This guarantees:
- No stale dedup state after a Flink worker restart (prevents spurious re-embeddings)
- No double-writes to the Vector DB after recovery

### 4. Model Version in Hash Key
The composite state key `(model_id, doc_id) → hash` ensures that when the embedding model is upgraded, **all existing hash entries are automatically invalidated** — triggering a clean re-embedding pass without manual state flushing or cache poisoning.

### 5. Credit-Based Backpressure
Flink's flow controller monitors GPU queue depth via metrics. When the Triton queue exceeds a high-water mark, the controller reduces the credit budget issued to upstream operators, naturally slowing Kafka consumption **without dropping events or causing OOM crashes**.

### 6. Zero-Downtime Dual-Write & Shadow Convergence
During model upgrades:
1. New inference cluster writes to **Shadow Index v2** in real time.
2. A backfill job migrates remaining documents from the old index.
3. Once shadow index lag reaches zero, the **active pointer is atomically swapped** — zero downtime for agent queries.

### 7. Observability & FinOps
| Metric | Purpose |
|--------|---------|
| Kafka Consumer Lag / Partition | Detect ingestion bottlenecks early |
| Dedup Hit Rate | Real-time GPU cost savings measurement |
| GPU Queue Depth | Drives dynamic backpressure signal |
| DLQ Depth Alert | Surfaces systematic embedding failures |

---

## FinOps Cost Impact

| Level | GPU Requests / Sec | Required Hardware | Estimated Cost/Month |
|-------|--------------------|-------------------|-----------------------|
| L4 (No dedup, synchronous) | 15,000/sec (unbatched) | System crashes | N/A (Outage) |
| L5 (Batched, no dedup) | 15,000/sec (batched) | ~16× H100 | ~$45,000 |
| **L6 (This architecture)** | **~3,000/sec (deduplicated)** | **~4× H100** | **~$11,000** |

> **Annual saving vs. L5:** ~$408,000/year in GPU compute alone.
