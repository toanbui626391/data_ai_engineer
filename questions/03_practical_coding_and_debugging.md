# Practical Coding & Debugging Challenges

This module provides practical coding, pipeline implementation, and live debugging challenges suitable for a **45–60 minute technical interview**.

---

## Challenge 1: AST SQL Guardrail & Partition Filter Enforcer (Python)

### Problem Description
You are building the execution sandbox for an enterprise **Text-to-SQL AI Agent**. The LLM produces raw SQL strings based on user prompts. Before sending the query to the Snowflake/BigQuery data warehouse:
1. The query must be strictly **Read-Only** (reject `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `GRANT`, `MERGE`, etc.).
2. The query must **enforce partition pruning**: If the query touches `analytics_db.fact_transactions` (partitioned by `transaction_date`), it MUST contain a `WHERE` clause filtering on `transaction_date`.
3. If no `LIMIT` is specified, inject `LIMIT 1000` at the outermost query level without corrupting nested subqueries or CTEs.

### Candidate Starter Code
```python
from typing import Tuple
import sqlglot
from sqlglot import exp, parse_one

class SQLAgentGuardrail:
    def __init__(self, partitioned_tables: dict[str, str]):
        """
        partitioned_tables maps table_name -> required_partition_column
        e.g., {"fact_transactions": "transaction_date"}
        """
        self.partitioned_tables = partitioned_tables

    def validate_and_sanitize(self, sql_query: str) -> Tuple[bool, str, str]:
        """
        Returns:
            (is_valid: bool, sanitized_sql: str, error_message: str)
        """
        # TODO: Implement validation & AST modification
        pass
```

### Expected Solution (Reference Implementation)
```python
from typing import Tuple
import sqlglot
from sqlglot import exp, parse_one

class SQLAgentGuardrail:
    def __init__(self, partitioned_tables: dict[str, str]):
        self.partitioned_tables = partitioned_tables
        self.forbidden_expressions = (
            exp.Drop, exp.Delete, exp.Update, exp.Insert, 
            exp.Alter, exp.Command, exp.Create
        )

    def validate_and_sanitize(self, sql_query: str) -> Tuple[bool, str, str]:
        try:
            expression = parse_one(sql_query, read="snowflake")
        except Exception as e:
            return False, "", f"SQL Syntax Error: {str(e)}"

        # 1. Check for DML / DDL operations
        if any(expression.find(forbidden) for forbidden in self.forbidden_expressions):
            return False, "", "Security Violation: Non-read-only query detected."

        # 2. Check for Partition Column presence when target table is queried
        tables_found = [table.name.lower() for table in expression.find_all(exp.Table)]
        for table_name, partition_col in self.partitioned_tables.items():
            if table_name.lower() in tables_found:
                # Find all column references in WHERE clauses
                where_clauses = expression.find_all(exp.Where)
                has_partition_filter = False
                for where in where_clauses:
                    columns = [c.name.lower() for c in where.find_all(exp.Column)]
                    if partition_col.lower() in columns:
                        has_partition_filter = True
                        break
                
                if not has_partition_filter:
                    return False, "", f"Cost Violation: Query on '{table_name}' must filter on partition column '{partition_col}'."

        # 3. Enforce outer LIMIT 1000 if not present
        if not expression.args.get("limit"):
            expression = expression.limit(1000)

        return True, expression.sql(dialect="snowflake"), ""
```

#### Senior vs. Staff Leveling:
* **Senior Benchmark**: Uses regex or basic AST parsing; successfully implements read-only checks and limit appending; handles standard `SELECT * FROM table WHERE date = ...`.
* **Staff Benchmark**: Recognizes AST edge cases (e.g., tables alias resolution, CTEs `WITH ... AS (...)`, nested subquery limits vs outer limits, SQL dialect differences between BigQuery and Snowflake).

---

## Challenge 2: Live Debugging — Broken Kafka Embedding Consumer with Deadlock & Memory Leak

### The Scenario
Give the candidate the following Python snippet. It represents a streaming worker that consumes text changes from Kafka, calls a remote embedding API, and writes vectors to a Vector DB.

**The code has 4 critical production bugs / performance bottlenecks.** Ask the candidate to identify, explain, and fix them.

### Buggy Code Snippet
```python
import time
import requests
from kafka import KafkaConsumer
import numpy as np

class VectorSyncWorker:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'document_updates',
            bootstrap_servers=['localhost:9092'],
            group_id='vector_sync_group',
            enable_auto_commit=True,  # BUG 1
            auto_commit_interval_ms=1000
        )
        self.embedding_url = "http://internal-llm-service/v1/embeddings"
        self.memory_buffer = []  # BUG 2

    def get_embedding(self, text: str):
        # BUG 3: Unbatched synchronous HTTP call with no timeout or retry
        resp = requests.post(self.embedding_url, json={"input": text})
        return resp.json()['data'][0]['embedding']

    def run(self):
        for message in self.consumer:
            doc = message.value.decode('utf-8')
            
            # Append to buffer for analytics
            self.memory_buffer.append(doc)
            
            # Compute embedding
            vec = self.get_embedding(doc)
            
            # Write to vector database (simulated)
            self.upsert_to_vector_db(message.key, vec)
            
    def upsert_to_vector_db(self, key, vector):
        pass
```

### Bugs to Identify & Candidate Calibration

| Bug # | The Issue | Senior Level Fix | Staff Level Fix |
| :--- | :--- | :--- | :--- |
| **Bug 1** | `enable_auto_commit=True` causes **data loss or duplicate processing** if the worker crashes before vectors are persisted. | Disables auto-commit; manually commits Kafka offsets *after* vector store batch write succeeds. | Implements **at-least-once with idempotent upserts** in the vector store; manages commit offsets in alignment with transactional write batches. |
| **Bug 2** | `self.memory_buffer` grows indefinitely in memory $\to$ **Out-of-Memory (OOM) Crash** under high throughput. | Removes unbounded buffer or adds fixed-size circular queue (`collections.deque(maxlen=1000)`). | Implements a structured telemetry pipeline with periodic flush to analytical storage (S3/Iceberg) with backpressure limits. |
| **Bug 3** | **Unbatched 1-by-1 synchronous HTTP call** without timeout $\to$ extreme network overhead and thread blocking on downstream hiccups. | Adds `timeout=5.0`, retry logic, and micro-batches texts (e.g., 64 at a time) to the embedding API. | Implements `asyncio` / `aiohttp` connection pooling, dynamic batch windowing (e.g., flush every 100ms or 128 docs), and circuit breaker pattern. |
| **Bug 4 (Architectural)** | Re-embeds document even if only metadata or non-text fields changed in the CDC event. | Checks if text is not empty. | Computes SHA-256 hash of text content; compares against local cache/bloom filter to **skip redundant GPU inference completely**. |

---

## Challenge 3: Distributed Document Chunking with Parent-Child Relationships

### Problem Description
Write a distributed PySpark or Ray UDF function that takes a DataFrame of long enterprise Markdown documents and produces:
1. **Child Chunks** (200 tokens) for granular vector similarity search.
2. **Parent Chunks** (1,000 tokens) containing the surrounding context.
3. Metadata containing document breadcrumb headers (e.g., `# Section 1 > ## Subsection A`).

### What to Look For:
* **Senior**: Clean Python code, handles text splitting, attaches metadata dict with parent ID and child ID.
* **Staff**: Considers tokenization discrepancies (word split vs tiktoken/BPE tokens), avoids PySpark driver memory bottleneck by avoiding `collect()`, and structures clean partition keys for vector store sharding.
