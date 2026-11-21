# OpenSearch Data Modeling: Bad vs. Good Patterns

Top 5 common schema anti-patterns in OpenSearch and how to remodel them for scale. (For large-scale RAG data modeling specifically, see [opensearch_large_scale_rag.md](./opensearch_large_scale_rag.md).)

---

## 1. Dynamic Keys as Field Names ("Mapping Explosion")

Every unique JSON key adds a permanent field to cluster state. Putting dynamic IDs, timestamps, or user-defined tags as keys slows the Cluster Manager and hits `index.mapping.total_fields.limit` (default `1000`).

```json
// BAD: Unbounded dynamic keys create thousands of mapping fields
{
  "doc_id": "101",
  "custom_labels": {
    "customer_8841": "vip",
    "project_alpha_2026": "active"
  }
}

// GOOD: Fixed schema using key:value keyword tokens (1 mapping field)
{
  "doc_id": "101",
  "custom_labels": ["customer_8841:vip", "project_alpha_2026:active"]
}
```

| Dynamic-Key Pattern | Best When | Trade-off |
|---|---|---|
| **`"k:v"` `keyword` array** | Exact `key:value` tag filtering (labels, ACL tokens). Fastest & lowest memory. | No numeric/date range queries (`value > 100`). |
| **`flat_object` type** | Arbitrary nested JSON you cannot reshape; exact/prefix lookups only. | Treats values as strings; no numeric ranges or full-text analyzers. |
| **`nested` `[{"k": "...", "v_num": 10}]`** | Typed/range queries on dynamic attributes (`k == "price" AND v_num > 50`). | Higher memory and query cost (hidden Lucene docs). |

---

## 2. Blind Default Dynamic Mapping vs. Explicit Purpose-Driven Mapping

Default dynamic mapping indexes every string as *both* analyzed `text` and `.keyword`. IDs get needlessly tokenized (`"ERR-404"` -> `"err"`, `"404"`, breaking `term` filters), while display-only blobs (`raw_html`, URLs) waste inverted index and disk space.

```json
// BAD: Default dynamic mapping on IDs and display payloads
"properties": {
  "tenant_id": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
  "raw_html":  { "type": "text", "fields": { "keyword": { "type": "keyword" } } }
}

// GOOD: Explicit types ("dynamic": "strict") matched to access patterns
"dynamic": "strict",
"properties": {
  "tenant_id": { "type": "keyword" },
  "body_text": { "type": "text" },
  "raw_html":  { "type": "text", "index": false }
}
```
- **Rule**: Use `"dynamic": "strict"`. Map IDs/filters/facets as `keyword`, searchable prose as `text`, and display-only payloads with `"index": false`.

---

## 3. Array-of-Objects Flattening Trap vs. Composite Tokens / `nested`

Standard `object` arrays lose element boundaries when Lucene flattens them. Querying `dept == "hr" AND role == "admin"` **falsely matches** the bad document below because both values exist across separate elements.

```json
// BAD: Flattens into dept:["hr","eng"] and role:["viewer","admin"]
{
  "permissions": [
    { "dept": "hr",  "role": "viewer" },
    { "dept": "eng", "role": "admin" }
  ]
}

// GOOD (Option A - Fastest): Composite keyword tokens for exact filters
{
  "permissions": ["hr:viewer", "eng:admin"]
}

// GOOD (Option B - Rich multi-field queries): Explicit "nested" mapping
// Mapping: "permissions": { "type": "nested" }
```
- **Rule**: Use composite `keyword` strings when exact matching suffices, or `"type": "nested"` when correlated multi-field queries are required.

---

## 4. Relational Normalization (`join`) vs. Denormalization

Splitting parent and child entities across indices or overusing `join` fields forces multi-hop application lookups or heap-heavy `has_parent` queries.

```json
// BAD: Normalized chunk requiring a second lookup for permissions and title
{
  "chunk_id": "c_99",
  "parent_doc_id": "doc_42",
  "chunk_text": "Reset the token in settings."
}

// GOOD: Denormalize small, frequently filtered/displayed parent attributes
{
  "chunk_id": "doc_42#3",
  "doc_id": "doc_42",
  "tenant_id": "acme",
  "acl_principals": ["group:support"],
  "doc_title": "SSO Troubleshooting Guide",
  "chunk_text": "Reset the token in settings."
}
```
- **Rule**: Duplicate compact, read-heavy parent metadata (`tenant_id`, `acl_principals`, `doc_title`) onto child documents for single-pass retrieval.

---

## 5. Monolithic Document with Embedded Vectors vs. Granular Routed Documents

Packing hundreds of chunks and `float32` vectors into one document (and keeping vectors inside `_source`) means editing one line re-indexes every HNSW vector and every search hit transfers megabytes of float arrays.

```json
// BAD: 1 giant document per file + raw float32 vectors stored inside _source
{
  "doc_id": "manual_1",
  "chunks": [
    { "text": "...", "vector": [0.012, -0.044, "... 1536 floats ..."] }
  ]
}

// GOOD: 1 doc = 1 chunk, routed by tenant_id, vector excluded from _source
// Settings: "_routing": { "required": true }, "_source": { "excludes": ["chunk_embedding"] }
{
  "tenant_id": "acme",
  "doc_id": "manual_1",
  "chunk_index": 0,
  "chunk_text": "...",
  "chunk_embedding": [0.012, -0.044, "..."]
}
```
- **Rule**: Model at the **unit of retrieval** (1 doc = 1 chunk/event), exclude `knn_vector` from `_source`, quantize vectors, and route by `tenant_id`.
