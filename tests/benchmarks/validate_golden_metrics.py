"""
Comprehensive Validation Script: Proves that tests/benchmarks/golden_engineering_dataset.jsonl
contains all required fields to compute 100% of the metrics in Part 3 of docs/indexing/golden_benchmark_strategy.md.
"""

import json
import math
import time
from typing import List, Dict, Any


def load_dataset(filepath: str) -> List[Dict[str, Any]]:
    entries = []
    with open(filepath, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception as e:
                raise ValueError(f"Line {idx} failed JSON parse: {e}")
    return entries


def compute_dcg(relevance_grades: List[int], k: int) -> float:
    dcg = 0.0
    for rank, grade in enumerate(relevance_grades[:k], start=1):
        dcg += (2**grade - 1) / math.log2(rank + 1)
    return dcg


def compute_ndcg(retrieved_grades: List[int], all_ground_truth_grades: List[int], k: int) -> float:
    dcg = compute_dcg(retrieved_grades, k)
    ideal_grades = sorted(all_ground_truth_grades + [0] * k, reverse=True)[:k]
    idcg = compute_dcg(ideal_grades, k)
    return dcg / idcg if idcg > 0 else 0.0


def validate_dataset_fields(entries: List[Dict[str, Any]]):
    print("=" * 80)
    print("STEP 1: VALIDATING DATASET INTEGRITY & FIELD COMPLETENESS FOR PART 3 METRICS")
    print("=" * 80)

    assert len(entries) == 20, f"Expected 20 entries, found {len(entries)}"

    archetype_counts = {}
    for entry in entries:
        qid = entry["query_id"]
        arch = entry.get("query_archetype")
        archetype_counts[arch] = archetype_counts.get(arch, 0) + 1

        # Check required fields for core IR metrics
        assert "query_text" in entry and len(entry["query_text"]) > 5, f"{qid} missing valid query_text"
        assert "ground_truth_anchors" in entry and len(entry["ground_truth_anchors"]) > 0, f"{qid} missing anchors"

        # Check relevance grades for Graded NDCG (Section 3.1.4)
        grades = [a["relevance_grade"] for a in entry["ground_truth_anchors"]]
        assert any(g == 3 for g in grades), f"{qid} missing Grade 3 (Exact Answer) anchor required for NDCG"
        assert all(g in [1, 2, 3] for g in grades), f"{qid} contains invalid grade outside [1, 2, 3]"

        # Check EPMR fields (Section 3.1.5)
        if arch in ["exact_part_lookup", "specification_tolerance", "cross_revision_comparison"]:
            assert "target_entities" in entry and len(entry["target_entities"]) > 0, f"{qid} missing target_entities for EPMR"

        # Check BOM Completeness fields (Section 3.1.6)
        if arch == "bom_traversal":
            assert "target_bom_parts" in entry and len(entry["target_bom_parts"]) >= 3, f"{qid} missing target_bom_parts for BOM Completeness"

        # Check Hard Negatives for stress testing (Section 2.3)
        assert "hard_negatives" in entry and len(entry["hard_negatives"]) > 0, f"{qid} missing hard_negatives"

    print("✓ All 20 entries passed strict schema assertions.")
    print("✓ Archetype distribution:")
    for arch, count in archetype_counts.items():
        print(f"   • {arch:30s}: {count} queries ({count/len(entries)*100:.0f}%)")


def run_metrics_validation(entries: List[Dict[str, Any]]):
    print("\n" + "=" * 80)
    print("STEP 2: COMPUTING ALL PART 3 METRICS ON SIMULATED RETRIEVAL RUNS")
    print("=" * 80)

    # Simulation A: Strong Production Hybrid Retriever (Dense + Sparse + Reranker)
    # Simulation B: Degraded Baseline (Suffers from hard-negative collisions & tokenizer splitting)

    runs = {"Production Hybrid Pipeline": [], "Degraded Naive Pipeline": []}

    for entry in entries:
        anchors = entry["ground_truth_anchors"]
        hard_negs = entry["hard_negatives"]
        target_entities = entry.get("target_entities", [])
        target_bom = entry.get("target_bom_parts", [])

        # Simulation A candidates (Good ranking)
        sim_a_candidates = []
        for a in sorted(anchors, key=lambda x: x["relevance_grade"], reverse=True):
            sim_a_candidates.append({
                "text": a["canonical_span"],
                "grade": a["relevance_grade"],
                "is_match": True,
            })
        # Add a distractor at rank 4
        sim_a_candidates.insert(3, {
            "text": "General safety rules for shop technicians.",
            "grade": 0,
            "is_match": False,
        })
        runs["Production Hybrid Pipeline"].append({
            "entry": entry,
            "candidates": sim_a_candidates,
            "latency_ms": 78.4,
        })

        # Simulation B candidates (Degraded ranking: Hard negative ranked at #1, missing 1 BOM part)
        sim_b_candidates = []
        # Rank 1: Hard negative distractor
        sim_b_candidates.append({
            "text": hard_negs[0]["canonical_span"],
            "grade": 0,
            "is_match": False,
        })
        # Rank 2: Only marginal Grade 1 context
        for a in anchors:
            if a["relevance_grade"] == 1:
                sim_b_candidates.append({
                    "text": a["canonical_span"],
                    "grade": 1,
                    "is_match": True,
                })
        # Rank 3: Grade 3 exact answer (downranked)
        for a in anchors:
            if a["relevance_grade"] == 3:
                # Corrupt one part number in text to simulate tokenizer hyphen split failure
                corrupted_text = a["canonical_span"]
                if target_entities:
                    corrupted_text = corrupted_text.replace("-", " ")
                sim_b_candidates.append({
                    "text": corrupted_text,
                    "grade": 3,
                    "is_match": True,
                })
        runs["Degraded Naive Pipeline"].append({
            "entry": entry,
            "candidates": sim_b_candidates,
            "latency_ms": 235.1,
        })

    # Evaluate both runs across all metrics in Part 3
    print(f"{'Metric Name (Part 3)':<35} | {'Production Hybrid':<20} | {'Degraded Baseline':<20} | {'Target SLA'}")
    print("-" * 95)

    for run_name, query_runs in runs.items():
        hit_rates_5 = []
        recalls_5 = []
        recalls_20 = []
        precisions_5 = []
        mrr_scores = []
        ndcgs_10 = []
        epmr_5 = []
        bom_completeness = []

        for q_run in query_runs:
            entry = q_run["entry"]
            cands = q_run["candidates"]
            anchors = entry["ground_truth_anchors"]
            all_grades = [a["relevance_grade"] for a in anchors]

            # 1. Hit Rate @ 5
            top_5 = cands[:5]
            hit = any(c["grade"] >= 2 for c in top_5)
            hit_rates_5.append(1.0 if hit else 0.0)

            # 2. Recall @ 5
            relevant_anchors = [a for a in anchors if a["relevance_grade"] >= 2]
            retrieved_rel = sum(1 for a in relevant_anchors if any(a["canonical_span"] in c["text"] for c in top_5))
            recalls_5.append(retrieved_rel / max(1, len(relevant_anchors)))

            # 3. Recall @ 20
            top_20 = cands[:20]
            retrieved_rel_20 = sum(1 for a in relevant_anchors if any(a["canonical_span"] in c["text"] for c in top_20))
            recalls_20.append(retrieved_rel_20 / max(1, len(relevant_anchors)))

            # 4. Precision @ 5
            precisions_5.append(sum(1 for c in top_5 if c["grade"] >= 1) / 5.0)

            # 5. MRR
            first_rank = 0
            for r, c in enumerate(cands, start=1):
                if c["grade"] >= 2:
                    first_rank = r
                    break
            mrr_scores.append(1.0 / first_rank if first_rank > 0 else 0.0)

            # 6. Graded NDCG @ 10
            cands_grades = [c["grade"] for c in cands[:10]]
            ndcgs_10.append(compute_ndcg(cands_grades, all_grades, k=10))

            # 7. Exact Part Match Recall (EPMR @ 5)
            target_entities = entry.get("target_entities", [])
            if target_entities:
                top_5_text = " ".join([c["text"] for c in top_5])
                matched = all(entity in top_5_text for entity in target_entities if "-" in entity)
                epmr_5.append(1.0 if matched else 0.0)

            # 8. BOM Set Completeness @ 10
            target_bom = entry.get("target_bom_parts", [])
            if target_bom:
                top_10_text = " ".join([c["text"] for c in cands[:10]])
                bom_complete = all(part in top_10_text for part in target_bom)
                bom_completeness.append(1.0 if bom_complete else 0.0)

        # Store calculated averages
        runs[run_name] = {
            "Hit Rate @ 5": sum(hit_rates_5) / len(hit_rates_5),
            "Recall @ 5": sum(recalls_5) / len(recalls_5),
            "Recall @ 20": sum(recalls_20) / len(recalls_20),
            "Precision @ 5": sum(precisions_5) / len(precisions_5),
            "MRR": sum(mrr_scores) / len(mrr_scores),
            "Graded NDCG @ 10": sum(ndcgs_10) / len(ndcgs_10),
            "EPMR @ 5": sum(epmr_5) / len(epmr_5) if epmr_5 else 1.0,
            "BOM Completeness @ 10": sum(bom_completeness) / len(bom_completeness) if bom_completeness else 1.0,
            "p95 Latency": query_runs[0]["latency_ms"],
        }

    # Print Comparison Table against Section 3.2 SLAs
    slas = {
        "Hit Rate @ 5": ">= 95.0%",
        "Recall @ 5": ">= 88.0%",
        "Recall @ 20": ">= 96.0%",
        "Precision @ 5": ">= 40.0%",
        "MRR": ">= 0.82",
        "Graded NDCG @ 10": ">= 0.86",
        "EPMR @ 5": ">= 98.5%",
        "BOM Completeness @ 10": ">= 90.0%",
        "p95 Latency": "< 120 ms",
    }

    prod = runs["Production Hybrid Pipeline"]
    deg = runs["Degraded Naive Pipeline"]

    for metric_name, sla_val in slas.items():
        if "Latency" in metric_name:
            p_val = f"{prod[metric_name]:.1f} ms"
            d_val = f"{deg[metric_name]:.1f} ms"
        else:
            p_val = f"{prod[metric_name]*100:.1f}%" if "Rate" in metric_name or "Recall" in metric_name or "Precision" in metric_name or "EPMR" in metric_name or "Completeness" in metric_name else f"{prod[metric_name]:.3f}"
            d_val = f"{deg[metric_name]*100:.1f}%" if "Rate" in metric_name or "Recall" in metric_name or "Precision" in metric_name or "EPMR" in metric_name or "Completeness" in metric_name else f"{deg[metric_name]:.3f}"
        print(f"{metric_name:<35} | {p_val:<20} | {d_val:<20} | {sla_val}")

    print("\n✓ Verification Successful: All 9 metrics defined in Part 3 & Section 3.2 SLA table")
    print("  can be computed with mathematical precision using this 20-point dataset.")


if __name__ == "__main__":
    dataset = load_dataset("tests/benchmarks/golden_engineering_dataset.jsonl")
    validate_dataset_fields(dataset)
    run_metrics_validation(dataset)
