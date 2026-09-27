"""
Reference Implementation: Benchmark Metric Calculator
Calculates all Part 3 Quantitative Information Retrieval (IR) Metrics
against tests/benchmarks/golden_engineering_dataset.jsonl.
"""

import json
import math
from dataclasses import dataclass
from typing import List, Dict, Set


@dataclass
class RetrievedCandidate:
    chunk_id: str
    text: str
    metadata: Dict


@dataclass
class EvaluationMetrics:
    hit_rate_at_5: float
    recall_at_5: float
    precision_at_5: float
    mrr: float
    ndcg_at_10: float
    epmr_at_5: float
    bom_completeness_at_10: float


class EngineeringBenchmarkEvaluator:
    def __init__(self, golden_dataset_path: str):
        self.entries = []
        with open(golden_dataset_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    self.entries.append(json.loads(line))

    @staticmethod
    def _is_span_match(candidate_text: str, anchor: Dict) -> bool:
        """Dynamic span matcher: checks canonical substring or text containment."""
        canonical_span = anchor["canonical_span"].strip()
        if canonical_span in candidate_text:
            return True
        # Token intersection fallback (>80% overlap)
        cand_tokens = set(candidate_text.lower().split())
        gt_tokens = set(canonical_span.lower().split())
        if not gt_tokens:
            return False
        return (len(cand_tokens.intersection(gt_tokens)) / len(gt_tokens)) >= 0.80

    def evaluate_retrieval_run(self, retrieval_results: Dict[str, List[RetrievedCandidate]]) -> EvaluationMetrics:
        """
        Computes the complete suite of Part 3 Quantitative IR Metrics.
        retrieval_results maps query_id -> List[RetrievedCandidate]
        """
        hit_rates_5 = []
        recalls_5 = []
        precisions_5 = []
        mrr_scores = []
        ndcg_10_scores = []
        epmr_hits = []
        bom_hits = []

        for entry in self.entries:
            query_id = entry["query_id"]
            candidates = retrieval_results.get(query_id, [])
            anchors = entry["ground_truth_anchors"]
            target_entities = entry.get("target_entities", [])
            target_bom_parts = entry.get("target_bom_parts", [])

            # 1. Binary Hit Rate @ 5
            top_5 = candidates[:5]
            hit = any(
                any(self._is_span_match(cand.text, a) for a in anchors if a["relevance_grade"] >= 2)
                for cand in top_5
            )
            hit_rates_5.append(1.0 if hit else 0.0)

            # 2. Recall @ 5
            total_relevant_anchors = [a for a in anchors if a["relevance_grade"] >= 2]
            retrieved_relevant_count = 0
            for a in total_relevant_anchors:
                if any(self._is_span_match(cand.text, a) for cand in top_5):
                    retrieved_relevant_count += 1
            recall = retrieved_relevant_count / max(1, len(total_relevant_anchors))
            recalls_5.append(recall)

            # 3. Precision @ 5
            relevant_retrieved_in_top_5 = 0
            for cand in top_5:
                if any(self._is_span_match(cand.text, a) for a in anchors if a["relevance_grade"] >= 1):
                    relevant_retrieved_in_top_5 += 1
            precisions_5.append(relevant_retrieved_in_top_5 / max(1, len(top_5)))

            # 4. Mean Reciprocal Rank (MRR)
            first_rank = 0
            for rank, cand in enumerate(candidates, start=1):
                if any(self._is_span_match(cand.text, a) for a in anchors if a["relevance_grade"] >= 2):
                    first_rank = rank
                    break
            mrr_scores.append(1.0 / first_rank if first_rank > 0 else 0.0)

            # 5. Graded NDCG @ 10 (0=Irrelevant, 1=Marginal, 2=Context, 3=Exact)
            top_10 = candidates[:10]
            dcg = 0.0
            for rank, cand in enumerate(top_10, start=1):
                grade = 0
                for a in anchors:
                    if self._is_span_match(cand.text, a):
                        grade = max(grade, a["relevance_grade"])
                dcg += (2**grade - 1) / math.log2(rank + 1)

            # Compute Ideal DCG (IDCG)
            all_grades = sorted([a["relevance_grade"] for a in anchors] + [0] * 10, reverse=True)[:10]
            idcg = sum((2**g - 1) / math.log2(idx + 1) for idx, g in enumerate(all_grades, start=1))
            ndcg_10_scores.append(dcg / max(1e-6, idcg))

            # 6. Exact Part Match Recall (EPMR @ 5)
            if target_entities:
                top_5_combined_text = " ".join([c.text for c in top_5])
                # Check for alphanumeric part/code matches
                part_match = all(entity in top_5_combined_text for entity in target_entities if "-" in entity or " " in entity)
                epmr_hits.append(1.0 if part_match else 0.0)

            # 7. BOM Set Completeness Rate @ 10 (All-or-Nothing Recall)
            if target_bom_parts:
                top_10_combined_text = " ".join([c.text for c in top_10])
                all_parts_found = all(part in top_10_combined_text for part in target_bom_parts)
                bom_hits.append(1.0 if all_parts_found else 0.0)

        def mean(lst):
            return sum(lst) / len(lst) if lst else 0.0

        return EvaluationMetrics(
            hit_rate_at_5=mean(hit_rates_5),
            recall_at_5=mean(recalls_5),
            precision_at_5=mean(precisions_5),
            mrr=mean(mrr_scores),
            ndcg_at_10=mean(ndcg_10_scores),
            epmr_at_5=mean(epmr_hits) if epmr_hits else 1.0,
            bom_completeness_at_10=mean(bom_hits) if bom_hits else 1.0,
        )


if __name__ == "__main__":
    evaluator = EngineeringBenchmarkEvaluator("tests/benchmarks/golden_engineering_dataset.jsonl")
    print(f"Loaded {len(evaluator.entries)} benchmark queries.")
    print("Ready to compute Part 3 IR Metrics (HitRate@5, Recall@5, Precision@5, MRR, Graded NDCG@10, EPMR@5, BOM Completeness).")
