from datetime import datetime, timezone
 
 
# ── Severity colours ───────────────────────────────────────────────────────────
SEVERITY_EMOJI = {
    "HIGH":    "🔴",
    "MEDIUM":  "🟡",
    "LOW":     "🟢",
    "UNKNOWN": "⚪",
}
 
CONSISTENCY_TEXT = {
    "HIGH":    "✅ High — sources largely agree",
    "MEDIUM":  "⚠️ Medium — some variation between sources",
    "LOW":     "❌ Low — significant contradictions found",
    "N/A":     "📄 N/A — only one source available",
    "UNKNOWN": "❓ Unknown",
}
 
 
# ── Helper: deduplicate sources ────────────────────────────────────────────────
def _get_unique_sources(chunks: list) -> list:
    """
    Takes the list of 8 chunks (which may repeat the same source)
    and returns a deduplicated list, keeping the highest score per source.
    """
    seen = {}
    for chunk in chunks:
        src = chunk.get("source", "Unknown")
        # Keep the entry with the highest relevance score
        if src not in seen or chunk["score"] > seen[src]["score"]:
            seen[src] = chunk
 
    # Sort by score, best first
    return sorted(seen.values(), key=lambda x: x["score"], reverse=True)
 
 
# ── Helper: format the sources section ────────────────────────────────────────
def _build_sources_md(chunks: list) -> str:
    unique = _get_unique_sources(chunks)
 
    if not unique:
        return "## 📚 Cited Sources\n\n_No sources were retrieved._"
 
    lines = ["## 📚 Cited Sources\n_(ranked by relevance to your question)_\n"]
    for i, src in enumerate(unique, 1):
        lines.append(
            f"**[{i}]** `{src['source']}`  "
            f"| Page: {src['page']}  "
            f"| Relevance score: **{src['score']:.3f}**"
        )
    return "\n".join(lines)
 
 
# ── Helper: format the contradictions section ──────────────────────────────────
def _build_conflicts_md(contradictions: dict) -> str:
    if not contradictions.get("found"):
        note        = contradictions.get("analyst_note", "No conflicts detected.")
        consistency = contradictions.get("overall_consistency", "N/A")
        return (
            f"## ✅ No Contradictions Found\n\n"
            f"**Evidence Consistency:** {CONSISTENCY_TEXT.get(consistency, consistency)}\n\n"
            f"_{note}_"
        )
 
    lines = []
    count       = contradictions.get("count", 0)
    consistency = contradictions.get("overall_consistency", "?")
 
    lines.append(f"## ⚠️ {count} Contradiction(s) Found")
    lines.append(f"**Overall Evidence Consistency:** {CONSISTENCY_TEXT.get(consistency, consistency)}\n")
 
    for i, conflict in enumerate(contradictions.get("conflicts", []), 1):
        severity = conflict.get("severity", "UNKNOWN")
        emoji    = SEVERITY_EMOJI.get(severity, "⚪")
 
        lines.append(f"---\n### {emoji} Conflict {i}: {conflict.get('topic', '')}")
        lines.append(f"**Severity:** {severity}\n")
 
        lines.append(f"**Claim A:** {conflict.get('claim_a', '_not provided_')}")
        lines.append(f"\n**Claim B:** {conflict.get('claim_b', '_not provided_')}")
 
        # Root cause — this is what the architecture diagram specifies
        lines.append(f"\n**Root Cause:** {conflict.get('root_cause', '_not identified_')}")
 
        # Clinical implication — also specified in architecture diagram
        lines.append(f"\n**Clinical Implication:** _{conflict.get('clinical_implication', '_not provided_')}_")
 
    note = contradictions.get("analyst_note", "")
    if note:
        lines.append(f"\n\n---\n_{note}_")
 
    return "\n".join(lines)
 
 
# ── Main function ──────────────────────────────────────────────────────────────
def compile_answer(
    question:        str,
    summary:         str,
    contradictions:  dict,
    chunks:          list,
    pipeline_meta:   dict = None,   # timing/score stats from LangSmith steps
) -> dict:
    """
    Assembles the complete structured answer.
 
    Returns a dict with these keys (one per Gradio tab):
        full_answer   — complete report (used in Full Report tab)
        summary_md    — just the summary section (used in Chat tab)
        conflicts_md  — just the contradictions (used in Conflicts tab)
        sources_md    — just the sources (used in Sources tab)
        stats_md      — pipeline stats (used in System tab)
    """
 
    if pipeline_meta is None:
        pipeline_meta = {}
 
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
 
    # ── Build each section ─────────────────────────────────────────────────────
 
    # 1. Summary section
    summary_md = f"## 📋 Evidence Summary\n\n{summary}"
 
    # 2. Contradictions section (includes root cause + clinical implication)
    conflicts_md = _build_conflicts_md(contradictions)
 
    # 3. Sources section (deduplicated, ranked)
    sources_md = _build_sources_md(chunks)
 
    # 4. Pipeline stats
    total_time = round(
        pipeline_meta.get("retrieval_latency_sec", 0) +
        pipeline_meta.get("summarizer_latency_sec", 0) +
        pipeline_meta.get("contradiction_latency_sec", 0),
        2
    )
    unique_sources = len(_get_unique_sources(chunks))
    n_conflicts    = contradictions.get("count", 0)
    top_score      = pipeline_meta.get("top_retrieval_score", "?")
 
    stats_md = (
        f"## ⚙️ Pipeline Stats\n"
        f"- **Chunks retrieved:** {len(chunks)}\n"
        f"- **Unique sources:** {unique_sources}\n"
        f"- **Top relevance score:** {top_score}\n"
        f"- **Contradictions found:** {n_conflicts}\n"
        f"- **Total time:** {total_time}s\n"
        f"- **Generated:** {timestamp}"
    )
 
    # 5. Full answer = all sections combined (for the Full Report tab)
    full_answer = "\n\n---\n\n".join([
        f"# 🏥 Medical Evidence Report\n**Question:** _{question}_\n**{timestamp}**",
        summary_md,
        conflicts_md,
        sources_md,
        stats_md,
    ])
 
    return {
        "full_answer":  full_answer,
        "summary_md":   summary_md,
        "conflicts_md": conflicts_md,
        "sources_md":   sources_md,
        "stats_md":     stats_md,
    }
 
 
# ── Test this file directly ────────────────────────────────────────────────────
# Command: python src/query_pipeline/compiler.py
if __name__ == "__main__":
    fake_summary = """## Summary
 
Based on the retrieved evidence, metformin remains the first-line therapy for type 2 diabetes [Source: ADA_2023.pdf].
However, the ACCORD trial raised concerns about aggressive glucose targets [Source: ACCORD_2008.pdf].
 
## Key Points
- Metformin is first-line for most T2DM patients [Source: ADA_2023.pdf]
- HbA1c target <7% reduces microvascular complications [Source: UKPDS_1998.pdf]
- Aggressive targeting (<6.5%) may increase CV mortality [Source: ACCORD_2008.pdf]"""
 
    fake_contradictions = {
        "found": True,
        "count": 1,
        "conflicts": [{
            "topic": "HbA1c target in T2DM",
            "claim_a": "Chunk 1 (ACCORD): HbA1c <6.5% increases cardiovascular mortality",
            "claim_b": "Chunk 2 (ADA 2023): HbA1c <7.0% is the recommended standard target",
            "severity": "HIGH",
            "root_cause": "Different outcome measures",
            "clinical_implication": "Use ADA 2023 target of <7.0% for most patients. Avoid aggressive targets in high cardiovascular risk patients."
        }],
        "overall_consistency": "MEDIUM",
        "analyst_note": "One landmark RCT conflicts with current guidelines — doctor should consider individual patient risk."
    }
 
    fake_chunks = [
        {"source": "ACCORD_2008.pdf", "page": "3", "score": 0.91,
         "text": "Targeting HbA1c below 6.5% increased CV mortality."},
        {"source": "ADA_Standards_2023.pdf", "page": "15", "score": 0.88,
         "text": "Standard HbA1c target is below 7.0%."},
        {"source": "ADA_Standards_2023.pdf", "page": "16", "score": 0.84,
         "text": "Individualise targets based on patient risk."},
    ]
 
    result = compile_answer(
        question="What HbA1c target for my T2DM patient?",
        summary=fake_summary,
        contradictions=fake_contradictions,
        chunks=fake_chunks,
        pipeline_meta={
            "retrieval_latency_sec": 0.3,
            "summarizer_latency_sec": 1.8,
            "contradiction_latency_sec": 1.5,
            "top_retrieval_score": 0.91,
        }
    )
 
    print("=== FULL ANSWER ===")
    print(result["full_answer"])