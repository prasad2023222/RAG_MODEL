import os
import time
from typing import TypedDict, Optional
from dotenv import load_dotenv
 
load_dotenv()
 
# ── LangSmith setup — MUST happen before importing langgraph ──────────────────
# LangSmith records every step silently in the background
_LANGSMITH_KEY = os.getenv("LANGSMITH_API_KEY", "")
if _LANGSMITH_KEY:
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"]    = _LANGSMITH_KEY
    os.environ["LANGCHAIN_PROJECT"]    = "pubmed-rag-contradiction"
    os.environ["LANGCHAIN_ENDPOINT"]   = "https://api.smith.langchain.com"
    print("[Pipeline] ✅ LangSmith tracing ON — view at smith.langchain.com")
else:
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    print("[Pipeline] ⚠️  LangSmith OFF — add LANGSMITH_API_KEY to .env to enable")
 
from langgraph.graph import StateGraph, END
from langsmith import traceable
 
# Import our 4 step functions
from src.query_pipeline.retriever      import retrieve
from src.query_pipeline.summarizer     import summarize
from src.query_pipeline.contradiction  import detect_contradictions
from src.query_pipeline.compiler       import compile_answer
 
 
# ── State = the data box that travels through every step ──────────────────────
# Think of it like a patient file that gets filled in as it moves through a hospital
# Each step READS from the box and ADDS to the box — never removes anything
class State(TypedDict):
    question:    str            # set at start — the doctor's question
    chunks:      list           # filled by Step 1 (retriever)
    summary:     str            # filled by Step 2 (summarizer)
    contradictions: dict        # filled by Step 3 (contradiction)
    final_answer: dict          # filled by Step 4 (compiler)
    meta:        dict           # filled throughout — timing and score stats
    error:       Optional[str]  # filled if something goes wrong
 
 
# ── Node 1: Retriever ─────────────────────────────────────────────────────────
@traceable(name="Step1_Retriever")   # LangSmith records this with this label
def node_retrieve(state: State) -> State:
    print(f"\n[Step 1/4] 🔍 Retrieving from ChromaDB...")
    t_start = time.time()
 
    try:
        chunks  = retrieve(state["question"])
        elapsed = round(time.time() - t_start, 2)
 
        # Save timing + score stats to meta (LangSmith also picks these up)
        meta = state.get("meta", {})
        meta["retrieval_latency_sec"] = elapsed
        meta["chunks_retrieved"]      = len(chunks)
        meta["top_retrieval_score"]   = chunks[0]["score"] if chunks else 0
        meta["all_scores"]            = [c["score"] for c in chunks]
 
        print(f"[Step 1/4] ✅ Done in {elapsed}s — top score: {meta['top_retrieval_score']}")
        return {**state, "chunks": chunks, "meta": meta}
 
    except Exception as e:
        print(f"[Step 1/4] ❌ Failed: {e}")
        return {**state, "chunks": [], "error": f"Retriever failed: {e}"}
 
 
# ── Node 2: Summarizer ────────────────────────────────────────────────────────
@traceable(name="Step2_Summarizer")  # LangSmith records token usage here
def node_summarize(state: State) -> State:
    # If previous step failed or got no results, skip and return message
    if state.get("error") or not state.get("chunks"):
        return {**state, "summary": "No relevant information found in the database."}
 
    print(f"[Step 2/4] 📝 Summarising with Groq LLaMA3...")
    t_start = time.time()
 
    try:
        summary = summarize(state["question"], state["chunks"])
        elapsed = round(time.time() - t_start, 2)
 
        meta = state.get("meta", {})
        meta["summarizer_latency_sec"] = elapsed
 
        print(f"[Step 2/4] ✅ Done in {elapsed}s")
        return {**state, "summary": summary, "meta": meta}
 
    except Exception as e:
        print(f"[Step 2/4] ❌ Failed: {e}")
        return {**state, "summary": f"Summary failed: {e}", "error": str(e)}
 
 
# ── Node 3: Contradiction Detector ────────────────────────────────────────────
@traceable(name="Step3_ContradictionDetector")
def node_contradiction(state: State) -> State:
    if state.get("error"):
        return {**state, "contradictions": {"found": False, "conflicts": []}}
 
    print(f"[Step 3/4] ⚔️  Checking for contradictions...")
    t_start = time.time()
 
    try:
        result  = detect_contradictions(state["question"], state["chunks"])
        elapsed = round(time.time() - t_start, 2)
        n       = result.get("count", 0)
 
        meta = state.get("meta", {})
        meta["contradiction_latency_sec"] = elapsed
        meta["contradictions_found"]      = n
        meta["evidence_consistency"]      = result.get("overall_consistency", "?")
 
        print(f"[Step 3/4] ✅ Done in {elapsed}s — {n} contradiction(s) found")
        return {**state, "contradictions": result, "meta": meta}
 
    except Exception as e:
        print(f"[Step 3/4] ❌ Failed: {e}")
        return {**state,
                "contradictions": {"found": False, "conflicts": [],
                                   "analyst_note": f"Analysis failed: {e}"},
                "meta": state.get("meta", {})}
 
 
# ── Node 4: Compiler ─────────────────────────────────────────────────────────
@traceable(name="Step4_Compiler")
def node_compile(state: State) -> State:
    print(f"[Step 4/4] 📊 Compiling structured answer...")
 
    answer = compile_answer(
        question       = state["question"],
        summary        = state.get("summary", ""),
        contradictions = state.get("contradictions", {}),
        chunks         = state.get("chunks", []),
        pipeline_meta  = state.get("meta", {}),
    )
 
    # Log total time
    meta = state.get("meta", {})
    total = round(
        meta.get("retrieval_latency_sec", 0) +
        meta.get("summarizer_latency_sec", 0) +
        meta.get("contradiction_latency_sec", 0),
        2
    )
    print(f"[Step 4/4] ✅ Done — total pipeline time: {total}s")
 
    return {**state, "final_answer": answer}
 
 
# ── Build the LangGraph ───────────────────────────────────────────────────────
def _build_graph():
    graph = StateGraph(State)
 
    # Register 4 nodes
    graph.add_node("retrieve",      node_retrieve)
    graph.add_node("summarize",     node_summarize)
    graph.add_node("contradiction", node_contradiction)
    graph.add_node("compile",       node_compile)
 
    # Connect in order: retrieve → summarize → contradiction → compile → END
    graph.set_entry_point("retrieve")
    graph.add_edge("retrieve",      "summarize")
    graph.add_edge("summarize",     "contradiction")
    graph.add_edge("contradiction", "compile")
    graph.add_edge("compile",        END)
 
    return graph.compile()
 
 
# Build once at import time — reused for every query
print("[Pipeline] Building LangGraph pipeline...")
_graph = _build_graph()
print("[Pipeline] ✅ Pipeline ready (4 nodes: retrieve → summarize → contradiction → compile)\n")
 
 
# ── Public function — called by app.py ───────────────────────────────────────
def run_pipeline(question: str) -> dict:
    """
    The only function app.py needs to call.
 
    Input:  question string
    Output: dict with keys:
              full_answer  — full markdown report
              summary_md   — just the summary
              conflicts_md — just the contradictions
              sources_md   — just the sources
              stats_md     — pipeline timing stats
    """
    print(f"\n{'='*60}")
    print(f"[Pipeline] New query: '{question[:70]}...'")
 
    initial_state = {
        "question":       question,
        "chunks":         [],
        "summary":        "",
        "contradictions": {},
        "final_answer":   {},
        "meta":           {},
        "error":          None,
    }
 
    final_state = _graph.invoke(initial_state)
    return final_state.get("final_answer", {})
 
 
# ── Test the full pipeline ────────────────────────────────────────────────────
# Command: python src/query_pipeline/pipeline.py
if __name__ == "__main__":
    question = "What is the recommended HbA1c target for type 2 diabetes?"
    answer   = run_pipeline(question)
 
    print("\n" + "="*60)
    print("FULL ANSWER:")
    print(answer.get("full_answer", "No answer"))