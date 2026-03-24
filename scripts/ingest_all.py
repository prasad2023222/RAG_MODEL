"""
scripts/ingest_all.py
Run this once to index all 28 papers into ChromaDB
Usage: python scripts/ingest_all.py
"""

import os, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

os.environ["CHROMA_PATH"]     = os.getenv("CHROMA_PATH",     "./data/chroma_db")
os.environ["EMBEDDING_MODEL"] = os.getenv("EMBEDDING_MODEL", "NeuML/pubmedbert-base-embeddings")

from src.data_pipeline.pdf_loader import load_pdf, get_stats

PDF_BASE = Path("./data/pdfs")

# ── Map each filename to its topic ────────────────────────────
PAPERS = {
    "cardiology": {
        "sprint_trial.pdf":           "Hypertension Treatment",
        "esh_guidelines_2023.pdf":    "Hypertension Treatment",
        "jupiter_statin.pdf":         "Statin Therapy",
        "aspree_aspirin.pdf":         "Aspirin Prevention",
        "arrive_aspirin.pdf":         "Aspirin Prevention",
        "aspirin_meta_analysis.pdf":  "Aspirin Prevention",
        "emperor_reduced.pdf":        "Heart Failure Management",
        "paradigm_hf.pdf":            "Heart Failure Management",
        "metformin_cv_review.pdf":    "Metformin Cardiovascular",
        "accord_trial.pdf":           "Metformin Cardiovascular",
    },
    "diabetes": {
        "accord_trial.pdf":              "Glucose Control",
        "ada_standards_2024.pdf":        "Type 2 Diabetes Treatment",
        "advance_trial.pdf":             "Glucose Control",
        "credence_canagliflozin.pdf":    "SGLT2 Inhibitors",
        "dapa_hf.pdf":                   "SGLT2 Inhibitors",
        "empa_reg_outcome.pdf":          "SGLT2 Inhibitors",
        "leader_liraglutide.pdf":        "GLP-1 Receptor Agonists",
        "step1_weight_loss.pdf":         "GLP-1 Receptor Agonists",
        "sustain6_semaglutide.pdf":      "GLP-1 Receptor Agonists",
        "ukpds_followup_2008.pdf":       "Glucose Control",
    },
    "oncology": {
        "car_t_therapy.pdf":         "Cancer Immunotherapy",
        "checkmate_227.pdf":         "Lung Cancer Therapy",
        "checkpoint_review.pdf":     "Cancer Immunotherapy",
        "flaura_osimertinib.pdf":    "Lung Cancer Therapy",
        "ipilimumab_melanoma.pdf":   "Cancer Immunotherapy",
        "keynote_024_nsclc.pdf":     "Lung Cancer Therapy",
        "keynote_552_breast.pdf":    "Breast Cancer Treatment",
        "monarch2_breast.pdf":       "Breast Cancer Treatment",
    },
}


def run():
    print("\n" + "="*55)
    print("  PubMed RAG — Bulk Ingestion")
    print("  PDF → Clean → Chunk → Embed → ChromaDB")
    print("="*55)

    # Collect all PDFs
    all_pdfs = []
    for domain, topic_map in PAPERS.items():
        for filename, topic in topic_map.items():
            path = PDF_BASE / domain / filename
            if path.exists():
                all_pdfs.append({"path": str(path), "domain": domain.capitalize(), "topic": topic, "name": filename})
            else:
                print(f"  ⚠️  Not found: {domain}/{filename}")

    print(f"\n  Found: {len(all_pdfs)} PDFs")
    for domain in PAPERS:
        n = sum(1 for p in all_pdfs if p["domain"].lower() == domain)
        print(f"    {domain.capitalize():15s}: {n} PDFs")

    print()
    input("  Press ENTER to start (Ctrl+C to cancel)...")
    print()

    start      = time.time()
    ok = skip = err = total_chunks = 0

    for i, pdf in enumerate(all_pdfs, 1):
        print(f"  [{i:02d}/{len(all_pdfs)}] {pdf['domain']:12s} | {pdf['topic']:35s} | {pdf['name']}")
        result = load_pdf(pdf["path"], pdf["domain"], pdf["topic"])

        if result["status"] == "success":
            ok += 1
            total_chunks += result.get("chunks", 0)
            print(f"           ✅ chunks={result['chunks']} | pages={result['pages']} | type={result['paper_type']} | sections={result['sections']}\n")
        elif result["status"] == "duplicate":
            skip += 1
            print(f"           ⏭️  Already indexed\n")
        else:
            err += 1
            print(f"           ❌ {result.get('error','?')}\n")

    elapsed = (time.time() - start) / 60
    stats   = get_stats()

    print("="*55)
    print("  DONE")
    print("="*55)
    print(f"  Indexed  : {ok} PDFs")
    print(f"  Skipped  : {skip}")
    print(f"  Errors   : {err}")
    print(f"  Chunks   : {stats['total_chunks']:,}")
    print(f"  Time     : {elapsed:.1f} mins")
    print()
    for domain, d in stats.get("by_domain", {}).items():
        print(f"  {domain:15s}: {d['pdfs']} PDFs | {d['chunks']:,} chunks")
    print()

    if err == 0:
        print("  ✅ All done! ChromaDB is ready.")
        print("  Next → build the agents and run app.py")
    else:
        print(f"  ⚠️  {err} errors above — fix before deploying")
    print()


if __name__ == "__main__":
    run()