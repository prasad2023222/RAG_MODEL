"""
scripts/ingest_all.py
Run: python scripts/ingest_all.py
"""

import os, sys, shutil, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

os.environ["CHROMA_PATH"]     = os.getenv("CHROMA_PATH",     "./data/chroma_db")
os.environ["EMBEDDING_MODEL"] = os.getenv("EMBEDDING_MODEL", "NeuML/pubmedbert-base-embeddings")

CHROMA_PATH   = "./data/chroma_db"
REGISTRY_FILE = "./data/pdf_registry.json"
PDF_BASE      = Path("./data/pdfs")

PAPERS = {
    "cardiology": {
        "sprint_trial.pdf":          "Hypertension Treatment",
        "esh_guidelines_2023.pdf":   "Hypertension Treatment",
        "jupiter_statin.pdf":        "Statin Therapy",
        "aspree_aspirin.pdf":        "Aspirin Prevention",
        "arrive_aspirin.pdf":        "Aspirin Prevention",
        "aspirin_meta_analysis.pdf": "Aspirin Prevention",
        "emperor_reduced.pdf":       "Heart Failure Management",
        "paradigm_hf.pdf":           "Heart Failure Management",
        "metformin_cv_review.pdf":   "Metformin Cardiovascular",
        "accord_trial.pdf":          "Metformin Cardiovascular",
    },
    "diabetes": {
        "accord_trial.pdf":           "Glucose Control",
        "ada_standards_2024.pdf":     "Type 2 Diabetes Treatment",
        "advance_trial.pdf":          "Glucose Control",
        "credence_canagliflozin.pdf": "SGLT2 Inhibitors",
        "dapa_hf.pdf":                "SGLT2 Inhibitors",
        "empa_reg_outcome.pdf":       "SGLT2 Inhibitors",
        "leader_liraglutide.pdf":     "GLP-1 Receptor Agonists",
        "step1_weight_loss.pdf":      "GLP-1 Receptor Agonists",
        "sustain6_semaglutide.pdf":   "GLP-1 Receptor Agonists",
        "ukpds_followup_2008.pdf":    "Glucose Control",
    },
    "oncology": {
        "car_t_therapy.pdf":        "Cancer Immunotherapy",
        "checkmate_227.pdf":        "Lung Cancer Therapy",
        "checkpoint_review.pdf":    "Cancer Immunotherapy",
        "flaura_osimertinib.pdf":   "Lung Cancer Therapy",
        "ipilimumab_melanoma.pdf":  "Cancer Immunotherapy",
        "keynote_024_nsclc.pdf":    "Lung Cancer Therapy",
        "keynote_552_breast.pdf":   "Breast Cancer Treatment",
        "monarch2_breast.pdf":      "Breast Cancer Treatment",
    },
}


def ask_reset():
    print("\n" + "="*55)
    print("  PubMed RAG — Ingestion Pipeline")
    print("="*55)

    if Path(REGISTRY_FILE).exists():
        import json
        with open(REGISTRY_FILE) as f:
            reg = json.load(f)
        print(f"\n  Existing data:")
        print(f"    Papers : {len(reg.get('files', {}))}")
        print(f"    Chunks : {reg.get('total_chunks', 0):,}")
    else:
        print("\n  No existing data found.")

    print()
    answer = input("  Reset and start fresh? (y/n): ").strip().lower()
    return answer == "y"


def reset():
    print("\n  Resetting...")
    if Path(CHROMA_PATH).exists():
        shutil.rmtree(CHROMA_PATH)
        print("  ✅ ChromaDB cleared")
    if Path(REGISTRY_FILE).exists():
        os.remove(REGISTRY_FILE)
        print("  ✅ Registry cleared")
    os.makedirs(CHROMA_PATH, exist_ok=True)
    print("  ✅ Fresh ChromaDB created\n")


def ingest():
    from src.data_pipeline.pdf_loader import load_pdf, get_stats

    # Collect all PDFs
    all_pdfs = []
    missing  = []
    for domain, topic_map in PAPERS.items():
        for filename, topic in topic_map.items():
            path = PDF_BASE / domain / filename
            if path.exists():
                all_pdfs.append({
                    "path":   str(path),
                    "domain": domain.capitalize(),
                    "topic":  topic,
                    "name":   filename,
                })
            else:
                missing.append(f"{domain}/{filename}")

    if missing:
        print("  ⚠️  Missing (check filenames):")
        for m in missing:
            print(f"     {m}")
        print()

    print(f"  Found: {len(all_pdfs)} PDFs")
    print()
    input("  Press ENTER to start...")
    print()

    start    = time.time()
    ok = err = 0

    for i, pdf in enumerate(all_pdfs, 1):
        print(f"  [{i:02d}/{len(all_pdfs)}] {pdf['domain']:12s} | {pdf['name']}")

        result = load_pdf(pdf["path"], pdf["domain"], pdf["topic"])

        if result["status"] == "success":
            ok += 1
            print(
                f"           ✅ {result['chunks']} chunks | "
                f"{result['pages']} pages | "
                f"type={result['paper_type']} | "
                f"sections={result['sections']}\n"
            )
        elif result["status"] == "duplicate":
            ok += 1
            print("           ⏭️  Already indexed\n")
        else:
            err += 1
            print(f"           ❌ {result.get('error', '?')}\n")

    elapsed = (time.time() - start) / 60
    stats   = get_stats()

    print("="*55)
    print("  DONE")
    print("="*55)
    print(f"  Papers  : {ok} ok | {err} errors")
    print(f"  Chunks  : {stats['total_chunks']:,}")
    print(f"  Time    : {elapsed:.1f} mins\n")

    for domain, d in stats.get("by_domain", {}).items():
        print(f"  {domain:15s}: {d['pdfs']} PDFs | {d['chunks']:,} chunks")
    print()

    if err == 0:
        print("  ✅ All done! ChromaDB is ready.")
    else:
        print(f"  ⚠️  {err} errors — check above")
    print()


if __name__ == "__main__":
    do_reset = ask_reset()
    if do_reset:
        reset()
    ingest()