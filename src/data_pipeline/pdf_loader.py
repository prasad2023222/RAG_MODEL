"""
src/data_pipeline/pdf_loader.py
ONE JOB: Coordinate the full pipeline for one PDF

Flow:
  extractor   → reads PDF → returns raw_text
  cleaner     → cleans raw_text → returns clean_text
  detector    → detects paper type
  section_finder → finds sections in clean_text
  chunker     → splits into chunks
  chromadb    → stores chunks
"""

import os, json, hashlib
from pathlib import Path
from datetime import datetime
from loguru import logger

from src.data_pipeline.extractor       import extract_text_and_tables
from src.data_pipeline.medical_cleaner import MedicalCleaner
from src.data_pipeline.detector        import detect_paper_type
from src.data_pipeline.section_finder  import find_sections
from src.data_pipeline.chunker         import create_chunks

CHROMA_PATH     = os.getenv("CHROMA_PATH",     "./data/chroma_db")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "NeuML/pubmedbert-base-embeddings")
REGISTRY_FILE   = "./data/pdf_registry.json"

cleaner = MedicalCleaner()
_vs     = None

os.makedirs(CHROMA_PATH, exist_ok=True)
os.makedirs("./data",    exist_ok=True)


def load_pdf(pdf_path: str, domain: str, topic: str) -> dict:
    reg   = _load_registry()
    fhash = _file_hash(pdf_path)

    # Skip if already indexed
    if any(v.get("hash") == fhash for v in reg["files"].values()):
        logger.info(f"Already indexed: {Path(pdf_path).name}")
        return {"status": "duplicate"}

    logger.info(f"Processing: {Path(pdf_path).name}")

    # ── Step 1: Read PDF ──────────────────────────────────────
    extracted = extract_text_and_tables(pdf_path)

    # Check using correct key "raw_text"
    if not extracted.get("raw_text"):
        return {"status": "error", "error": "Could not read PDF"}

    # ── Step 2: Clean text ────────────────────────────────────
    clean_text = cleaner.clean(extracted["raw_text"])

    if not clean_text or len(clean_text) < 100:
        return {"status": "error", "error": "Text empty after cleaning"}

    # ── Step 3: Detect paper type ─────────────────────────────
    paper_type = detect_paper_type(clean_text)

    # ── Step 4: Find sections ─────────────────────────────────
    sections = find_sections(clean_text)

    # ── Step 5: Build metadata for every chunk ────────────────
    meta      = extracted["metadata"]
    base_meta = {
        "file_name":   Path(pdf_path).name,
        "title":       meta.get("title") or Path(pdf_path).stem,
        "authors":     meta.get("author",     "Unknown"),
        "year":        meta.get("year",        "Unknown"),
        "sample_size": meta.get("sample_size", "Not specified"),
        "study_type":  meta.get("study_type",  "Unknown"),
        "paper_type":  paper_type,
        "doi":         meta.get("doi",    ""),
        "source":      meta.get("doi_url", ""),
        "domain":      domain,
        "topic":       topic,
        "pages":       extracted["pages"],
    }

    # ── Step 6: Create chunks ─────────────────────────────────
    docs = create_chunks(clean_text, sections, extracted["tables"], base_meta)

    # ── Step 7: Filter noise chunks ───────────────────────────
    # Only filter out chunks with zero medical content
    docs = [d for d in docs if cleaner.is_medical_chunk(d.page_content)]

    if not docs:
        logger.warning(f"  No valid chunks for {Path(pdf_path).name} — storing as full text")
        # Last resort: store the whole clean text as one big chunk
        from langchain_core.documents import Document
        docs = [Document(page_content=clean_text[:8000], metadata={**base_meta, "section": "full_text", "section_weight": "MEDIUM", "chunk_type": "fallback", "chunk_index": 0})]

    # ── Step 8: Store in ChromaDB ─────────────────────────────
    vs = _get_vectorstore()
    vs.add_documents(docs)
    vs.persist()

    # ── Step 9: Save to registry ──────────────────────────────
    _save_to_registry(
        Path(pdf_path).name, fhash, domain, topic,
        len(docs), extracted["pages"], paper_type,
        list(sections.keys()), reg
    )

    logger.success(f"  ✅ {len(docs)} chunks | type={paper_type} | sections={list(sections.keys())}")

    return {
        "status":     "success",
        "file":       Path(pdf_path).name,
        "paper_type": paper_type,
        "chunks":     len(docs),
        "pages":      extracted["pages"],
        "tables":     len(extracted["tables"]),
        "sections":   list(sections.keys()),
    }


def get_stats() -> dict:
    reg = _load_registry()
    try:
        total = _get_vectorstore()._collection.count()
    except Exception:
        total = reg.get("total_chunks", 0)
    by_domain = {}
    for info in reg["files"].values():
        d = info.get("domain", "Unknown")
        by_domain.setdefault(d, {"pdfs": 0, "chunks": 0})
        by_domain[d]["pdfs"]   += 1
        by_domain[d]["chunks"] += info.get("chunks", 0)
    return {
        "total_pdfs":   len(reg["files"]),
        "total_chunks": total,
        "by_domain":    by_domain,
        "files":        list(reg["files"].values()),
    }


# ── Helpers ───────────────────────────────────────────────────

def _get_vectorstore():
    global _vs
    if _vs:
        return _vs
    from langchain_community.vectorstores import Chroma
    from langchain_community.embeddings import HuggingFaceEmbeddings
    logger.info("Loading PubMedBERT embeddings (first time ~3 mins)...")
    emb = HuggingFaceEmbeddings(
        model_name=EMBEDDING_MODEL,
        model_kwargs={"device": "cpu"},
        encode_kwargs={"normalize_embeddings": True},
    )
    _vs = Chroma(persist_directory=CHROMA_PATH, embedding_function=emb)
    return _vs

def _file_hash(path: str) -> str:
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()[:10]

def _load_registry() -> dict:
    if os.path.exists(REGISTRY_FILE):
        with open(REGISTRY_FILE) as f:
            return json.load(f)
    return {"files": {}, "total_chunks": 0}

def _save_registry(reg: dict):
    with open(REGISTRY_FILE, "w") as f:
        json.dump(reg, f, indent=2)

def _save_to_registry(name, fhash, domain, topic,
                      chunks, pages, ptype, sections, reg):
    reg["files"][name] = {
        "hash":       fhash,
        "domain":     domain,
        "topic":      topic,
        "chunks":     chunks,
        "pages":      pages,
        "paper_type": ptype,
        "sections":   sections,
        "indexed":    str(datetime.now()),
    }
    reg["total_chunks"] = reg.get("total_chunks", 0) + chunks
    _save_registry(reg)