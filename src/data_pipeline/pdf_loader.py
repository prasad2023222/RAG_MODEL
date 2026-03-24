"""
src/data_pipeline/pdf_loader.py
ONE JOB: Coordinate all pipeline steps for one PDF
Calls: extractor → detector → section_finder → chunker → chromadb
"""

import os, json, hashlib
from pathlib import Path
from datetime import datetime
from loguru import logger

from src.data_pipeline.extractor      import extract_pdf
from src.data_pipeline.detector       import detect_paper_type
from src.data_pipeline.section_finder import find_section 
from src.data_pipeline.chunker        import chunk_paper
from src.data_pipeline.medical_cleaner import MedicalCleaner

#print(sf.__file__)

CHROMA_PATH     = os.getenv("CHROMA_PATH",     "./data/chroma_db")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "NeuML/pubmedbert-base-embeddings")
REGISTRY_FILE   = "./data/pdf_registry.json"

os.makedirs(CHROMA_PATH, exist_ok=True)
os.makedirs("./data",    exist_ok=True)

_vs      = None
cleaner  = MedicalCleaner()


def get_vectorstore():
    """Open ChromaDB once, reuse forever"""
    global _vs
    if _vs:
        return _vs
    from langchain_community.vectorstores import Chroma
    from langchain_community.embeddings import HuggingFaceEmbeddings
    logger.info(f"Loading PubMedBERT — first time takes 2-3 mins...")
    emb = HuggingFaceEmbeddings(
        model_name    = EMBEDDING_MODEL,
        model_kwargs  = {"device": "cpu"},
        encode_kwargs = {"normalize_embeddings": True},
    )
    _vs = Chroma(persist_directory=CHROMA_PATH, embedding_function=emb)
    return _vs


def load_pdf(pdf_path: str, domain: str, topic: str) -> dict:
    """
    Full pipeline for ONE PDF:
    Read → Clean → Detect → Section → Chunk → Validate → Store
    """
    reg   = _load_registry()
    fhash = _file_hash(pdf_path)

    # Skip if already indexed
    if any(v.get("hash") == fhash for v in reg["files"].values()):
        logger.warning(f"Already indexed: {Path(pdf_path).name}")
        return {"status": "duplicate"}

    logger.info(f"\nProcessing: {Path(pdf_path).name}")

    # Step 1: Extract text + tables from PDF
    extracted = extract_pdf(pdf_path)
    if not extracted["full_text"]:
        return {"status": "error", "error": "No text extracted"}

    # Step 2: Detect paper type
    paper_type = detect_paper_type(extracted["full_text"])

    # Step 3: Find sections
    sections = find_section(extracted["full_text"])
    logger.info(f"  Sections: {list(sections.keys()) or 'none — using fulltext fallback'}")

    # Step 4: Build metadata for every chunk
    meta      = extracted["metadata"]
    base_meta = {
        "file_name":   extracted["file_name"],
        "title":       meta.get("title") or extracted["file_name"].replace(".pdf",""),
        "authors":     meta.get("author",      "Unknown"),
        "year":        meta.get("year",        "Unknown"),
        "sample_size": meta.get("sample_size", "Not specified"),
        "study_type":  meta.get("study_type",  "Unknown"),
        "paper_type":  paper_type,
        "doi":         meta.get("doi",     ""),
        "source":      meta.get("doi_url", f"./data/pdfs/{domain}/{extracted['file_name']}"),
        "domain":      domain,
        "topic":       topic,
        "pages":       extracted["pages"],
    }

    # Step 5: Chunk the paper
    docs = chunk_paper(
        extracted["full_text"],
        sections,
        extracted["tables"],
        base_meta,
    )

    # Step 6: Drop noise chunks
    clean_docs = [d for d in docs if cleaner._medical_score(d.page_content) >= 2]
    logger.info(f"  Chunks: {len(docs)} total → {len(clean_docs)} after validation")

    if not clean_docs:
        return {"status": "error", "error": "No valid chunks after validation"}

    # Step 7: Store in ChromaDB
    vs = get_vectorstore()
    vs.add_documents(clean_docs)
    vs.persist()

    # Step 8: Register so we don't index twice
    reg["files"][extracted["file_name"]] = {
        "hash": fhash, "domain": domain, "topic": topic,
        "chunks": len(clean_docs), "pages": extracted["pages"],
        "paper_type": paper_type, "sections": list(sections.keys()),
        "indexed": str(datetime.now()),
    }
    reg["total_chunks"] = reg.get("total_chunks", 0) + len(clean_docs)
    with open(REGISTRY_FILE, "w") as f:
        json.dump(reg, f, indent=2)

    logger.success(f"  Done: {len(clean_docs)} chunks stored")

    return {
        "status":     "success",
        "file":       extracted["file_name"],
        "paper_type": paper_type,
        "chunks":     len(clean_docs),
        "pages":      extracted["pages"],
        "tables":     len(extracted["tables"]),
        "sections":   list(sections.keys()),
    }


def get_stats() -> dict:
    """How many papers + chunks are in ChromaDB"""
    reg = _load_registry()
    try:
        total = get_vectorstore()._collection.count()
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


def _load_registry():
    if os.path.exists(REGISTRY_FILE):
        with open(REGISTRY_FILE) as f:
            return json.load(f)
    return {"files": {}, "total_chunks": 0}


def _file_hash(path: str) -> str:
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()[:10]