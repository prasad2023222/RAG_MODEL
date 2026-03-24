"""
src/data_pipeline/extractor.py
ONE JOB: Read a PDF file and return its text + tables + metadata
"""

import re
from pathlib import Path
from collections import Counter
from loguru import logger


def extract_pdf(pdf_path: str) -> dict:
    """
    Opens a PDF and extracts:
    - full text (all pages combined)
    - tables (as text rows)
    - metadata (title, author, year, DOI, sample size)

    Returns a clean dict ready for section detection + chunking.
    """
    import pdfplumber
    from pypdf import PdfReader
    from src.data_pipeline.medical_cleaner import MedicalCleaner

    cleaner = MedicalCleaner()

    result = {
        "file_name": Path(pdf_path).name,
        "full_text": "",
        "tables":    [],
        "metadata":  {},
        "pages":     0,
    }

    # ── Step 1: Read PDF metadata (title, author) ─────────────
    try:
        reader       = PdfReader(pdf_path)
        raw          = reader.metadata or {}
        result["pages"]    = len(reader.pages)
        result["metadata"] = {
            "title":  str(raw.get("/Title",  "")).strip(),
            "author": str(raw.get("/Author", "")).strip(),
        }
    except Exception as e:
        logger.warning(f"  Metadata read failed: {e}")

    # ── Step 2: Extract text + tables from each page ──────────
    raw_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):

                # Page text
                page_text  = page.extract_text() or ""
                raw_text  += f"\n[Page {page_num}]\n{page_text}"

                # Tables on this page
                for table in page.extract_tables():
                    if table and len(table) > 1:
                        rows = [
                            " | ".join(
                                str(cell).strip() if cell else ""
                                for cell in row
                            )
                            for row in table
                            if any(cell for cell in row)
                        ]
                        if rows:
                            result["tables"].append({
                                "page": page_num,
                                "text": "\n".join(rows),
                            })

    except Exception as e:
        logger.error(f"  PDF read failed: {e}")
        return result

    # ── Step 3: Clean the text ────────────────────────────────
    result["full_text"] = cleaner.clean(raw_text)

    # ── Step 4: Extract inline metadata from text ─────────────
    result["metadata"].update(
        _extract_inline_metadata(result["full_text"])
    )

    logger.info(
        f"  Extracted: {result['pages']} pages | "
        f"{len(result['tables'])} tables | "
        f"{len(result['full_text'])} chars"
    )

    return result


def _extract_inline_metadata(text: str) -> dict:
    """
    Pulls useful metadata from paper body text:
    sample size, year, DOI, study type
    """
    meta = {}

    # Sample size: n=48,276 or 9361 patients
    for pattern in [r'n\s*=\s*([\d,]+)', r'([\d,]+)\s+patients']:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            meta["sample_size"] = m.group(1).replace(",", "")
            break

    # Year: most common 4-digit year in text
    years = re.findall(r'\b(20[0-2]\d|199\d)\b', text)
    if years:
        meta["year"] = Counter(years).most_common(1)[0][0]

    # DOI
    m = re.search(r'(?:doi:|DOI:)\s*(10\.\d{4,}/\S+)', text)
    if m:
        doi = m.group(1).rstrip('.,')
        meta["doi"]     = doi
        meta["doi_url"] = f"https://doi.org/{doi}"

    # Study type
    for study_type, pattern in {
        "rct":           r'randomized\s+controlled\s+trial|RCT',
        "meta_analysis": r'meta.?analysis|systematic\s+review',
        "guideline":     r'clinical\s+(?:practice\s+)?guideline',
        "review":        r'(?:narrative|literature)\s+review',
    }.items():
        if re.search(pattern, text[:2000], re.IGNORECASE):
            meta["study_type"] = study_type
            break

    return meta