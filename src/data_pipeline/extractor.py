"""
src/data_pipeline/extractor.py
ONE JOB: Read a PDF and return raw text + tables + metadata

Returns dict with key "raw_text" (not cleaned yet)
Cleaning happens separately in pdf_loader
"""

import re
from pathlib import Path
from collections import Counter
from loguru import logger


def extract_text_and_tables(pdf_path: str) -> dict:
    """
    Opens PDF and returns:
      raw_text  → all page text combined (NOT cleaned)
      tables    → list of tables as text
      metadata  → title, author, year, doi etc
      pages     → page count
    """
    import pdfplumber
    from pypdf import PdfReader

    result = {
        "raw_text": "",
        "tables":   [],
        "metadata": {},
        "pages":    0,
    }

    # ── Get PDF metadata (title, author) ─────────────────────
    try:
        reader = PdfReader(pdf_path)
        raw    = reader.metadata or {}
        result["pages"] = len(reader.pages)
        result["metadata"] = {
            "title":  str(raw.get("/Title",  "")).strip(),
            "author": str(raw.get("/Author", "")).strip(),
        }
    except Exception:
        pass

    # ── Extract text and tables page by page ─────────────────
    all_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):

                # Page text
                page_text = page.extract_text() or ""
                all_text += f"\n[Page {page_num}]\n{page_text}"

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
        logger.error(f"PDF read error in {Path(pdf_path).name}: {e}")
        return result

    # Store raw text
    result["raw_text"] = all_text

    # Extract metadata from text body
    result["metadata"].update(_extract_body_metadata(all_text))

    logger.info(
        f"  Extracted: {result['pages']} pages | "
        f"{len(result['tables'])} tables | "
        f"{len(result['raw_text'])} chars"
    )
    return result


def _extract_body_metadata(text: str) -> dict:
    """Pull year, sample size, DOI, study type from text body."""
    meta = {}

    # Sample size
    for pattern in [
        r'n\s*=\s*([\d,]+)',
        r'([\d,]+)\s+patients',
        r'([\d,]+)\s+participants',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            meta["sample_size"] = m.group(1).replace(",", "")
            break

    # Most common year
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