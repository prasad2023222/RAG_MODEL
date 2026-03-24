"""
src/data_pipeline/chunker.py
ONE JOB: Split paper text into chunks for ChromaDB
"""

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from src.data_pipeline.section_finder import SECTION_WEIGHT


def chunk_paper(
    full_text: str,
    sections:  dict,
    tables:    list,
    base_meta: dict
) -> list:
    """
    Splits paper into chunks.

    PATH A — sections found:
        Chunks each section separately.
        Tags each chunk with section name + weight.

    PATH B — no sections found:
        Chunks the full text.
        Used as fallback for unstructured papers.

    ALWAYS adds table chunks at the end.
    """

    splitter = RecursiveCharacterTextSplitter(
        chunk_size    = 700,
        chunk_overlap = 150,
        separators    = ["\n\n", "\n", ". ", " "],
    )

    docs = []

    # ── PATH A: section-based chunks ─────────────────────────
    if sections:
        for sec_name, sec_text in sections.items():
            if len(sec_text.strip()) < 100:
                continue

            weight = SECTION_WEIGHT.get(sec_name, "LOW")
            chunks = splitter.split_text(sec_text)

            for i, chunk in enumerate(chunks):
                if len(chunk.strip()) < 80:
                    continue
                docs.append(Document(
                    page_content = chunk,
                    metadata     = {
                        **base_meta,
                        "section":        sec_name,
                        "section_weight": weight,
                        "chunk_type":     "section",
                        "chunk_index":    i,
                    }
                ))

    # ── PATH B: full-text fallback ────────────────────────────
    else:
        chunks = splitter.split_text(full_text)
        for i, chunk in enumerate(chunks):
            if len(chunk.strip()) < 80:
                continue
            docs.append(Document(
                page_content = chunk,
                metadata     = {
                    **base_meta,
                    "section":        "full_text",
                    "section_weight": "MEDIUM",
                    "chunk_type":     "fulltext",
                    "chunk_index":    i,
                }
            ))

    # ── ALWAYS: table chunks ──────────────────────────────────
    for table in tables:
        table_text = table.get("text", "")
        if len(table_text) > 60:
            docs.append(Document(
                page_content = f"[TABLE page {table['page']}]\n{table_text}",
                metadata     = {
                    **base_meta,
                    "section":        "table",
                    "section_weight": "HIGH",
                    "chunk_type":     "table",
                    "chunk_index":    table["page"],
                }
            ))

    return docs