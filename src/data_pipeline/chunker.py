"""
src/data_pipeline/chunker.py
ONE JOB: Split paper text into chunks for ChromaDB
"""

from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from src.data_pipeline.section_finder import SECTION_WEIGHT

CHUNK_SIZE    = 800
CHUNK_OVERLAP = 200
MIN_CHUNK_LEN = 50


def create_chunks(clean_text: str, sections: dict,
                  tables: list, base_meta: dict) -> list:
    """
    PATH A: sections found    → chunk section by section
    PATH B: no sections found → chunk full clean text
    ALWAYS: add table chunks
    """
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ". ", " "],
    )
    docs = []

    # ── PATH A: Section-based chunks ─────────────────────────
    if sections:
        for section_name, section_text in sections.items():
            if len(section_text.strip()) < MIN_CHUNK_LEN:
                continue

            weight = SECTION_WEIGHT.get(section_name, "LOW")

            for i, chunk in enumerate(splitter.split_text(section_text)):
                if len(chunk.strip()) < MIN_CHUNK_LEN:
                    continue
                docs.append(Document(
                    page_content=chunk,
                    metadata={
                        **base_meta,
                        "section":        section_name,
                        "section_weight": weight,
                        "chunk_type":     "section",
                        "chunk_index":    i,
                    }
                ))

    # ── PATH B: Full text fallback ────────────────────────────
    if not docs:
        for i, chunk in enumerate(splitter.split_text(clean_text)):
            if len(chunk.strip()) < MIN_CHUNK_LEN:
                continue
            docs.append(Document(
                page_content=chunk,
                metadata={
                    **base_meta,
                    "section":        "full_text",
                    "section_weight": "MEDIUM",
                    "chunk_type":     "fulltext",
                    "chunk_index":    i,
                }
            ))

    # ── ALWAYS: Table chunks ──────────────────────────────────
    for table in tables:
        table_text = table.get("text", "")
        if len(table_text) > MIN_CHUNK_LEN:
            docs.append(Document(
                page_content=f"[TABLE page {table['page']}]\n{table_text}",
                metadata={
                    **base_meta,
                    "section":        "table",
                    "section_weight": "HIGH",
                    "chunk_type":     "table",
                    "chunk_index":    table["page"],
                }
            ))

    return docs