"""
src/data_pipeline/section_finder.py
ONE JOB: Find sections inside cleaned paper text
"""

import re

SECTION_PATTERNS = [
    ("abstract",     [r"(?im)^\s*abstract\s*$", r"(?im)^\s*summary\s*$", r"(?im)^\s*executive\s+summary\s*$"]),
    ("introduction", [r"(?im)^\s*(?:\d+[\.\s]+)?introduction\s*$", r"(?im)^\s*(?:\d+[\.\s]+)?background\s*$"]),
    ("methods",      [r"(?im)^\s*(?:\d+[\.\s]+)?methods?\s*$", r"(?im)^\s*(?:\d+[\.\s]+)?materials?\s+and\s+methods?\s*$",
                      r"(?im)^\s*(?:\d+[\.\s]+)?patients?\s+and\s+methods?\s*$", r"(?im)^\s*(?:\d+[\.\s]+)?study\s+design\s*$",
                      r"(?im)^\s*(?:\d+[\.\s]+)?search\s+strategy\s*$"]),
    ("results",      [r"(?im)^\s*(?:\d+[\.\s]+)?results?\s*$", r"(?im)^\s*(?:\d+[\.\s]+)?findings?\s*$",
                      r"(?im)^\s*(?:\d+[\.\s]+)?outcomes?\s*$"]),
    ("discussion",   [r"(?im)^\s*(?:\d+[\.\s]+)?discussion\s*$", r"(?im)^\s*(?:\d+[\.\s]+)?interpretation\s*$"]),
    ("conclusion",   [r"(?im)^\s*(?:\d+[\.\s]+)?conclusions?\s*$", r"(?im)^\s*(?:\d+[\.\s]+)?concluding\s+remarks?\s*$"]),
    ("limitations",  [r"(?im)^\s*(?:\d+[\.\s]+)?limitations?\s*$", r"(?im)^\s*(?:\d+[\.\s]+)?strengths?\s+and\s+limitations?\s*$"]),
    ("recommendations", [r"(?im)^\s*(?:\d+[\.\s]+)?recommendations?\s*$", r"(?im)^\s*(?:\d+[\.\s]+)?guidance\s*$"]),
    ("references",   [r"(?im)^\s*references?\s*$", r"(?im)^\s*bibliography\s*$"]),
]

SECTION_WEIGHT = {
    "results": "HIGH", "methods": "HIGH", "conclusion": "HIGH",
    "recommendations": "HIGH", "abstract": "MEDIUM",
    "discussion": "MEDIUM", "limitations": "MEDIUM",
    "introduction": "LOW", "references": "LOW", "full_text": "MEDIUM",
}


def find_section(text: str) -> dict:
    """
    Finds all sections in a paper.
    Returns {section_name: section_text}
    """
    # Find where each section starts
    found = {}
    for name, patterns in SECTION_PATTERNS:
        for pat in patterns:
            m = re.search(pat, text)
            if m and name not in found:
                found[name] = (m.start(), m.end())
                break

    if not found:
        return {}

    # Sort by position
    sorted_secs = sorted(found.items(), key=lambda x: x[1][0])

    # Extract text between headers
    sections = {}
    for i, (name, (start, header_end)) in enumerate(sorted_secs):
        content_end = sorted_secs[i+1][1][0] if i+1 < len(sorted_secs) else len(text)
        content = text[header_end:content_end].strip()
        if len(content) > 100:
            sections[name] = content[:5000]

    return sections