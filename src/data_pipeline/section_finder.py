"""
src/data_pipeline/section_finder.py
ONE JOB: Find sections in a cleaned medical paper
"""

import re

SECTION_PATTERNS = {
    "abstract":        [r"(?im)^\s*ABSTRACT\s*$",
                        r"(?im)^\s*abstract\s*$",
                        r"(?im)^\s*summary\s*$",
                        r"(?im)^\s*executive\s+summary\s*$"],

    "introduction":    [r"(?im)^\s*INTRODUCTION\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?introduction\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?background\s*$"],

    "methods":         [r"(?im)^\s*METHODS\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?methods?\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?materials?\s+and\s+methods?\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?patients?\s+and\s+methods?\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?study\s+design\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?search\s+strategy\s*$"],

    "results":         [r"(?im)^\s*RESULTS\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?results?\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?findings?\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?outcomes?\s*$"],

    "discussion":      [r"(?im)^\s*DISCUSSION\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?discussion\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?interpretation\s*$"],

    "conclusion":      [r"(?im)^\s*CONCLUSION\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?conclusions?\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?concluding\s+remarks?\s*$"],

    "limitations":     [r"(?im)^\s*LIMITATIONS\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?limitations?\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?strengths?\s+and\s+limitations?\s*$"],

    "recommendations": [r"(?im)^\s*RECOMMENDATIONS\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?recommendations?\s*$",
                        r"(?im)^\s*(?:\d+[\.\s]+)?guidance\s*$"],

    "references":      [r"(?im)^\s*REFERENCES\s*$",
                        r"(?im)^\s*references?\s*$",
                        r"(?im)^\s*bibliography\s*$"],
}

# Weight of each section for contradiction detection
SECTION_WEIGHT = {
    "results":         "HIGH",
    "methods":         "HIGH",
    "conclusion":      "HIGH",
    "recommendations": "HIGH",
    "abstract":        "MEDIUM",
    "discussion":      "MEDIUM",
    "limitations":     "MEDIUM",
    "introduction":    "LOW",
    "references":      "SKIP",   # never chunk references
    "full_text":       "MEDIUM",
}


def find_sections(text: str) -> dict:
    """
    Finds sections in cleaned text.
    Returns {section_name: section_text}
    Returns empty dict if no sections found → triggers full text fallback
    """
    # Find start position of each section
    found = {}
    for section_name, patterns in SECTION_PATTERNS.items():
        for pattern in patterns:
            match = re.search(pattern, text)
            if match and section_name not in found:
                found[section_name] = (match.start(), match.end())
                break

    if not found:
        return {}

    # Sort by position in document
    sorted_sections = sorted(found.items(), key=lambda x: x[1][0])

    # Extract text between headers
    sections = {}
    for i, (name, (start, header_end)) in enumerate(sorted_sections):
        content_end = sorted_sections[i+1][1][0] if i+1 < len(sorted_sections) else len(text)
        content     = text[header_end:content_end].strip()

        # Skip empty sections and references
        if len(content) > 80 and name != "references":
            sections[name] = content

    return sections