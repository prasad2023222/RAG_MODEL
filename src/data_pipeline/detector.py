"""
src/data_pipeline/detector.py
ONE JOB: Detect what type of medical paper this is
"""

import re


def detect_paper_type(text: str) -> str:
    """
    Reads first 3000 chars and returns one of:
    clinical_trial / meta_analysis / guideline / review / general
    """
    sample = text[:3000]

    if re.search(r'randomized\s+controlled\s+trial|double.blind|placebo.controlled', sample, re.IGNORECASE):
        return "clinical_trial"

    if re.search(r'meta.?analysis|systematic\s+review|forest\s+plot', sample, re.IGNORECASE):
        return "meta_analysis"

    if re.search(r'guideline|strong\s+recommendation|level\s+of\s+evidence', sample, re.IGNORECASE):
        return "guideline"

    if re.search(r'narrative\s+review|literature\s+review|comprehensive\s+review', sample, re.IGNORECASE):
        return "review"

    return "general"