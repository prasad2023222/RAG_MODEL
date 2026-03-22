
#ONE JOB: Detect what type of paper this is


from ast import pattern
import re

def detect_paper_type(text: str) -> str:
    """
    Reads first 3000 chars and decides:
    clinical_trial / meta_analysis / guideline / review / general
    """
    signals = {
        "clinical_trial": [
            r"randomized\s+controlled\s+trial",
            r"\bRCT\b",
            r"double.blind",
            r"hazard\s+ratio",
            r"primary\s+endpoint",
        ],
        "meta_analysis": [
            r"meta.?analysis",
            r"systematic\s+review",
            r"forest\s+plot",
            r"PRISMA",
        ],
        "guideline": [
            r"guideline",
            r"strong\s+recommendation",
            r"grade\s+[A-D]\b",
            r"level\s+of\s+evidence",
        ],
        "review": [
            r"narrative\s+review",
            r"literature\s+review",
            r"state\s+of\s+the\s+art",
        ],
    }

    sample=text[:3000]

    scores={}

    for paper_type,patterns in signals.items():
        score=sum(
            1 for p in patterns
            if re.search(p,sample,re.IGNORECASE)
        )

        scores[paper_type]=score

        best=max(scores,key=scores.get)
        return best if  scores[best]>0 else "general"