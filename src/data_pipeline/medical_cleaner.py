"""
src/data_pipeline/medical_cleaner.py

ONE JOB: Clean raw PDF text before processing.
Removes all noise, keeps only clinical content.

8 Steps:
  1. Remove page headers and footers
  2. Remove author affiliations and emails
  3. Remove copyright and licence text
  4. Remove figure references
  5. Remove inline citation numbers [1][2][3]
  6. Fix broken words and whitespace
  7. Remove paragraphs with no medical content
  8. Standardise section header names
"""

import re


# Medical terms used to score chunks in Step 7
MEDICAL_TERMS = {
    # Study design
    "randomized", "controlled", "trial", "placebo",
    "double-blind", "meta-analysis", "systematic",
    # Statistics
    "p-value", "confidence", "interval", "hazard",
    "ratio", "odds", "mean", "median", "standard",
    # Clinical
    "patient", "participants", "dose", "mg", "kg",
    "mmhg", "treatment", "therapy", "efficacy",
    "safety", "adverse", "outcome", "mortality",
    "survival", "clinical",
    # Cardiology
    "cardiovascular", "cardiac", "heart",
    "hypertension", "stroke", "systolic", "diastolic",
    "cholesterol", "statin", "aspirin",
    # Diabetes
    "diabetes", "insulin", "glucose", "hba1c",
    "glycemic", "metformin", "sglt2", "glp-1",
    # Oncology
    "cancer", "tumor", "tumour", "chemotherapy",
    "immunotherapy", "pembrolizumab", "nivolumab",
}


class MedicalCleaner:

    def clean(self, raw_text: str) -> str:
        """
        Run all 8 cleaning steps on raw PDF text.
        Returns clean text ready for section detection.
        """
        if not raw_text or len(raw_text) < 50:
            return raw_text

        text = raw_text
        text = self._step1_remove_headers_footers(text)
        text = self._step2_remove_author_block(text)
        text = self._step3_remove_copyright(text)
        text = self._step4_remove_figure_refs(text)
        text = self._step5_remove_citation_numbers(text)
        text = self._step6_fix_whitespace(text)
        text = self._step7_remove_noise_paragraphs(text)
        text = self._step8_standardise_headers(text)

        return text.strip()

    # ─────────────────────────────────────────────────────────
    # STEP 1 — Remove page headers and footers
    # Examples removed:
    #   "Journal of Medicine Vol 45 ISSN 1234-5678"
    #   "Downloaded by user on 15 March 2024"
    #   "Page 4 of 12"
    # ─────────────────────────────────────────────────────────
    def _step1_remove_headers_footers(self, text: str) -> str:
        patterns = [
            r'(?i)ISSN[\s\-:]?\d{4}[\-]\d{4}',
            r'(?i)Vol(?:ume)?\.?\s*\d+.*?Issue.*?\d+',
            r'(?i)downloaded\s+by\s+\S+\s+on\s+.{0,50}',
            r'\[Page\s+\d+\]',
            r'(?m)^\s*Page\s+\d+\s+of\s+\d+\s*$',
            r'(?i)(?:©|copyright)\s+\d{4}\s+\S+',
            r'(?i)all\s+rights\s+reserved\.?',
            r'(?i)published\s+by\s+(?:Elsevier|Springer|Wiley|Oxford|Nature)\S*',
        ]
        for pat in patterns:
            text = re.sub(pat, ' ', text)
        return text

    # ─────────────────────────────────────────────────────────
    # STEP 2 — Remove author affiliations and emails
    # Examples removed:
    #   "¹Department of Medicine, Harvard Medical School"
    #   "Corresponding author: rahman@harvard.edu"
    #   "Received: January 5 2023"
    #   "ORCID: 0000-0002-1234-5678"
    # ─────────────────────────────────────────────────────────
    def _step2_remove_author_block(self, text: str) -> str:
        patterns = [
            r'(?m)^[\d¹²³⁴⁵]+\s*(?:Department|School|Faculty|'
            r'Institute|University|Hospital|Center|Division)'
            r'\s+of\s+.+$',
            r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b',
            r'(?i)(?:corresponding|correspondence)\s+author.{0,120}',
            r'(?i)received\s*:?\s*\w+\s+\d+[,\s]+\d{4}',
            r'(?i)accepted\s*:?\s*\w+\s+\d+[,\s]+\d{4}',
            r'(?i)revised\s*:?\s*\w+\s+\d+[,\s]+\d{4}',
            r'ORCID\s*:?\s*[\d\-X]{19}',
        ]
        for pat in patterns:
            text = re.sub(pat, ' ', text, flags=re.IGNORECASE)
        return text

    # ─────────────────────────────────────────────────────────
    # STEP 3 — Remove copyright and licence text
    # Examples removed:
    #   "This is an open access article under CC BY 4.0"
    #   "The authors declare no competing interests"
    #   "For personal use only. Not for redistribution"
    # ─────────────────────────────────────────────────────────
    def _step3_remove_copyright(self, text: str) -> str:
        patterns = [
            r'(?i)this\s+is\s+an?\s+open.access\s+article.{0,200}',
            r'(?i)(?:CC\s+BY|Creative\s+Commons).{0,100}',
            r'(?i)the\s+authors?\s+declare\s+no\s+competing'
            r'\s+interests?.{0,100}',
            r'(?i)conflict.{0,10}interest.{0,200}',
            r'(?i)for\s+personal\s+use\s+only',
            r'(?i)not\s+for\s+redistribution',
            r'(?i)access\s+provided\s+by\s+.{0,60}',
        ]
        for pat in patterns:
            text = re.sub(pat, ' ', text, flags=re.DOTALL)
        return text

    # ─────────────────────────────────────────────────────────
    # STEP 4 — Remove figure references
    # Examples removed:
    #   "(See Figure 1)"
    #   "(Insert Figure 2 here)"
    #   "as shown in Fig. 3"
    # ─────────────────────────────────────────────────────────
    def _step4_remove_figure_refs(self, text: str) -> str:
        patterns = [
            r'\((?:see\s+)?[Ff]ig(?:ure)?\.?\s*\d+[a-zA-Z]?\)',
            r'(?i)\(?\s*[Ii]nsert\s+[Ff]ig(?:ure)?\s*\d+\s*here\s*\)?',
            r'(?i)\bin\s+[Ff]ig(?:ure)?\.?\s*\d+',
            r'(?i)(?:Supplementary|Extended)\s+[Ff]ig(?:ure)?'
            r'\.?\s*[A-Z]?\d*',
        ]
        for pat in patterns:
            text = re.sub(pat, ' ', text)
        return text

    # ─────────────────────────────────────────────────────────
    # STEP 5 — Remove inline citation numbers
    # Examples removed:
    #   "[1][2][3]"  "[1,2,3]"  "[1-5]"
    # ─────────────────────────────────────────────────────────
    def _step5_remove_citation_numbers(self, text: str) -> str:
        text = re.sub(r'\[\s*\d+(?:\s*[,\-]\s*\d+)*\s*\]', '', text)
        text = re.sub(r'\(\s*\d+\s*\)', '', text)
        return text

    # ─────────────────────────────────────────────────────────
    # STEP 6 — Fix whitespace and broken words
    # Examples fixed:
    #   "cardio-\nvascular"  →  "cardiovascular"
    #   multiple spaces      →  single space
    #   3+ blank lines       →  1 blank line
    # ─────────────────────────────────────────────────────────
    def _step6_fix_whitespace(self, text: str) -> str:
        # Fix hyphenated line breaks
        text = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)
        # Replace tabs and non-breaking spaces
        text = text.replace('\t', ' ').replace('\xa0', ' ')
        # Multiple blank lines → single blank line
        text = re.sub(r'\n{3,}', '\n\n', text)
        # Multiple spaces → single space
        text = re.sub(r'[ ]{2,}', ' ', text)
        return text.strip()

    # ─────────────────────────────────────────────────────────
    # STEP 7 — Remove paragraphs with no medical content
    # Keeps paragraphs that score 2+ on medical terms
    # Drops paragraphs that are pure publication noise
    # ─────────────────────────────────────────────────────────
    def _step7_remove_noise_paragraphs(self, text: str) -> str:
        paragraphs = re.split(r'\n{2,}', text)
        kept = []
        for para in paragraphs:
            para = para.strip()
            if len(para) < 40:
                continue
            if self._medical_score(para) >= 2:
                kept.append(para)
        return '\n\n'.join(kept)

    # ─────────────────────────────────────────────────────────
    # STEP 8 — Standardise section header names
    # Makes section detection reliable
    # Examples:
    #   "2. METHODS"          →  "METHODS"
    #   "Materials and Methods" →  "METHODS"
    #   "IV. Results"         →  "RESULTS"
    # ─────────────────────────────────────────────────────────
    def _step8_standardise_headers(self, text: str) -> str:
        replacements = [
            (r'(?im)^\s*(?:\d+\.?\s+)?abstract\s*:?\s*$',
             'ABSTRACT'),
            (r'(?im)^\s*(?:\d+\.?\s+)?(?:introduction|background)\s*:?\s*$',
             'INTRODUCTION'),
            (r'(?im)^\s*(?:\d+\.?\s+)?(?:methods?|materials?\s+and\s+methods?'
             r'|study\s+design)\s*:?\s*$',
             'METHODS'),
            (r'(?im)^\s*(?:\d+\.?\s+)?results?\s*:?\s*$',
             'RESULTS'),
            (r'(?im)^\s*(?:\d+\.?\s+)?discussion\s*:?\s*$',
             'DISCUSSION'),
            (r'(?im)^\s*(?:\d+\.?\s+)?conclusions?\s*:?\s*$',
             'CONCLUSION'),
            (r'(?im)^\s*(?:\d+\.?\s+)?limitations?\s*:?\s*$',
             'LIMITATIONS'),
            (r'(?im)^\s*(?:\d+\.?\s+)?recommendations?\s*:?\s*$',
             'RECOMMENDATIONS'),
            (r'(?im)^\s*references?\s*:?\s*$',
             'REFERENCES'),
        ]
        for pattern, replacement in replacements:
            text = re.sub(pattern, f'\n{replacement}\n', text)
        return text

    # ─────────────────────────────────────────────────────────
    # HELPER — Score how medical a piece of text is
    # Used by Step 7 to decide keep or drop
    # Returns score 0-9 (higher = more clinical content)
    # ─────────────────────────────────────────────────────────
    def _medical_score(self, text: str) -> int:
        text_lower = text.lower()
        score = 0

        # Count medical terms (max 5 points)
        hits = sum(1 for term in MEDICAL_TERMS if term in text_lower)
        score += min(hits, 5)

        # Has statistics like p<0.001, HR=0.75, 30% (2 points)
        if re.search(
            r'(?:p\s*[<=>]\s*0\.\d+|\d+\.?\d*\s*%|'
            r'(?:HR|OR|RR|CI)\s*[=:]\s*\d)',
            text, re.IGNORECASE
        ):
            score += 2

        # Has dosages like 2000mg, 1.5kg (1 point)
        if re.search(
            r'\b\d+(?:\.\d+)?\s*(?:mg|kg|ml|mmol|mmHg)\b',
            text, re.IGNORECASE
        ):
            score += 1

        # Has a proper sentence (1 point)
        if re.search(r'[A-Z][^.!?]{20,}[.!?]', text):
            score += 1

        return score