"""
src/data_pipeline/medical_cleaner.py
ONE JOB: Clean raw PDF text - remove noise, keep clinical content
"""

import re

MEDICAL_TERMS = {
    "randomized","controlled","trial","placebo","double-blind",
    "meta-analysis","systematic","confidence","interval","hazard",
    "ratio","odds","mean","median","patient","participants",
    "dose","mg","kg","mmhg","treatment","therapy","efficacy",
    "safety","adverse","outcome","mortality","survival","clinical",
    "cardiovascular","cardiac","heart","hypertension","stroke",
    "systolic","diastolic","cholesterol","statin","aspirin",
    "diabetes","insulin","glucose","hba1c","glycemic","metformin",
    "sglt2","cancer","tumor","tumour","chemotherapy","immunotherapy",
    "pembrolizumab","nivolumab","blood","pressure","reduction",
    "significant","primary","secondary","endpoint","follow-up",
}


class MedicalCleaner:

    def clean(self, raw_text: str) -> str:
        """Run all cleaning steps. Returns clean text."""
        if not raw_text or len(raw_text) < 50:
            return raw_text
        text = raw_text
        text = self._remove_headers_footers(text)
        text = self._remove_author_block(text)
        text = self._remove_copyright(text)
        text = self._remove_figure_refs(text)
        text = self._remove_citation_numbers(text)
        text = self._fix_whitespace(text)
        text = self._standardise_headers(text)
        # NOTE: We do NOT filter paragraphs here anymore
        # Filtering happens in pdf_loader after chunking
        return text.strip()

    def _remove_headers_footers(self, text):
        patterns = [
            r'(?i)ISSN[\s\-:]?\d{4}[\-]\d{4}',
            r'(?i)Vol(?:ume)?\.?\s*\d+.*?Issue.*?\d+',
            r'(?i)downloaded\s+by\s+\S+\s+on\s+.{0,50}',
            r'(?m)^\s*Page\s+\d+\s+of\s+\d+\s*$',
            r'(?i)(?:©|copyright)\s+\d{4}\s+\S+',
            r'(?i)all\s+rights\s+reserved\.?',
            r'(?i)published\s+by\s+(?:Elsevier|Springer|Wiley|Oxford|Nature)\S*',
        ]
        for pat in patterns:
            text = re.sub(pat, ' ', text)
        return text

    def _remove_author_block(self, text):
        patterns = [
            r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b',
            r'(?i)(?:corresponding|correspondence)\s+author.{0,120}',
            r'(?i)received\s*:?\s*\w+\s+\d+[,\s]+\d{4}',
            r'(?i)accepted\s*:?\s*\w+\s+\d+[,\s]+\d{4}',
            r'ORCID\s*:?\s*[\d\-X]{19}',
        ]
        for pat in patterns:
            text = re.sub(pat, ' ', text, flags=re.IGNORECASE)
        return text

    def _remove_copyright(self, text):
        patterns = [
            r'(?i)this\s+is\s+an?\s+open.access\s+article.{0,200}',
            r'(?i)(?:CC\s+BY|Creative\s+Commons).{0,100}',
            r'(?i)the\s+authors?\s+declare\s+no\s+competing\s+interests?.{0,100}',
            r'(?i)for\s+personal\s+use\s+only',
            r'(?i)not\s+for\s+redistribution',
        ]
        for pat in patterns:
            text = re.sub(pat, ' ', text, flags=re.DOTALL)
        return text

    def _remove_figure_refs(self, text):
        patterns = [
            r'\((?:see\s+)?[Ff]ig(?:ure)?\.?\s*\d+[a-zA-Z]?\)',
            r'(?i)\(?\s*[Ii]nsert\s+[Ff]ig(?:ure)?\s*\d+\s*here\s*\)?',
            r'(?i)\bin\s+[Ff]ig(?:ure)?\.?\s*\d+',
        ]
        for pat in patterns:
            text = re.sub(pat, ' ', text)
        return text

    def _remove_citation_numbers(self, text):
        text = re.sub(r'\[\s*\d+(?:\s*[,\-]\s*\d+)*\s*\]', '', text)
        return text

    def _fix_whitespace(self, text):
        text = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)
        text = text.replace('\t', ' ').replace('\xa0', ' ')
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ ]{2,}', ' ', text)
        return text.strip()

    def _standardise_headers(self, text):
        replacements = [
            (r'(?im)^\s*(?:\d+\.?\s+)?abstract\s*:?\s*$',                   '\nABSTRACT\n'),
            (r'(?im)^\s*(?:\d+\.?\s+)?(?:introduction|background)\s*:?\s*$', '\nINTRODUCTION\n'),
            (r'(?im)^\s*(?:\d+\.?\s+)?(?:methods?|materials?\s+and\s+methods?|study\s+design)\s*:?\s*$', '\nMETHODS\n'),
            (r'(?im)^\s*(?:\d+\.?\s+)?results?\s*:?\s*$',                   '\nRESULTS\n'),
            (r'(?im)^\s*(?:\d+\.?\s+)?discussion\s*:?\s*$',                 '\nDISCUSSION\n'),
            (r'(?im)^\s*(?:\d+\.?\s+)?conclusions?\s*:?\s*$',               '\nCONCLUSION\n'),
            (r'(?im)^\s*(?:\d+\.?\s+)?limitations?\s*:?\s*$',               '\nLIMITATIONS\n'),
            (r'(?im)^\s*(?:\d+\.?\s+)?recommendations?\s*:?\s*$',           '\nRECOMMENDATIONS\n'),
            (r'(?im)^\s*references?\s*:?\s*$',                               '\nREFERENCES\n'),
        ]
        for pattern, replacement in replacements:
            text = re.sub(pattern, replacement, text)
        return text

    def is_medical_chunk(self, text: str) -> bool:
        """Returns True if chunk has enough medical content to keep."""
        text_lower = text.lower()
        hits = sum(1 for term in MEDICAL_TERMS if term in text_lower)
        if hits >= 2:
            return True
        if re.search(r'(?:p\s*[<=>]\s*0\.\d+|(?:HR|OR|RR|CI)\s*[=:]\s*\d|\d+\.?\d*\s*%)', text, re.IGNORECASE):
            return True
        if re.search(r'\b\d+(?:\.\d+)?\s*(?:mg|kg|ml|mmol|mmHg)\b', text, re.IGNORECASE):
            return True
        return False