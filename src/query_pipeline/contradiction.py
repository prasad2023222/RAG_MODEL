import os
import json
import re 
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

GROQ_MODEL="qwen/qwen3-32b"
MAX_TOKENS=1500
TEMPERATURE=0.05

SYSTEM_PROMPT = """You are a medical contradiction detector.
 
Your job: look at the provided research chunks and identify REAL contradictions.
 
REAL contradiction = two chunks that:
- Recommend OPPOSITE treatments for the same patient type
- Give CONFLICTING numbers (e.g. "dose 500mg" vs "dose 1000mg")
- Have OPPOSING conclusions about the same drug or therapy
 
NOT a contradiction:
- Two chunks saying the same thing in different words
- An old study superseded by a newer guideline
- Studies on different patient populations
 
RETURN ONLY VALID JSON — no extra text, no markdown fences, just the JSON:
{
  "found": true or false,
  "count": number of contradictions found,
  "conflicts": [
    {
      "topic": "brief topic e.g. HbA1c target",
      "claim_a": "what one chunk says (include which chunk number)",
      "claim_b": "what the conflicting chunk says (include which chunk number)",
      "severity": "HIGH or MEDIUM or LOW",
      "root_cause": "why they conflict — choose one: Different study populations | Different time periods | Different study designs | Different outcome measures | Evolving evidence base | Regional guideline variation",
      "clinical_implication": "what should the doctor do given this conflict"
    }
  ],
  "overall_consistency": "HIGH or MEDIUM or LOW",
  "analyst_note": "one sentence summary of the evidence quality"
}
"""

_groq_client=Groq(api_key=os.getenv("GROQ_API_KEY",""))

def detect_contradictions(question:str,chunks:list)->dict:

    if len(chunks) < 2:
        return {
            "found": False,
            "count": 0,
            "conflicts": [],
            "overall_consistency": "N/A",
            "analyst_note": "Only one chunk retrieved — cannot compare for contradictions."
        }

     # Format chunks for the AI
    chunks_block = ""
    for i, chunk in enumerate(chunks, 1):
        chunks_block += f"\n[Chunk {i}] Source: {chunk['source']}\n"
        chunks_block += chunk["text"]
        chunks_block += "\n" + "─" * 40 + "\n"
 
    user_message = f"""Doctor's question: {question}
 
Analyse these {len(chunks)} chunks for contradictions:
{chunks_block}
 
Return only JSON, no other text."""


    try:
        response = _groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_message},
            ],
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
        )
 
        raw_text = response.choices[0].message.content
 
        # Sometimes AI wraps JSON in ```json ... ``` — strip that out
        cleaned = re.sub(r"```(?:json)?", "", raw_text).strip().strip("`").strip()
 
        return json.loads(cleaned)
 
    except json.JSONDecodeError:
        # If AI didn't return valid JSON, return a safe default
        return {
            "found": False,
            "count": 0,
            "conflicts": [],
            "overall_consistency": "UNKNOWN",
            "analyst_note": "Contradiction analysis could not be parsed. Please try again."
        }
    except Exception as e:
        return {
            "found": False,
            "count": 0,
            "conflicts": [],
            "overall_consistency": "UNKNOWN",
            "analyst_note": f"Analysis failed: {str(e)}"
        }
 
if __name__ == "__main__":
    fake_chunks = [
        {
            "text": "The ACCORD trial (n=10,251) found that targeting HbA1c below 6.5% significantly increased cardiovascular mortality compared to standard glucose control.",
            "source": "ACCORD_Trial_2008.pdf",
            "page": "3",
            "score": 0.91,
        },
        {
            "text": "ADA 2023 guidelines recommend an HbA1c target of below 7.0% for most non-pregnant adults with type 2 diabetes to reduce microvascular complications.",
            "source": "ADA_Standards_2023.pdf",
            "page": "15",
            "score": 0.88,
        },
        {
            "text": "The UKPDS trial showed intensive glucose lowering targeting HbA1c below 7% significantly reduced microvascular complications over 10 years.",
            "source": "UKPDS_Trial_1998.pdf",
            "page": "7",
            "score": 0.85,
        },
    ]
 
    question = "What HbA1c target should I set for my type 2 diabetes patient?"
    print(f"Question: {question}\n")
    print("Checking for contradictions...\n")
 
    result = detect_contradictions(question, fake_chunks)
    print(json.dumps(result, indent=2))
 