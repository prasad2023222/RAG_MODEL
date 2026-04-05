import os 
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

GROQ_MODEL="qwen/qwen3-32b"
MAX_TOKENS=1024
TEMPERATURE=0.1

SYSTEM_PROMPT = """You are a medical evidence summariser helping doctors make decisions.
 
STRICT RULES — you must follow all of these:
1. ONLY use information from the chunks provided below. Never add your own knowledge.
2. Cite every claim using [Source: filename] so the doctor knows where it came from.
3. If two chunks say opposite things, mention BOTH — never pick one over the other.
4. Use clear, professional medical language suitable for a doctor.
5. If the chunks don't contain enough information to answer, say so clearly.
 
Output format — always use this exact structure:
 
## Summary
(2-3 paragraphs summarising the key findings. Include [Source: filename] after each fact.)
 
## Key Points
- Key point 1 [Source: filename]
- Key point 2 [Source: filename]
- Key point 3 [Source: filename]
(maximum 5 bullet points)
"""

_groq_client=Groq(api_key=os.getenv("GROQ_API_KEY",""))


def summarize(question:str,chunks:list)->str:
    if not chunks:
        return "## Summary\n\nNo relevant information was found in the database for this question."


#format all the chunks into numbered block
#this is what gets sent to AI along with question
    chunks_block=""

    for i,chunk in enumerate(chunks,1):
        chunks_block+=f"\n[chunk{i}] source:{chunk["source"]}|Page:{chunk['page']}\n"
        chunks_block+=chunk["text"]
        chunks_block+="\n"+"-"*40+"\n"

 # Build the full message for the AI
    user_message=f"""Doctor's question:{question} here are {len(chunks)} chunks retrieved from mediacl research papers:{chunks_block}
                   please write your summary following your strict rules."""


 # Send to Groq and get the reply
    response=_groq_client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role":"system","content":SYSTEM_PROMPT},
             {"role":"user","content":user_message},
        ],
        max_tokens=MAX_TOKENS,
         temperature=TEMPERATURE,
     )

    return response.choices[0].message.content


if __name__=="__main__":
    fake_chunks = [
        {
            "text": "Metformin is the recommended first-line pharmacological therapy for most patients with type 2 diabetes due to its proven efficacy, safety profile, and low cost.",
            "source": "ADA_Guidelines_2023.pdf",
            "page": "12",
            "score": 0.91,
        },
        {
            "text": "SGLT2 inhibitors are recommended for patients with type 2 diabetes and established cardiovascular disease or high cardiovascular risk to reduce cardiac events.",
            "source": "ESC_Diabetes_Guidelines_2023.pdf",
            "page": "8",
            "score": 0.87,
        },
        {
            "text": "GLP-1 receptor agonists should be considered in overweight or obese patients with type 2 diabetes due to additional weight loss benefits.",
            "source": "WHO_T2DM_Management_2022.pdf",
            "page": "22",
            "score": 0.83,
        },
    ]
 
    question = "What is the first-line treatment for type 2 diabetes?"
    print(f"Question: {question}\n")
    print("Sending to Groq AI...\n")
 
    result = summarize(question, fake_chunks)
    print(result)