from src.data_pipeline.extractor import extract_pdf

result = extract_pdf(r"D:\MedRag\data\pdfs\cardiology\accord_trial.pdf")

print(result["metadata"])       # just metadata
print(result["full_text"][:500]) # first 500 chars of text
print(result["tables"])          # all tables
print(result["pages"])           # page count