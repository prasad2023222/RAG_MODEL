import chromadb
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()

CHROMA_PATH="./data/chroma_db"
COLLECTION_NAME="langchain"
EMBED_MODEL="NeuML/pubmedbert-base-embeddings"
TOP_K=8

embed_model=SentenceTransformer(EMBED_MODEL)

_chroma_client=chromadb.PersistentClient(path=CHROMA_PATH)

try:
    _collection


def retriver(question:str)->list:
    if _collection is None:
        raise RuntimeError("chromadb is not connected")

    if not question.strip():
        raise ValueError("question cannot be empty")

    