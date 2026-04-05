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
    _collection=_chroma_client.get_collection(COLLECTION_NAME)
    print(f"connected-{_collection.count()}chunks ready")
except Exception as e:
    print(f"failed to connect {e}")
    _collection=None

def retrieve(question:str)->list:
    if _collection is None:
        raise RuntimeError("chromadb is not connected")

    if not question.strip():
        raise ValueError("question cannot be empty")

    ##convert question to a vector of numbers

    question_vector=embed_model.encode(question,normalize_embeddings=True).tolist()

    #search chromadb for the 8 most similar chunks
    results=_collection.query(
        query_embeddings=[question_vector],
        n_results=TOP_K,
        include=["documents","metadatas","distances"]
    )

    chunks=[]
    docs=results["documents"][0]
    metadata=results["metadatas"][0]
    distances=results["distances"][0]

    for i in range(len(docs)):
        similarity=round(1-distances[i]/2,4)

        chunks.append({
            "text":docs[i],
            "score":similarity,
            "source":metadata[i].get("source","Unknown PDF"),
            "page":set(metadata[i].get("page","2"))
        })

    return chunks

if __name__=="__main__":
    test_question="What is the first-line treatment for type 2 diabetes?"
    print(f"\nTest question: '{test_question}'\n")

    chunks=retrieve(test_question)

    for i,chunk in enumerate(chunks,1):
        print(f"Chunk {i} | Score: {chunk['score']} | {chunk['source']} | page {chunk['page']}")
        print(f"  {chunk['text'][:200]}...")
        print()