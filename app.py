# app.py
import os
import gradio as gr
from dotenv import load_dotenv

load_dotenv()

from src.query_pipeline.pipeline import run_pipeline


def get_system_info():
    try:
        import chromadb
        client     = chromadb.PersistentClient("./data/chroma_db")
        collection = client.get_collection("langchain")
        count      = collection.count()
        db_info    = f"✅ Connected — **{count:,} chunks** in database"
    except Exception as e:
        db_info = f"❌ Error: `{e}`"

    groq_ok      = "✅ Set" if os.getenv("GROQ_API_KEY")      else "❌ Missing"
    langsmith_ok = "✅ Set" if os.getenv("LANGSMITH_API_KEY") else "⚠️ Not set"

    return f"""## ⚙️ System Status

### ChromaDB
{db_info}
- Collection: `langchain`  |  Path: `./data/chroma_db`

### API Keys
- **GROQ_API_KEY:** {groq_ok}
- **LANGSMITH_API_KEY:** {langsmith_ok}

### Models
- **Embedding:** `NeuML/pubmedbert-base-embeddings`
- **LLM:** `llama3-70b-8192` via Groq

### Pipeline Steps
1. retriever.py → searches ChromaDB, returns 8 chunks
2. summarizer.py → Groq AI summarises the chunks
3. contradiction.py → finds conflicts between chunks
4. compiler.py → builds structured answer

### Architecture
- Layer A ✅ ChromaDB built
- Layer B ✅ Live query pipeline (this app)
- Layer C ⏳ HuggingFace deployment
"""


def handle_query(question, chat_history):
    if not question.strip():
        yield chat_history, "_Please type a question._", "_No data._", "_No data._"
        return

    loading = (
        "⏳ Running pipeline...\n\n"
        "Step 1/4: Searching ChromaDB...\n"
        "Step 2/4: Summarising with Groq LLaMA3...\n"
        "Step 3/4: Detecting contradictions...\n"
        "Step 4/4: Compiling answer..."
    )

    yield (
        chat_history + [[question, loading]],
        "⏳ Processing...",
        "⏳ Processing...",
        "⏳ Processing...",
    )

    try:
        answer = run_pipeline(question)

        summary_md   = answer.get("summary_md",   "_No summary._")
        conflicts_md = answer.get("conflicts_md",  "_No conflicts._")
        sources_md   = answer.get("sources_md",    "_No sources._")
        full_answer  = answer.get("full_answer",   "_No output._")

        yield (
            chat_history + [[question, summary_md]],
            full_answer,
            conflicts_md,
            sources_md,
        )

    except Exception as e:
        err = (
            f"❌ **Error:** {str(e)}\n\n"
            f"- Check GROQ_API_KEY in .env\n"
            f"- Run from D:\\MedRag\\ folder\n"
            f"- Run `python test_db.py` to verify ChromaDB"
        )
        yield chat_history + [[question, err]], err, "", ""


with gr.Blocks(title="MedRag") as app:

    gr.Markdown("# 🏥 MedRag — Medical Evidence & Contradiction Detection\nAsk a medical question → search 28 research papers → get structured answer with contradiction alerts")

    with gr.Row():
        with gr.Column(scale=5):
            question_input = gr.Textbox(
                label="Your Medical Question",
                placeholder="e.g. What is the recommended HbA1c target for type 2 diabetes?",
                lines=2,
            )
        with gr.Column(scale=1):
            submit_button = gr.Button("🔍 Search", variant="primary")

    gr.Examples(
        examples=[
            "What is the recommended HbA1c target for type 2 diabetes?",
            "What is the first-line treatment for non-small cell lung cancer?",
            "Compare statins vs fibrates for cardiovascular risk reduction.",
            "Does intensive blood pressure control reduce stroke risk?",
            "What are the benefits of SGLT2 inhibitors beyond glucose control?",
        ],
        inputs=question_input,
        label="📌 Click an example to try:",
    )

    with gr.Tabs():

        with gr.TabItem("💬 Chat"):
            chatbot   = gr.Chatbot(label="Evidence-Based Chat", height=500)
            clear_btn = gr.Button("🗑️ Clear chat", variant="secondary")

        with gr.TabItem("📄 Full Report"):
            full_report = gr.Markdown("_Submit a question to see the full report._")

        with gr.TabItem("⚔️ Conflicts"):
            conflicts_display = gr.Markdown("_Submit a question to see contradiction analysis._")

        with gr.TabItem("📚 Sources"):
            sources_display = gr.Markdown("_Submit a question to see cited sources._")

        with gr.TabItem("⚙️ System"):
            system_display = gr.Markdown(get_system_info())
            gr.Button("🔄 Refresh").click(fn=get_system_info, outputs=system_display)

    submit_button.click(
        fn=handle_query,
        inputs=[question_input, chatbot],
        outputs=[chatbot, full_report, conflicts_display, sources_display],
    )
    question_input.submit(
        fn=handle_query,
        inputs=[question_input, chatbot],
        outputs=[chatbot, full_report, conflicts_display, sources_display],
    )
    clear_btn.click(fn=lambda: [], outputs=chatbot)


if __name__ == "__main__":
    print("="*60)
    print("🚀 Starting MedRag...")
    print("📖 Open browser at: http://localhost:7860")
    print("="*60)
    app.launch(server_name="0.0.0.0", server_port=7860, show_error=True)