"""
Ask WellNest — RAG-powered Q&A over federal education and health policy.

Hits the FastAPI /api/ask endpoint, which runs a LangChain retrieval chain
against FAISS-indexed policy documents. The chat interface persists across
reruns via session state.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import requests
import streamlit as st

from dashboard.utils.cache import TTLCache

st.set_page_config(
    page_title="Ask WellNest | WellNest",
    page_icon="https://em-content.zobj.net/source/twitter/408/seedling_1f331.png",
    layout="wide",
)

st.title("Ask WellNest")
st.markdown(
    '<p style="font-size:15px;color:#636E72;margin-top:-10px;margin-bottom:20px">'
    "Ask questions about child wellbeing, education policy, and health resources. "
    "Answers are grounded in federal policy documents via RAG.</p>",
    unsafe_allow_html=True,
)

API_BASE = os.getenv("WELLNEST_API_URL", "http://localhost:8000")
API_KEY = os.getenv("WELLNEST_API_KEY", "")
answer_cache = TTLCache(namespace="ask_wellnest", ttl_seconds=300)


def _ask_api(question: str) -> Optional[dict]:
    """Post a question to the RAG endpoint and return the response."""
    try:
        headers = {"Content-Type": "application/json"}
        if API_KEY:
            headers["X-API-Key"] = API_KEY

        resp = requests.post(
            f"{API_BASE}/api/ask",
            json={"question": question},
            headers=headers,
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()
        else:
            return {"error": f"API returned {resp.status_code}: {resp.text[:200]}"}
    except requests.RequestException as exc:
        return {"error": f"Could not reach the API: {str(exc)[:150]}"}


# ---------------------------------------------------------------------------
# Example questions
# ---------------------------------------------------------------------------

with st.sidebar:
    st.subheader("Example Questions")
    st.markdown(
        '<div style="font-size:13px;color:#636E72;margin-bottom:12px">'
        "Click any question to ask it:</div>",
        unsafe_allow_html=True,
    )

    example_questions = [
        "What are the Title I eligibility requirements?",
        "How does ESSA define 'evidence-based interventions'?",
        "What federal programs fund school-based mental health?",
        "What are the CDC recommendations for school health programs?",
        "How do HPSA designations affect healthcare funding?",
        "What environmental health standards apply to schools?",
    ]

    for q in example_questions:
        if st.button(q, key=f"ex_{hash(q)}", use_container_width=True):
            st.session_state.pending_question = q


# ---------------------------------------------------------------------------
# Chat history
# ---------------------------------------------------------------------------

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


# ---------------------------------------------------------------------------
# Chat display
# ---------------------------------------------------------------------------

for entry in st.session_state.chat_history:
    role = entry["role"]
    content = entry["content"]

    if role == "user":
        st.markdown(
            f'<div style="background:#2E86AB;color:#fff;padding:10px 16px;'
            f'border-radius:12px 12px 4px 12px;margin-bottom:10px;'
            f'max-width:80%;margin-left:auto;font-size:14px">'
            f"{content}</div>",
            unsafe_allow_html=True,
        )
    else:
        sources_html = ""
        if entry.get("sources"):
            source_items = "".join(
                f'<li style="margin-bottom:2px">{s}</li>'
                for s in entry["sources"]
            )
            sources_html = (
                f'<div style="margin-top:10px;padding-top:8px;'
                f'border-top:1px solid #E0E4EA;font-size:12px;color:#636E72">'
                f"<b>Sources:</b>"
                f"<ul style='margin:4px 0 0 16px;padding:0'>{source_items}</ul>"
                f"</div>"
            )

        st.markdown(
            f'<div style="background:#FFFFFF;border:1px solid #E0E4EA;'
            f'padding:12px 16px;border-radius:12px 12px 12px 4px;'
            f'margin-bottom:10px;max-width:85%;font-size:14px;'
            f'line-height:1.6;color:#2D3436">'
            f"{content}"
            f"{sources_html}"
            f"</div>",
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Input
# ---------------------------------------------------------------------------

pending = st.session_state.pop("pending_question", None)

question = st.chat_input("Ask a question about child wellbeing or education policy...")

active_question = pending or question

if active_question:
    st.session_state.chat_history.append({"role": "user", "content": active_question})

    with st.spinner("Searching policy documents..."):
        cached = answer_cache.get(active_question)
        if cached:
            result = cached
        else:
            result = _ask_api(active_question)
            if result and "error" not in result:
                answer_cache.set(active_question, result)

    if result and "error" not in result:
        answer_text = result.get("answer", "No answer returned.")
        sources = result.get("sources", [])

        st.session_state.chat_history.append({
            "role": "assistant",
            "content": answer_text,
            "sources": sources,
        })
    elif result:
        error_msg = result.get("error", "Unknown error")
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": f"Sorry, I couldn't process that question. {error_msg}",
        })
    else:
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": "No response from the API. Is the backend running?",
        })

    st.rerun()


# ---------------------------------------------------------------------------
# Clear chat
# ---------------------------------------------------------------------------

if st.session_state.chat_history:
    st.markdown("---")
    if st.button("Clear conversation"):
        st.session_state.chat_history = []
        st.rerun()

if not st.session_state.chat_history:
    st.markdown(
        '<div style="text-align:center;padding:60px 0;color:#B2BEC3;font-size:15px">'
        "Ask a question to get started. Answers are grounded in federal "
        "education and health policy documents.</div>",
        unsafe_allow_html=True,
    )
