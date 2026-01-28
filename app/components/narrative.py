from __future__ import annotations

import streamlit as st


def render_tab_intro(persona: str, business_question: str, context: str | None = None) -> None:
    """
    Required on every tab:
    - persona framing
    - explicit business question
    - optional 1–2 line context
    """
    st.markdown(
        f"""
<div class="tab-intro">
  <div class="tab-intro-persona">{persona}</div>
  <div class="tab-intro-question">{business_question}</div>
  {f'<div class="tab-intro-context">{context}</div>' if context else ''}
</div>
        """,
        unsafe_allow_html=True,
    )


def render_chart_annotation(title: str, body: str) -> None:
    st.markdown(
        f"""
<div class="callout callout-annot">
  <div class="callout-title">{title}</div>
  <div class="callout-body">{body}</div>
</div>
        """,
        unsafe_allow_html=True,
    )


def render_action_hint(title: str, body: str) -> None:
    """
    Required when charts are present: "what decision this enables".
    """
    st.markdown(
        f"""
<div class="callout callout-action">
  <div class="callout-title">{title}</div>
  <div class="callout-body">{body}</div>
</div>
        """,
        unsafe_allow_html=True,
    )

