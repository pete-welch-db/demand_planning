"""
AI Assistant View
==================
Chat interface with Databricks Genie API integration for natural language
querying of demand planning data.

API Reference: https://docs.databricks.com/api/workspace/genie
"""
from __future__ import annotations

import streamlit as st

from components.narrative import render_action_hint
from config import AppConfig
from data.genie_client import GenieClient, GenieResponse, get_genie_client


def render(cfg: AppConfig, use_mock: bool) -> None:
    """Render the AI Assistant page."""
    
    st.markdown("""
    <h1 style="margin-bottom: 8px;">🤖 AI Assistant</h1>
    <p style="font-size: 1rem; color: #6b6b69; margin-bottom: 16px;">
        Chat with Databricks Genie to analyze demand planning data using natural language
    </p>
    """, unsafe_allow_html=True)
    
    _render_chat_interface(cfg)


def _format_genie_response(response: GenieResponse) -> None:
    """Format and display a Genie response with proper styling."""
    
    # Main content/description
    if response.content:
        st.markdown(response.content)
    
    # Data table
    if response.data_table:
        st.markdown(f"**Results** ({response.row_count} rows):")
        st.markdown(response.data_table)
    
    # SQL query in expander (hidden by default like Databricks UI)
    if response.sql_query:
        with st.expander("📝 View SQL Query", expanded=False):
            st.code(response.sql_query, language="sql")
    
    # Suggested follow-up questions
    if response.suggested_questions:
        st.markdown("---")
        st.markdown("**💡 Suggested follow-ups:**")
        for q in response.suggested_questions:
            if st.button(f"→ {q}", key=f"suggest_{hash(q)}", use_container_width=True):
                st.session_state.pending_question = q
                st.session_state.messages.append({"role": "user", "content": q})
                st.rerun()


def _render_chat_interface(cfg: AppConfig) -> None:
    """Render the chat interface with Genie API integration."""
    
    # Initialize session state
    if "messages" not in st.session_state:
        genie_status = "connected to your Genie space" if cfg.genie_space_id else "running in demo mode (no Genie space configured)"
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": f"""👋 Hello! I'm your AI Assistant, {genie_status}.

I can help you analyze demand planning data. Try asking:

• "What's our overall OTIF rate this month?"
• "Which region has the worst forecast accuracy?"
• "Show me the top 5 plants by freight cost per ton"
• "Which SKU families have the highest late-risk probability?"

Ask me anything about your supply chain data!""",
                "type": "intro"
            }
        ]
    
    if "genie_client" not in st.session_state:
        st.session_state.genie_client = get_genie_client(cfg)
    
    if "processing" not in st.session_state:
        st.session_state.processing = False
    
    if "pending_question" not in st.session_state:
        st.session_state.pending_question = None
    
    # Check for pending question from quick buttons
    pending_prompt = st.session_state.pending_question
    if pending_prompt:
        st.session_state.pending_question = None  # Clear immediately
    
    # Display chat messages
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg.get("type") == "genie_response" and "response_obj" in msg:
                # Render structured Genie response
                _format_genie_response(msg["response_obj"])
            else:
                # Render plain text
                st.markdown(msg["content"])
    
    # Quick Questions - show above chat input if no conversation yet
    if len(st.session_state.messages) <= 1:
        st.markdown("##### ⚡ Quick Questions")
        
        quick_questions = [
            ("📊 OTIF Trend", "Which region is trending down in OTIF over the last 13 weeks?"),
            ("🎯 Accuracy", "Which SKU family has the worst forecast accuracy (MAPE)?"),
            ("🚚 Freight", "Show the top 5 plant→DC lanes by freight cost per ton"),
            ("⚠️ Late Risk", "Which orders have the highest late-risk probability this week?"),
            ("📈 Demand", "Compare actual vs forecast demand by region this month"),
        ]
        
        cols = st.columns(5)
        for i, (label, question) in enumerate(quick_questions):
            with cols[i]:
                if st.button(label, key=f"quick_{label}", use_container_width=True):
                    st.session_state.pending_question = question
                    st.session_state.messages.append({"role": "user", "content": question})
                    st.rerun()
    
    # Get prompt from either chat input or pending question
    chat_prompt = st.chat_input("Ask about demand planning data...", disabled=st.session_state.processing)
    prompt = pending_prompt or chat_prompt
    
    # Chat input
    if prompt:
        # Add user message if not already added by quick button
        if not any(m.get("content") == prompt and m.get("role") == "user" for m in st.session_state.messages[-2:]):
            st.session_state.messages.append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Get Genie response
        with st.chat_message("assistant"):
            status_placeholder = st.empty()
            status_placeholder.markdown("🔄 *Sending request to Genie...*")
            
            st.session_state.processing = True
            
            try:
                client: GenieClient = st.session_state.genie_client
                
                if not client.is_configured():
                    status_placeholder.empty()
                    fallback_msg = """⚠️ **Genie is not configured** (missing `GENIE_SPACE_ID`).

To enable the AI Assistant:
1. Create a Genie space in Databricks with your Gold tables
2. Add `GENIE_SPACE_ID` to your environment variables
3. Restart the app

For now, use the **Control Tower** and **What-If Scenarios** tabs to explore your data."""
                    st.markdown(fallback_msg)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": fallback_msg
                    })
                else:
                    status_placeholder.markdown("⏳ *Analyzing your question (this may take up to 30 seconds)...*")
                    
                    message_id, response = client.send_message(prompt)
                    
                    status_placeholder.empty()
                    
                    if response:
                        # Display the formatted response
                        _format_genie_response(response)
                        
                        # Store in messages
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": client.format_response_markdown(response),
                            "type": "genie_response",
                            "response_obj": response
                        })
                    else:
                        error_msg = "Sorry, I couldn't get a response. Please try again."
                        st.markdown(error_msg)
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": error_msg
                        })
                        
            except Exception as e:
                status_placeholder.empty()
                error_msg = f"Error connecting to Genie: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"⚠️ {error_msg}"
                })
            finally:
                st.session_state.processing = False
    
    # Action buttons at bottom
    st.divider()
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("🗑️ Clear Chat", key="clear_chat_btn", use_container_width=True):
            st.session_state.messages = [st.session_state.messages[0]]
            st.session_state.genie_client.reset_conversation()
            st.rerun()
    
    with col2:
        if st.button("🔄 New Conversation", key="new_convo_btn", use_container_width=True):
            st.session_state.genie_client.reset_conversation()
            st.toast("Started new conversation", icon="✅")
    
    with col3:
        # Tips expander inline
        with st.expander("💡 Tips for Better Results"):
            st.markdown("""
**Be specific about time:** "last 13 weeks", "this month", "YTD"

**Name the tables:** `control_tower_weekly`, `weekly_demand_actual`, `order_late_risk_scored_ml`

**Specify grouping:** "by region", "by SKU family", "by plant→DC lane"

**Ask for output shape:** "top 10", "show trend", "include summary"
            """)
    
    # Link to Genie space if configured
    if cfg.genie_space_id and cfg.databricks_host:
        st.caption(f"[Open Genie Space in Databricks →]({cfg.databricks_host.rstrip('/')}/genie/spaces/{cfg.genie_space_id})")
    
    render_action_hint(
        title="Action this enables",
        body="Use natural language to quickly explore data, validate hypotheses, and surface insights—then take action in the Control Tower or What-If Scenarios tabs.",
    )
