"""
Genie Client - Databricks AI/BI Genie API Integration
======================================================
Provides conversational access to Genie for natural language querying of data.

API Reference: https://docs.databricks.com/api/workspace/genie
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

import requests

from config import AppConfig


@dataclass
class GenieResponse:
    """Structured response from Genie API."""
    content: str = ""  # Main text/description
    sql_query: Optional[str] = None  # Generated SQL
    data_table: Optional[str] = None  # Markdown table of results
    row_count: int = 0
    suggested_questions: List[str] = field(default_factory=list)
    raw: Optional[dict] = None


@dataclass(frozen=True)
class GenieAnswer:
    """Legacy answer format for backwards compatibility."""
    ok: bool
    text: str
    raw: Optional[dict] = None


class GenieClient:
    """
    Databricks Genie API client with conversation support.
    
    Uses the Genie Conversation API to:
    - Start conversations
    - Send messages and poll for results
    - Parse structured responses (SQL, tables, follow-ups)
    """

    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.conversation_id: Optional[str] = None
        self._base_url = f"{cfg.databricks_host.rstrip('/')}/api/2.0/genie/spaces/{cfg.genie_space_id}"
        self._headers = {"Authorization": f"Bearer {cfg.databricks_token}"} if cfg.databricks_token else {}

    def is_configured(self) -> bool:
        return bool(self.cfg.genie_space_id and self.cfg.databricks_host and self.cfg.databricks_token)

    def reset_conversation(self) -> None:
        """Start a fresh conversation."""
        self.conversation_id = None

    def start_conversation(self, question: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Start a new Genie conversation with an initial question.
        Returns (conversation_id, message_id) or (None, None) on failure.
        """
        if not self.is_configured():
            return None, None

        try:
            resp = requests.post(
                f"{self._base_url}/start-conversation",
                json={"content": question},
                headers=self._headers,
                timeout=30
            )
            if resp.status_code >= 300:
                return None, None
            data = resp.json()
            self.conversation_id = data.get("conversation_id")
            message_id = data.get("message_id")
            return self.conversation_id, message_id
        except Exception:
            return None, None

    def send_message(self, question: str, max_wait: int = 60) -> Tuple[Optional[str], Optional[GenieResponse]]:
        """
        Send a message to Genie and wait for the response.
        Returns (message_id, GenieResponse) or (None, None) on failure.
        """
        if not self.is_configured():
            return None, None

        # Start new conversation if needed
        if not self.conversation_id:
            conv_id, msg_id = self.start_conversation(question)
            if not conv_id:
                return None, None
        else:
            # Continue existing conversation
            try:
                resp = requests.post(
                    f"{self._base_url}/conversations/{self.conversation_id}/messages",
                    json={"content": question},
                    headers=self._headers,
                    timeout=30
                )
                if resp.status_code >= 300:
                    # Conversation may have expired, start fresh
                    self.conversation_id = None
                    return self.send_message(question, max_wait)
                data = resp.json()
                msg_id = data.get("id") or data.get("message_id")
            except Exception:
                return None, None

        # Poll for result
        return msg_id, self._poll_for_result(msg_id, max_wait)

    def _poll_for_result(self, message_id: str, max_wait: int) -> Optional[GenieResponse]:
        """Poll until message is complete or timeout."""
        if not self.conversation_id or not message_id:
            return None

        start = time.time()
        poll_interval = 1.0

        while time.time() - start < max_wait:
            try:
                resp = requests.get(
                    f"{self._base_url}/conversations/{self.conversation_id}/messages/{message_id}",
                    headers=self._headers,
                    timeout=30
                )
                if resp.status_code >= 300:
                    return None

                data = resp.json()
                status = data.get("status", "").upper()

                if status in ("COMPLETED", "COMPLETE", "SUCCEEDED"):
                    return self._parse_response(data)
                elif status in ("FAILED", "ERROR", "CANCELLED"):
                    error = data.get("error", {}).get("message", "Query failed")
                    return GenieResponse(content=f"❌ {error}", raw=data)

                # Still processing, wait and retry
                time.sleep(poll_interval)
                poll_interval = min(poll_interval * 1.2, 3.0)

            except Exception:
                return None

        return GenieResponse(content="⏱️ Request timed out. Please try again.", raw=None)

    def _parse_response(self, data: dict) -> GenieResponse:
        """Parse Genie API response into structured format."""
        response = GenieResponse(raw=data)

        # Extract attachments (contains SQL, results, etc.)
        attachments = data.get("attachments", [])
        
        for att in attachments:
            att_type = att.get("type", "")
            
            # Text content
            if att_type == "TEXT":
                text_content = att.get("text", {})
                response.content = text_content.get("content", "")
                
            # SQL query
            elif att_type == "QUERY":
                query_content = att.get("query", {})
                response.sql_query = query_content.get("query", "")
                
                # Results may be in the query attachment
                result = query_content.get("result", {})
                if result:
                    response.data_table, response.row_count = self._format_result_table(result)
                    
            # Query result
            elif att_type == "QUERY_RESULT":
                result = att.get("query_result", {})
                if result:
                    response.data_table, response.row_count = self._format_result_table(result)

        # Suggested follow-up questions
        followups = data.get("followup_questions", [])
        if isinstance(followups, list):
            response.suggested_questions = [q.get("question", q) if isinstance(q, dict) else str(q) for q in followups[:3]]

        # If no content parsed, use raw message
        if not response.content and not response.data_table:
            response.content = data.get("content", str(data))

        return response

    def _format_result_table(self, result: dict) -> Tuple[str, int]:
        """Format query result as markdown table."""
        columns = result.get("columns", [])
        rows = result.get("data", []) or result.get("rows", [])
        
        if not columns or not rows:
            return "", 0

        # Build markdown table
        col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]
        
        # Header
        header = "| " + " | ".join(col_names) + " |"
        separator = "| " + " | ".join(["---"] * len(col_names)) + " |"
        
        # Rows (limit to 20 for display)
        display_rows = rows[:20]
        row_lines = []
        for row in display_rows:
            if isinstance(row, dict):
                values = [str(row.get(c, "")) for c in col_names]
            else:
                values = [str(v) if v is not None else "" for v in row]
            row_lines.append("| " + " | ".join(values) + " |")
        
        table = "\n".join([header, separator] + row_lines)
        
        if len(rows) > 20:
            table += f"\n\n*...and {len(rows) - 20} more rows*"
        
        return table, len(rows)

    def format_response_markdown(self, response: GenieResponse) -> str:
        """Format response as plain markdown for storage."""
        parts = []
        if response.content:
            parts.append(response.content)
        if response.data_table:
            parts.append(f"\n**Results** ({response.row_count} rows):\n{response.data_table}")
        if response.sql_query:
            parts.append(f"\n```sql\n{response.sql_query}\n```")
        return "\n\n".join(parts) if parts else "No response"

    # Legacy method for backwards compatibility
    def ask(self, question: str) -> GenieAnswer:
        """Legacy ask method - returns simple GenieAnswer."""
        msg_id, response = self.send_message(question)
        
        if response:
            text = self.format_response_markdown(response)
            return GenieAnswer(ok=True, text=text, raw=response.raw)
        else:
            return GenieAnswer(
                ok=False,
                text="Genie is not configured or unavailable. Falling back to local insights.",
                raw=None
            )


def get_genie_client(cfg: AppConfig) -> GenieClient:
    """Factory function to get a Genie client instance."""
    return GenieClient(cfg)
