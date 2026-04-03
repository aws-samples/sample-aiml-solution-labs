"""Relevancy steering handler with LLM judge + Bedrock Guardrails scoring.

Runs two checks:
1. Before KB tool call: LLM judge verifies the query matches the user's intent
2. After final response: LLM judge + Guardrails ApplyGuardrail score the response

Configurable via environment variables:
  STEERING_MODE            - "passive" (log only) or "active" (retry on low scores)
  STEERING_RETRY_THRESHOLD - Score below this triggers retry (default: 0.6)
  STEERING_MAX_RETRIES     - Max retries before accepting (default: 2)
  STEERING_GUARDRAIL_ID    - Bedrock Guardrail ID for contextual grounding
  STEERING_GUARDRAIL_VERSION - Guardrail version (default: DRAFT)
  STEERING_LOG_BUCKET      - S3 bucket for score logs
"""

import os
import json
import logging
import uuid
from pydantic import BaseModel
from strands import Agent
from strands.models import BedrockModel
from strands.vended_plugins.steering.core.handler import SteeringHandler
from strands.vended_plugins.steering.core.action import (
    Proceed,
    Guide,
    ToolSteeringAction,
    ModelSteeringAction,
)
from strands.vended_plugins.steering.context_providers.ledger_provider import LedgerProvider
from relevancy_logger import log_score

logger = logging.getLogger(__name__)

# Config from env
MODE = os.environ.get("STEERING_MODE", "passive")
RETRY_THRESHOLD = float(os.environ.get("STEERING_RETRY_THRESHOLD", "0.6"))
MAX_RETRIES = int(os.environ.get("STEERING_MAX_RETRIES", "2"))
GUARDRAIL_ID = os.environ.get("STEERING_GUARDRAIL_ID", "")
GUARDRAIL_VERSION = os.environ.get("STEERING_GUARDRAIL_VERSION", "DRAFT")
REGION = os.environ.get("AWS_REGION", "us-west-2")

# Tools we want to intercept
KB_TOOLS = {"call_pricing_search_agent", "call_bedrock_quota_agent"}

# Judge model — use a cheap/fast model
JUDGE_MODEL_ID = os.environ.get(
    "STEERING_JUDGE_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0"
)

_bedrock_rt = None


def _get_bedrock_rt():
    global _bedrock_rt
    if _bedrock_rt is None:
        import boto3
        _bedrock_rt = boto3.client("bedrock-runtime", region_name=REGION)
    return _bedrock_rt


class JudgeScore(BaseModel):
    score: float
    reason: str


class RelevancySteeringHandler(SteeringHandler):
    """Steering handler that scores relevancy via LLM judge + Guardrails."""

    def __init__(self):
        super().__init__(context_providers=[LedgerProvider()])
        self._retry_count = 0
        self._user_query = ""
        self._kb_results = ""
        self._session_id = "unknown"
        self._invocation_id = ""
        self._judge = None

    def _get_judge(self) -> Agent:
        """Lazy-init a reusable judge agent."""
        if self._judge is None:
            self._judge = Agent(
                model=BedrockModel(model_id=JUDGE_MODEL_ID, temperature=0.0),
                system_prompt=(
                    "You evaluate relevancy and faithfulness of AI agent behavior. "
                    "Score 0.0-1.0. Check entity names, model names, service names, "
                    "regions, numbers — any mismatch is a low score."
                ),
            )
        return self._judge

    async def steer_before_tool(self, *, agent, tool_use, **kwargs) -> ToolSteeringAction:
        """Check KB query relevancy before the tool executes."""
        tool_name = tool_use.get("name", "")
        if tool_name not in KB_TOOLS:
            return Proceed(reason="Not a KB tool")

        # Extract user's original query from conversation
        self._user_query = self._extract_user_query(agent)
        tool_query = tool_use.get("input", {}).get("query", "")
        self._session_id = getattr(agent, "_steering_session_id", "unknown")
        if self._retry_count == 0:
            self._invocation_id = str(uuid.uuid4())[:8]

        # LLM judge: does the tool query match the user's intent?
        score, reason = await self._judge_query_relevancy(self._user_query, tool_query)

        log_score(
            session_id=self._session_id,
            check_type="before_kb_call",
            invocation_id=self._invocation_id,
            tool_name=tool_name,
            user_query=self._user_query,
            tool_query=tool_query,
            llm_judge_score=score,
            mode=MODE,
            retry_count=self._retry_count,
            details=reason,
        )

        if MODE == "active" and score < RETRY_THRESHOLD and self._retry_count < MAX_RETRIES:
            self._retry_count += 1
            return Guide(reason=f"KB query may not match user intent (score: {score:.2f}). "
                                f"User asked: '{self._user_query}'. "
                                f"You're querying: '{tool_query}'. {reason}")

        return Proceed(reason=f"Query relevancy score: {score:.2f}")

    async def steer_after_model(self, *, agent, message, stop_reason, **kwargs) -> ModelSteeringAction:
        """Score final response with LLM judge + Guardrails."""
        if stop_reason != "end_turn":
            return Proceed(reason="Not a final response")

        response_text = " ".join(
            b.get("text", "") for b in message.get("content", []) if "text" in b
        )
        if not response_text.strip():
            return Proceed(reason="Empty response")

        # Collect KB results from ledger
        ctx = self.steering_context.data.get()
        ledger = ctx.get("ledger", {})
        tool_calls = ledger.get("tool_calls", [])
        self._kb_results = self._extract_kb_results(tool_calls)

        # 1. LLM judge score
        judge_score, judge_reason = await self._judge_response_relevancy(
            self._user_query, self._kb_results, response_text
        )

        # 2. Guardrails contextual grounding scores
        gr_grounding, gr_relevance = self._guardrail_score(
            self._kb_results, self._user_query, response_text
        )

        log_score(
            session_id=self._session_id,
            check_type="after_response",
            invocation_id=self._invocation_id,
            user_query=self._user_query,
            llm_judge_score=judge_score,
            guardrail_grounding=gr_grounding,
            guardrail_relevance=gr_relevance,
            mode=MODE,
            retry_count=self._retry_count,
            details=judge_reason,
        )

        # Check if retry needed
        if MODE == "active" and self._retry_count < MAX_RETRIES:
            min_score = min(
                s for s in [judge_score, gr_grounding, gr_relevance] if s is not None
            )
            if min_score < RETRY_THRESHOLD:
                self._retry_count += 1
                return Guide(
                    reason=f"Response relevancy too low (judge: {judge_score:.2f}, "
                           f"grounding: {gr_grounding}, relevance: {gr_relevance}). "
                           f"Please verify you used the correct data for: '{self._user_query}'"
                )

        # Reset retry count for next invocation
        self._retry_count = 0
        return Proceed(reason=f"Scores — judge: {judge_score:.2f}, "
                              f"grounding: {gr_grounding}, relevance: {gr_relevance}")

    # ── Internal helpers ──

    def _extract_user_query(self, agent) -> str:
        """Get the latest user message from conversation history."""
        for msg in reversed(agent.messages or []):
            if msg.get("role") == "user":
                for block in msg.get("content", []):
                    if "text" in block:
                        text = block["text"]
                        # Strip conversation history prefix if present
                        if "User's new message:" in text:
                            return text.split("User's new message:")[-1].strip()
                        return text[:500]
        return ""

    def _extract_kb_results(self, tool_calls: list) -> str:
        """Collect results from KB tool calls in the ledger."""
        results = []
        for tc in tool_calls:
            if tc.get("tool_name") in KB_TOOLS and tc.get("status") == "success":
                for r in tc.get("result", []):
                    if "text" in r:
                        results.append(r["text"][:2000])
        return "\n".join(results)[:5000]  # Guardrails limit: 100K source, 5K response

    async def _judge_query_relevancy(self, user_query: str, tool_query: str) -> tuple[float, str]:
        """LLM judge: does the KB query match the user's intent?"""
        try:
            result = self._get_judge()(
                f"User asked: {user_query}\n\nKB query being sent: {tool_query}\n\n"
                "Score how well the KB query matches the user's intent.",
                structured_output_model=JudgeScore,
            )
            return result.structured_output.score, result.structured_output.reason
        except Exception as e:
            logger.error(f"Judge query relevancy failed: {e}")
            return 1.0, f"Judge error: {e}"

    async def _judge_response_relevancy(
        self, user_query: str, kb_results: str, response: str
    ) -> tuple[float, str]:
        """LLM judge: is the response relevant and faithful to KB data?"""
        try:
            result = self._get_judge()(
                f"User asked: {user_query}\n\n"
                f"Data retrieved from KB:\n{kb_results[:3000]}\n\n"
                f"Agent's response:\n{response[:2000]}\n\n"
                "Score relevancy and faithfulness.",
                structured_output_model=JudgeScore,
            )
            return result.structured_output.score, result.structured_output.reason
        except Exception as e:
            logger.error(f"Judge response relevancy failed: {e}")
            return 1.0, f"Judge error: {e}"

    def _guardrail_score(
        self, source: str, query: str, response: str
    ) -> tuple[float | None, float | None]:
        """Call ApplyGuardrail for grounding + relevance scores."""
        if not GUARDRAIL_ID or not source.strip():
            return None, None

        try:
            result = _get_bedrock_rt().apply_guardrail(
                guardrailIdentifier=GUARDRAIL_ID,
                guardrailVersion=GUARDRAIL_VERSION,
                source="OUTPUT",
                content=[
                    {"text": {"text": source[:100000], "qualifiers": ["grounding_source"]}},
                    {"text": {"text": query[:1000], "qualifiers": ["query"]}},
                    {"text": {"text": response[:5000]}},
                ],
            )
            filters = result.get("assessments", [{}])[0].get(
                "contextualGroundingPolicy", {}
            ).get("filters", [])

            grounding = next((f["score"] for f in filters if f["type"] == "GROUNDING"), None)
            relevance = next((f["score"] for f in filters if f["type"] == "RELEVANCE"), None)
            return grounding, relevance
        except Exception as e:
            logger.error(f"Guardrail scoring failed: {e}")
            return None, None
