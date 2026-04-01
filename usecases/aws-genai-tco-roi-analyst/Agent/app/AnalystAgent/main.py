"""
AgentCore entrypoint for the AWS TCO & BVA Analyst agent.
Uses AgentCore Memory (LessonsLearned) for cross-session knowledge retention.
"""

import os
import sys
import time
import json
import logging

from strands import Agent
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from bedrock_agentcore.memory.integrations.strands.config import (
    AgentCoreMemoryConfig,
    RetrievalConfig,
)
from bedrock_agentcore.memory.integrations.strands.session_manager import (
    AgentCoreMemorySessionManager,
)
from model.load import load_model, MODEL_ID
from mcp_client.client import get_streamable_http_mcp_client

from search_pricing_info import call_pricing_search_agent
from calculator_bva import bva_calculator, bva_what_if_analysis
from calculator_agentcore import use_agentcore_calculator, agentcore_what_if_analysis
from calculator_bedrock import use_bedrock_calculator, bedrock_what_if_analysis
from calculator_capacity_planning import capacity_planning_calculator
from search_bedrock_quota import call_bedrock_quota_agent
from system_prompt import TCO_ANALYST_PROMPT
from strands.models import CacheConfig
from strands import tool

os.environ["BYPASS_TOOL_CONSENT"] = "true"

app = BedrockAgentCoreApp()
log = app.logger

# MCP client for AWS Knowledge
mcp_client = get_streamable_http_mcp_client()

# AgentCore Memory configuration
MEMORY_ID = os.environ.get("MEMORY_ID", "")
REGION = os.environ.get("AWS_REGION", "us-east-1")

# Lazy-init boto3 client for direct memory operations
_agentcore_client = None


def _get_agentcore_client():
    global _agentcore_client
    if _agentcore_client is None:
        import boto3
        _agentcore_client = boto3.client("bedrock-agentcore", region_name=REGION)
    return _agentcore_client


@tool
def store_correction(mistake: str, correction: str, topic: str = "") -> str:
    """Store a lesson learned when you discover you made a mistake.

    Call this tool IMMEDIATELY when:
    - The user corrects you about pricing, billing units, or cost comparisons
    - You realize you described a pricing model incorrectly (e.g., said "per audio minute"
      when it's actually token-based)
    - You made an incomplete cost comparison (e.g., compared only storage cost while
      ignoring LLM invocation costs)
    - Any factual error about AWS services is identified

    The correction is stored as a REFLECTION in long-term memory so you avoid
    repeating the same mistake in future conversations.

    Args:
        mistake: What you said that was wrong (be specific).
        correction: What the correct information is (be specific).
        topic: Optional topic tag for easier retrieval (e.g., "Nova Sonic pricing",
               "AgentCore memory cost comparison").

    Returns:
        Confirmation that the correction was stored.
    """
    if not MEMORY_ID:
        return "Memory not available — MEMORY_ID not set."

    import uuid
    from datetime import datetime, timezone

    content = json.dumps({
        "title": f"Correction: {topic}" if topic else "Correction",
        "type": "CORRECTION",
        "mistake": mistake,
        "correction": correction,
        "topic": topic,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    })

    client = _get_agentcore_client()
    try:
        resp = client.batch_create_memory_records(
            memoryId=MEMORY_ID,
            records=[{
                "requestIdentifier": str(uuid.uuid4()),
                "namespaces": ["/episodes"],
                "content": {"text": content},
                "timestamp": datetime.now(timezone.utc),
            }],
            clientToken=str(uuid.uuid4()),
        )
        successful = resp.get("successfulRecords", [])
        failed = resp.get("failedRecords", [])
        if failed:
            return f"Failed to store correction: {failed}"
        rid = successful[0].get("memoryRecordId", "?") if successful else "?"
        return f"Correction stored (record: {rid}). Will avoid this mistake in future."
    except Exception as e:
        return f"Error storing correction: {e}"


@tool
def recall_corrections(query: str, top_k: int = 5) -> str:
    """Search long-term memory for past corrections and lessons learned.

    Call this tool BEFORE making claims about pricing models, billing units,
    or cost comparisons to check if you've been corrected on this topic before.

    Args:
        query: What to search for (e.g., "Nova Sonic pricing", "memory cost comparison").
               Keep it short and specific — max 200 characters recommended.
        top_k: Number of results to return (default: 5).

    Returns:
        Matching corrections from memory, or a message if none found.
    """
    if not MEMORY_ID:
        return "Memory not available — MEMORY_ID not set."

    # Truncate query to stay within API limit (10,000 chars max)
    query = query[:10000]

    client = _get_agentcore_client()
    try:
        resp = client.retrieve_memory_records(
            memoryId=MEMORY_ID,
            namespace="/episodes",
            searchCriteria={
                "searchQuery": query,
                "topK": top_k,
                "metadataFilters": [{
                    "left": {"metadataKey": "x-amz-agentcore-memory-recordType"},
                    "operator": "EQUALS_TO",
                    "right": {"metadataValue": {"stringValue": "REFLECTION"}},
                }],
            },
            maxResults=top_k,
        )
        records = resp.get("memoryRecordSummaries", [])
        if not records:
            return "No past corrections found for this topic."

        lines = []
        for i, rec in enumerate(records, 1):
            content = rec.get("content", {}).get("text", "")
            score = rec.get("relevanceScore", "N/A")
            lines.append(f"--- Lesson {i} (score: {score}) ---")
            try:
                parsed = json.loads(content)
                if parsed.get("type") == "CORRECTION":
                    lines.append(f"Topic: {parsed.get('topic', 'N/A')}")
                    lines.append(f"Mistake: {parsed.get('mistake', 'N/A')}")
                    lines.append(f"Correction: {parsed.get('correction', 'N/A')}")
                else:
                    lines.append(f"Title: {parsed.get('title', 'N/A')}")
                    lines.append(content[:500])
            except json.JSONDecodeError:
                lines.append(content[:500])
            lines.append("")
        return "\n".join(lines)
    except Exception as e:
        return f"Error searching memory: {e}"

# Agent tools
tools = [
    call_pricing_search_agent,
    bva_calculator,
    bva_what_if_analysis,
    use_agentcore_calculator,
    agentcore_what_if_analysis,
    use_bedrock_calculator,
    bedrock_what_if_analysis,
    capacity_planning_calculator,
    call_bedrock_quota_agent,
    store_correction,
    recall_corrections,
]

_agent = None
_agent_model_id = None
_agent_session_id = None
_mcp_entered = False
_session_manager = None


def _create_session_manager(session_id: str, actor_id: str = "user"):
    """Create an AgentCore Memory session manager for the given session.

    Configures retrieval from the episodic memory namespace:
    - /episodes/{actorId}/{sessionId} — episodic memories from conversations
    - /episodes/reflection — cross-session reflections and lessons learned
    """
    memory_id = MEMORY_ID
    if not memory_id:
        return None

    config = AgentCoreMemoryConfig(
        memory_id=memory_id,
        session_id=session_id,
        actor_id=actor_id,
        retrieval_config={
            "/episodes/{actorId}/{sessionId}": RetrievalConfig(
                top_k=5,
                relevance_score=0.3,
            ),
            "/episodes/reflection": RetrievalConfig(
                top_k=5,
                relevance_score=0.3,
            ),
        },
    )
    return AgentCoreMemorySessionManager(
        agentcore_memory_config=config,
        region_name=REGION,
    )


def get_or_create_agent(model_id=None, session_id="default"):
    """Get or create the TCO analyst agent with MCP tools and memory.

    Recreates the agent if model_id or session_id changes.
    """
    global _agent, _agent_model_id, _agent_session_id, _mcp_entered, _session_manager

    # Enter MCP client context once
    if not _mcp_entered:
        mcp_client.__enter__()
        _mcp_entered = True

    requested_model = model_id or MODEL_ID
    needs_rebuild = (
        _agent is None
        or requested_model != _agent_model_id
        or session_id != _agent_session_id
    )

    if needs_rebuild:
        # Close previous session manager if it exists
        if _session_manager is not None:
            try:
                _session_manager.close()
            except Exception as e:
                log.warning(f"Error closing previous session manager: {e}")

        from strands.models import BedrockModel
        model = BedrockModel(
            model_id=requested_model,
            temperature=0.1,
            cache_config=CacheConfig(strategy="auto"),
            cache_tools="default"
            )

        mcp_tools = mcp_client.list_tools_sync()
        all_tools = tools + list(mcp_tools)

        # Create session manager for memory
        _session_manager = _create_session_manager(session_id)
        if _session_manager:
            log.info(f"Memory enabled: {MEMORY_ID} (session: {session_id})")

        _agent = Agent(
            model=model,
            system_prompt=TCO_ANALYST_PROMPT,
            tools=all_tools,
            session_manager=_session_manager,
        )
        _agent_model_id = requested_model
        _agent_session_id = session_id
        log.info(f"Agent created with model: {requested_model}")

    return _agent

def invoke_with_retry(agent, query: str, max_retries: int = 3, base_delay: int = 1):
    """Invoke agent with exponential backoff retry for throttling errors."""
    for attempt in range(max_retries + 1):
        try:
            return agent(query)
        except Exception as e:
            error_str = str(e).lower()
            is_retryable = any(err in error_str for err in [
                "serviceunavailableexception",
                "modelthrottledexception",
                "throttling",
            ])
            if is_retryable and attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                log.warning(f"Retry {attempt + 1}/{max_retries} after {delay}s: {type(e).__name__}")
                time.sleep(delay)
                continue
            raise


@app.entrypoint
async def invoke(payload, context):
    """Handle incoming requests using the TCO analyst agent."""
    log.info("Invoking TCO Analyst Agent...")

    prompt = payload.get("prompt", "")
    model_id = payload.get("model_id", None)
    session_id = getattr(context, "session_id", "default") if context else "default"

    if not prompt:
        yield "Error: Missing 'prompt' in payload"
        return

    try:
        agent = get_or_create_agent(model_id=model_id, session_id=session_id)
        stream = agent.stream_async(prompt)

        async for event in stream:
            if "data" in event and isinstance(event["data"], str):
                yield event["data"]

    except Exception as e:
        import traceback
        yield f"Error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"


if __name__ == "__main__":
    app.run()
