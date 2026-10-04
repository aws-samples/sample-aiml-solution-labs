"""In-process session snapshot store for the local dev server.

Authoritative state lives in the LangGraph checkpointer; in the cloud, live
progress lives in DynamoDB. This is the local-only equivalent the dev server
reads. Node ids come from the workflow config, so it adapts automatically when
agents are added.
"""

from typing import Any

from app.common import clock
from app.common.config import NODE_IDS


def _new_snapshot(session_id: str, topic: str) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "topic": topic,
        "overall": "running",
        "nodes": {n: {"status": "pending", "pct": 0, "output": ""} for n in NODE_IDS},
        "history": {},
        "hitl": None,
        "result": None,
        "logs": [],
        "created": clock.now_str(),
    }


class SessionStore:
    def __init__(self) -> None:
        self._snap: dict[str, dict[str, Any]] = {}

    def create(self, session_id: str, topic: str) -> None:
        self._snap[session_id] = _new_snapshot(session_id, topic)

    def exists(self, session_id: str) -> bool:
        return session_id in self._snap

    def delete(self, session_id: str) -> bool:
        return self._snap.pop(session_id, None) is not None

    def snapshot(self, session_id: str) -> dict[str, Any]:
        return self._snap.get(session_id, {})

    def list_sessions(self) -> list[dict[str, Any]]:
        out = [{
            "session_id": s["session_id"], "topic": s["topic"],
            "overall": s["overall"], "hitl": s["hitl"], "created": s["created"],
        } for s in self._snap.values()]
        return sorted(out, key=lambda x: x["created"], reverse=True)

    async def publish(self, session_id: str, event: dict[str, Any]) -> None:
        snap = self._snap.get(session_id)
        if snap is not None:
            _apply(snap, event)


def _apply(snap: dict[str, Any], event: dict[str, Any]) -> None:
    etype = event.get("type")
    node = event.get("node")
    nodes = snap["nodes"]
    if etype == "node_status" and node in nodes:
        nodes[node]["status"] = event["status"]
        if "output" in event:
            nodes[node]["output"] = event["output"]
        if "history" in event:
            snap.setdefault("history", {})[node] = event["history"]
    elif etype == "heartbeat" and node in nodes:
        nodes[node]["pct"] = event.get("pct", 0)
    elif etype == "hitl_request":
        snap["overall"] = "waiting_human"
        snap["hitl"] = {"node": node, "question": event.get("question", "")}
        if node in nodes:
            nodes[node]["status"] = "waiting_human"
    elif etype == "hitl_resolved":
        snap["hitl"] = None
        if snap["overall"] == "waiting_human":
            snap["overall"] = "running"
    elif etype == "session_status":
        snap["overall"] = event["status"]
        if "result" in event:
            snap["result"] = event["result"]
    if event.get("log"):
        snap["logs"].append({"ts": clock.now_str(), "node": node, "msg": event["log"]})


bus = SessionStore()
