"""Tracks everything the demo creates, so teardown can be complete + idempotent.

state.json shape:
{
  "resources": [
     {"kind": "cdk-stack",        "id": "IrisDemoBaseline",     "phase": "baseline"},
     {"kind": "dynamodb-table",   "id": "iris-demo-customers-baseline",  "phase": "baseline"},
     {"kind": "iam-role",         "id": "iris-demo-exec-role",  "phase": "baseline"},
     {"kind": "agentcore-runtime","id": "<runtimeId>", "arn": "...", "phase": "baseline"}
  ]
}
"""
import json
import os
import threading

_HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(_HERE, "..", "state.json")
_lock = threading.Lock()


def _read():
    if not os.path.exists(STATE_PATH):
        return {"resources": []}
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {"resources": []}


def _write(data):
    with open(STATE_PATH, "w") as f:
        json.dump(data, f, indent=2)


def all_resources():
    return _read().get("resources", [])


def add_resource(kind, rid, phase="baseline", **extra):
    with _lock:
        data = _read()
        # de-dupe on (kind, id)
        data["resources"] = [r for r in data["resources"] if not (r.get("kind") == kind and r.get("id") == rid)]
        rec = {"kind": kind, "id": rid, "phase": phase}
        rec.update(extra)
        data["resources"].append(rec)
        _write(data)
    return rec


def remove_resource(kind, rid):
    with _lock:
        data = _read()
        data["resources"] = [r for r in data["resources"] if not (r.get("kind") == kind and r.get("id") == rid)]
        _write(data)


def clear():
    with _lock:
        _write({"resources": []})
