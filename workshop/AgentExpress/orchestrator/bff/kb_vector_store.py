"""A type="kb" tool's `vectorStore`: a vector store the customer already has, which the
Knowledge Base the framework creates writes to and searches, instead of the S3 Vectors
store it would otherwise create. For validate_build.py.
Bedrock checks the storage configuration only when the Knowledge Base is created, halfway
through a deploy, and the ARNs here go into the Knowledge Base role's grants. So each
store's settings are checked first, from one home (vocabulary.json kbVectorStores: what
each store requires, and the shapes Bedrock's API model gives them).
Mirrors web/src/builder/kbVectorStore.ts (same words: tests/fixtures/validation_cases.json),
cdk/lib/kb-vector-store.ts validateKbVectorStore and the precondition in terraform/kb.tf.
https://docs.aws.amazon.com/bedrock/latest/userguide/knowledge-base-setup.html
"""
from __future__ import annotations
import re
#: Aurora names columns, not document fields: Bedrock's ColumnName shape.
_AURORA_FIELDS = ("vectorField", "textField", "metadataField")


def settings(vocab_extra, store: str) -> list[str]:
    """Every setting a store takes, required first, in the digest's order."""
    return list((vocab_extra("kbVectorStores", "required") or {}).get(store) or []) + list(
        (vocab_extra("kbVectorStores", "optional") or {}).get(store) or [])


def check(tool: dict, path: str, where: dict, add, vocab_extra, vocab) -> None:
    vs = tool.get("vectorStore")
    if not isinstance(vs, dict):
        return
    p = f"{path}.vectorStore"
    stores = vocab("kbVectorStores")
    store = vs.get("type")
    if store not in stores:
        add("error", where, f"{p}.type", f"must be one of: {', '.join(stores)}")
        return
    required = (vocab_extra("kbVectorStores", "required") or {}).get(store) or []
    takes = settings(vocab_extra, store)
    patterns = vocab_extra("kbVectorStores", "patterns") or {}
    column = vocab_extra("kbVectorStores", "columnPattern") or ""
    for name in required:
        if vs.get(name) is None or vs.get(name) == "":
            add("error", where, f"{p}.{name}", f"is required for the {store} store")
    for name, value in vs.items():
        if name == "type":
            continue
        if name not in takes:
            add("error", where, f"{p}.{name}", f"does not apply to the {store} store")
            continue
        if not isinstance(value, str) or value == "":
            continue
        pattern = column if store == "aurora" and name in _AURORA_FIELDS else patterns.get(name, "")
        if pattern and not re.search(pattern, value):
            add("error", where, f"{p}.{name}", f'"{value}" does not match {pattern}')
