"""A type="kb" tool's `dataSource`: documents from a web crawl or a Confluence, SharePoint or
Salesforce site, instead of files in S3. For validate_build.py.
Bedrock checks a connector only when the data source is created, halfway through a deploy,
and AWS supports these connectors only on an OpenSearch Serverless vector store. So the
settings are checked first, from one home (vocabulary.json kbDataSources: what each
connector requires, and the shapes Bedrock's API model gives them).
Mirrors web/src/builder/kbDataSource.ts (same words: tests/fixtures/validation_cases.json),
cdk/lib/kb-data-source.ts validateKbConnector and the precondition in terraform/kb.tf.
https://docs.aws.amazon.com/bedrock/latest/userguide/data-source-connectors.html
"""
from __future__ import annotations

import re

#: The one store AWS supports these connectors on.
CONNECTOR_STORE = "opensearchServerless"
_FILTER_KEYS = ("objectType", "include", "exclude")


def _whole(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and float(v).is_integer()


def _patterns(items, pattern: str, p: str, where: dict, add) -> None:
    for i, item in enumerate(items):
        if isinstance(item, str) and pattern and not re.search(pattern, item):
            add("error", where, f"{p}[{i}]", f'"{item}" does not match {pattern}')


def _filters(value, spec: dict, p: str, where: dict, add) -> None:
    if not isinstance(value, list):
        return                      # the type check has said so
    if not spec["min"] <= len(value) <= spec["max"]:
        add("error", where, p, f"must list {spec['min']} to {spec['max']} filters")
    for i, f in enumerate(value):
        fp = f"{p}[{i}]"
        if not isinstance(f, dict):
            add("error", where, fp, 'must be {"objectType": "...", "include": [...], "exclude": [...]}')
            continue
        for k in f:
            if k not in _FILTER_KEYS:
                add("error", where, f"{fp}.{k}", f"unknown key; allowed: {', '.join(_FILTER_KEYS)}")
        ot = f.get("objectType")
        if not isinstance(ot, str) or not 1 <= len(ot) <= 50:
            add("error", where, f"{fp}.objectType", "is required: a content type such as Page, 1 to 50 characters")
        for k in ("include", "exclude"):
            if k in f and (not isinstance(f[k], list) or not 1 <= len(f[k]) <= 25
                           or not all(isinstance(x, str) and x for x in f[k])):
                add("error", where, f"{fp}.{k}", "must list 1 to 25 patterns")


def check(tool: dict, path: str, where: dict, add, vocab_extra, vocab) -> None:
    ds = tool.get("dataSource")
    if not isinstance(ds, dict):
        return
    p = f"{path}.dataSource"
    kinds = vocab("kbDataSources")
    kind = ds.get("type")
    if kind not in kinds:
        add("error", where, f"{p}.type", f"must be one of: {', '.join(kinds)}")
        return
    required = (vocab_extra("kbDataSources", "required") or {}).get(kind) or []
    takes = required + ((vocab_extra("kbDataSources", "optional") or {}).get(kind) or [])
    specs = vocab_extra("kbDataSources", "settings") or {}
    vs = tool.get("vectorStore")
    if not isinstance(vs, dict) or vs.get("type") != CONNECTOR_STORE:
        add("error", where, f"{p}.type",
            f'needs vectorStore.type "{CONNECTOR_STORE}": '
            f'AWS supports the {kind} connector only on OpenSearch Serverless')
    if tool.get("s3Uri") is not None:
        add("error", where, f"{p}", "takes the place of s3Uri: the documents come from one or the other")
    for name in required:
        if ds.get(name) is None or ds.get(name) == "" or ds.get(name) == []:
            add("error", where, f"{p}.{name}", f"is required for the {kind} connector")
    for name, value in ds.items():
        if name == "type":
            continue
        if name not in takes:
            add("error", where, f"{p}.{name}", f"does not apply to the {kind} connector")
            continue
        spec = specs.get(name) or {}
        sp = f"{p}.{name}"
        t = spec.get("type")
        if t == "string" and isinstance(value, str) and value and not re.search(spec["pattern"], value):
            add("error", where, sp, f'"{value}" does not match {spec["pattern"]}')
        elif t == "enum" and isinstance(value, str) and value not in spec["values"]:
            add("error", where, sp, f"must be one of: {', '.join(spec['values'])}")
        elif t == "int" and _whole(value) and not spec["min"] <= value <= spec["max"]:
            add("error", where, sp, f"must be a whole number from {spec['min']} to {spec['max']}")
        elif t == "list" and isinstance(value, list):
            if not spec["min"] <= len(value) <= spec["max"]:
                add("error", where, sp, f"must list {spec['min']} to {spec['max']}")
            _patterns(value, spec.get("pattern", ""), sp, where, add)
        elif t == "filters":
            _filters(value, spec, sp, where, add)
