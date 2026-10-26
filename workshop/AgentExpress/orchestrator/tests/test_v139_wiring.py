"""v1.3.9's tracks, wired together. Each track tests its own feature; this holds the joins:
  * the replay brief's pipeline, written with the final keys (parallel Document generators,
    a check gated with named options and a branch on `decision`, a deliver step after it,
    build storage and `${storage}` locations, run files), is valid to the validator, and is
    what scripts/assistant_regression.py passes;
  * a file the Document generator saves with a run is what a deliver step after it picks
    up (runs/<session>/outputs/, made by the generating agent) and copies into `${storage}`;
  * the runtime resolves `${storage}` the same way in the deliver step as the build-storage
    helper does (STORAGE_BUCKET);
  * Terraform resolves `${storage}` in the Document generator's settings and grants and in
    the deliver grant, and refuses a deliver target naming it when the build creates no
    storage (tests/fixtures/v139_wiring.tftest.hcl). cdk/test/v139-wiring.test.ts is the
    same for CDK.


"""
from __future__ import annotations

import copy
import json
import re
import shutil
import subprocess
import sys

import pytest
import validate_build
from conftest import ORCH_ROOT
from test_docgen_tool import dg  # noqa: F401 - the Document generator's handler, as a fixture

from app.common import deliver, storage

sys.path.insert(0, str(ORCH_ROOT / "scripts"))
import assistant_regression as regression

SID = "run-0123456789abcdef"
BUCKET = "multiagent-orchestrator-storage-123456789012"


def _docgen(fmt: str, what: str) -> dict:
    return {"type": "lambda", "description": f"Writes the {what}.", "source": "docgen",
            "call": "generate_document", "arg": "data",
            "toolSchema": [{"name": "generate_document", "description": "Write a document from data.",
                            "properties": {"format": {"type": "string", "required": True, "description": "pdf, pptx or docx."},
                                           "data": {"type": "object", "required": True, "description": "The content."},
                                           "runId": {"type": "string", "required": False, "description": "Set by the workflow."},
                                           "agent": {"type": "string", "required": False, "description": "Set by the workflow."}}}],
            "formats": [fmt], "templateS3Uri": "${storage}/templates/", "outputPrefix": "${storage}/outputs/"}


def _agent(name: str, tool: str | None = None) -> dict:
    return {"name": name, "runtime": "main", "maxTokens": 2000, **({"tool": tool} if tool else {})}


#: The replay brief, as the Assistant is taught to write it (bff/designer.py GUIDE 3a-3e).
REPLAY = {
    "orchestrator": {"files": {}, "storage": {"create": True}},
    "tools": {"pdfReport": _docgen("pdf", "PDF report"), "pptxDeck": _docgen("pptx", "PPTX deck"),
              "docxSummary": _docgen("docx", "DOCX summary")},
    "agents": {
        "parser": _agent("CSV Parser"),
        "pdf_writer": _agent("PDF Writer", "pdfReport"),
        "deck_writer": _agent("Deck Writer", "pptxDeck"),
        "summary_writer": _agent("Summary Writer", "docxSummary"),
        "checker": _agent("Consistency Checker"),
        "send": {"name": "Deliver", "runtime": "task", "task": {"kind": "deliver", "targets": [
            {"type": "s3", "uri": "${storage}/delivered/",
             "producedBy": ["pdf_writer", "deck_writer", "summary_writer"]}]}},
    },
    "steps": [
        {"agent": "parser"},
        {"parallel": ["pdf_writer", "deck_writer", "summary_writer"]},
        {"agent": "checker",
         "hitl": {"mode": "threshold", "when": [{"field": "passed", "equals": False}], "options": ["Override", "Abort"]},
         "branch": {"when": [{"field": "decision", "equals": "Abort", "goto": "END"}], "default": "send"}},
        {"agent": "send"},
    ],
    "ui": {"title": "Document pipeline", "heading": "Document pipeline"},
}


def _errors(wf: dict) -> list[str]:
    return [f"{i['path']}: {i['message']}" for i in validate_build.validate(wf) if i["severity"] == "error"]


def test_the_replay_pipeline_with_the_final_keys_is_valid_and_passes_the_regression_check():
    assert _errors(REPLAY) == []
    doc = {"turns": [{"role": "user", "text": regression.REPLAY_BRIEF},
                     {"role": "assistant", "text": "Built it.", "changes": [{"summary": "first draft"}]}]}
    result, evidence = regression.check_replay({"workflow": REPLAY}, doc)
    assert result == regression.PASS, evidence
    assert regression.check_storage({"workflow": REPLAY}, doc)[0] == regression.PASS
    assert regression.check_files({"workflow": REPLAY}, doc)[0] == regression.PASS


def test_the_regression_check_fails_what_the_validator_or_the_brief_would_not_take():
    doc = {"turns": [{"role": "assistant", "text": "Built it.", "changes": [{"summary": "first draft"}]}]}
    late = copy.deepcopy(REPLAY)
    late["steps"] = [late["steps"][0], late["steps"][1], {"agent": "send"}, late["steps"][2]]
    assert "does not come after the decision gate" in regression.check_replay({"workflow": late}, doc)[1]
    no_files = copy.deepcopy(REPLAY)
    del no_files["orchestrator"]["files"]
    assert _errors(no_files)
    assert "orchestrator.files is not set" in regression.check_replay({"workflow": no_files}, doc)[1]
    no_storage = copy.deepcopy(REPLAY)
    del no_storage["orchestrator"]["storage"]
    assert any("creates no storage" in e for e in _errors(no_storage))
    assert "orchestrator.storage.create is not true" in regression.check_replay({"workflow": no_storage}, doc)[1]


def test_the_regression_check_takes_a_template_upload_but_fails_a_delivery_bucket_question():
    template = {"path": "tools.pptxDeck.templateS3Uri",
                "question": "Upload deck.pptx to the build bucket's templates/ folder (my default)."}
    bucket = {"path": "agents.send.task.targets[0].uri",
              "question": "Deliver to the build's own bucket (default), or to a bucket of yours?"}

    def doc(*needs):
        return {"turns": [{"role": "assistant", "text": "Built it.",
                           "changes": [{"summary": "first draft", "needsInput": list(needs)}]}]}
    assert regression.check_replay({"workflow": REPLAY}, doc(template))[0] == regression.PASS
    assert regression.check_storage({"workflow": REPLAY}, doc(template))[0] == regression.PASS
    result, evidence = regression.check_replay({"workflow": REPLAY}, doc(template, bucket))
    assert result == regression.FAIL
    assert "Deliver to the build's own bucket" in evidence and "deck.pptx" not in evidence


def test_the_regression_check_fails_a_gate_that_ends_a_passing_run_or_delivers_on_abort():
    doc = {"turns": [{"role": "assistant", "text": "Built it.", "changes": [{"summary": "first draft"}]}]}
    inverted = copy.deepcopy(REPLAY)
    inverted["steps"][2]["branch"] = {"when": [{"field": "decision", "equals": "Override", "goto": "send"}],
                                      "default": "END"}
    assert _errors(inverted) == []
    result, evidence = regression.check_replay({"workflow": inverted}, doc)
    assert result == regression.FAIL
    assert "with no choice made the decision gate's branch goes to END, not to the deliver step" in evidence
    assert "Abort goes to END" not in evidence
    no_abort = copy.deepcopy(REPLAY)
    no_abort["steps"][2]["branch"] = {"when": [{"field": "decision", "equals": "Override", "goto": "send"}]}
    result, evidence = regression.check_replay({"workflow": no_abort}, doc)
    assert result == regression.FAIL
    assert "Abort goes to the next stage, not END" in evidence


def test_a_document_generator_needs_the_run_arguments_declared_and_no_harness():
    undeclared = copy.deepcopy(REPLAY)
    for t in undeclared["tools"].values():
        del t["toolSchema"][0]["properties"]["agent"]
    assert any(e.startswith("tools.pdfReport.toolSchema[0].properties: declare agent (optional strings)")
               for e in _errors(undeclared))
    harness = copy.deepcopy(REPLAY)
    harness["agents"]["pdf_writer"] = {"name": "PDF Writer", "runtime": "harness", "tool": "pdfReport",
                                       "instructions": "Write the PDF report from the parsed CSV."}
    assert any(e.startswith('agents.pdf_writer.tool: "pdfReport" is the Document generator') for e in _errors(harness))


class RunFiles:
    """One in-memory S3 both sides use: the Document generator's puts, the deliver step's
    list, head and copy."""

    def __init__(self):
        self.objects: dict[tuple[str, str], dict] = {}
        self.copies: list[dict] = []

    def put_object(self, **kw):
        self.objects[(kw["Bucket"], kw["Key"])] = {"body": kw["Body"], "type": kw.get("ContentType"),
                                                   "meta": kw.get("Metadata") or {}}

    def generate_presigned_url(self, op, Params, ExpiresIn):
        return f"https://{Params['Bucket']}.s3.amazonaws.com/{Params['Key']}"

    def list_objects_v2(self, **kw):
        return {"Contents": [{"Key": k, "Size": len(o["body"])} for (b, k), o in sorted(self.objects.items())
                             if b == kw["Bucket"] and k.startswith(kw["Prefix"])], "IsTruncated": False}

    def head_object(self, **kw):
        o = self.objects[(kw["Bucket"], kw["Key"])]
        return {"ContentType": o["type"], "Metadata": o["meta"], "ContentLength": len(o["body"])}

    def copy_object(self, **kw):
        self.copies.append(kw)
        src = self.objects[(kw["CopySource"]["Bucket"], kw["CopySource"]["Key"])]
        self.objects[(kw["Bucket"], kw["Key"])] = dict(src)


def test_a_file_the_document_generator_saves_is_what_a_deliver_step_sends(dg, monkeypatch):  # noqa: F811 - dg is the fixture
    s3 = RunFiles()
    dg._s3 = s3
    monkeypatch.setattr(deliver, "_client", lambda service, region="": s3)
    monkeypatch.setenv("STORAGE_BUCKET", BUCKET)
    made = dg.generate_document({"format": "pdf", "data": {"title": "Quarterly report"}, "fileName": "report",
                                 "runId": SID, "agent": "pdf_writer"})
    assert made["runFile"] == f"runs/{SID}/outputs/report.pdf"
    files = deliver.run_files(SID)
    assert [(f["name"], f["agent"], f["contentType"]) for f in files] == [("report.pdf", "pdf_writer", "application/pdf")]
    target = REPLAY["agents"]["send"]["task"]["targets"][0]
    picked = deliver.select(files, target)
    assert [f["name"] for f in picked] == ["report.pdf"]
    assert deliver.select(files, {**target, "producedBy": ["deck_writer"]}) == []
    sent = deliver.send_s3(SID, target, picked, {})
    assert sent["destination"] == f"s3://{BUCKET}/delivered/{SID}/"
    assert (BUCKET, f"delivered/{SID}/report.pdf") in s3.objects


def test_the_deliver_step_resolves_the_token_as_the_storage_helper_does(monkeypatch):
    monkeypatch.setenv("STORAGE_BUCKET", BUCKET)
    uri = "${storage}/delivered/"
    assert storage.resolve(uri) == f"s3://{BUCKET}/delivered/"
    assert deliver.resolve_uri(uri) == (BUCKET, "delivered/")
    assert deliver.STORAGE_TOKEN == storage.TOKEN


# --- Terraform -----------------------------------------------------------------------------


TF = ORCH_ROOT / "terraform"
TERRAFORM = shutil.which("terraform")
needs_tf = pytest.mark.skipif(not TERRAFORM or not (TF / ".terraform").exists(),
                              reason="terraform, initialised, is needed for the offline plan")


def _tf_copy(tmp_path, wf: dict, test: str):
    """A copy of the tree whose app/workflow.json is `wf` and whose terraform/tests holds
    only `test` (as tests/test_deliver_tf.py does)."""
    root = tmp_path / "orchestrator"
    root.mkdir()
    for entry in ORCH_ROOT.iterdir():
        if entry.name not in ("app", "terraform"):
            (root / entry.name).symlink_to(entry)
    shutil.copytree(ORCH_ROOT / "app", root / "app", ignore=shutil.ignore_patterns("__pycache__"))
    (root / "app" / "workflow.json").write_text(json.dumps(wf, indent=2))
    tf = root / "terraform"
    tf.mkdir()
    for entry in TF.iterdir():
        if entry.name not in (".terraform.lock.hcl", "tests"):
            (tf / entry.name).symlink_to(entry)
    shutil.copy(TF / ".terraform.lock.hcl", tf / ".terraform.lock.hcl")
    (tf / "tests").mkdir()
    (tf / "tests" / "wiring.tftest.hcl").write_text(test)
    return subprocess.run(  # noqa: S603 - fixed argv, no shell  # nosec B603  # nosemgrep
        [TERRAFORM, "test", "-no-color", "-filter=tests/wiring.tftest.hcl"], cwd=tf, capture_output=True,
        text=True, timeout=600, check=False)


def _shipped_with_v139(storage_on: bool) -> dict:
    wf = json.loads((ORCH_ROOT / "app" / "workflow.json").read_text())
    wf.setdefault("orchestrator", {})["files"] = {}
    if storage_on:
        wf["orchestrator"]["storage"] = {"create": True}
    wf["tools"]["reportDoc"] = _docgen("pdf", "report")
    wf["agents"]["send"] = copy.deepcopy(REPLAY["agents"]["send"])
    wf["agents"]["send"]["task"]["targets"][0].pop("producedBy")
    wf["steps"].append({"agent": "send"})
    return wf


@needs_tf
def test_terraform_resolves_the_token_for_docgen_and_deliver(tmp_path):
    r = _tf_copy(tmp_path, _shipped_with_v139(True), (ORCH_ROOT / "tests" / "fixtures" / "v139_wiring.tftest.hcl").read_text())
    assert r.returncode == 0, r.stdout[-6000:] + r.stderr[-2000:]
    assert re.search(r"Success! 1 passed, 0 failed", r.stdout), r.stdout[-2000:]


@needs_tf
def test_terraform_refuses_a_deliver_target_in_storage_the_build_does_not_create(tmp_path):
    wf = _shipped_with_v139(False)
    del wf["tools"]["reportDoc"]  # only the deliver target names the token
    fixture = (ORCH_ROOT / "tests" / "fixtures" / "v139_wiring.tftest.hcl").read_text()
    test = fixture.split('run "', 1)[0] + (
        'run "no_storage" {\n  command = plan\n  plan_options {\n    target = [terraform_data.storage_token]\n  }\n'
        '  expect_failures = [terraform_data.storage_token]\n}\n')
    r = _tf_copy(tmp_path, wf, test)
    assert r.returncode == 0, r.stdout[-6000:] + r.stderr[-2000:]
    assert re.search(r"Success! 1 passed, 0 failed", r.stdout), r.stdout[-2000:]
