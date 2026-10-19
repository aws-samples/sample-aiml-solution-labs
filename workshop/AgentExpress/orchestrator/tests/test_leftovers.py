"""leftovers.sh: lists what a destroy left tagged as THIS stack's, and deletes nothing.

Driven with a stand-in `aws` on PATH that records every call, so these check what matters
without an account: that it asks for exactly this stack's tag value (the whole ownership
rule), that it only ever reads, that a failure to look is not reported as 'nothing left',
and that it runs under the bash 3.2 macOS ships (no mapfile, no empty-array expansion).
"""
from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

ORCH = Path(__file__).resolve().parent.parent
SCRIPT = ORCH / "leftovers.sh"
STOCK_BASH = "/bin/bash"          # 3.2 on macOS; whatever the system has elsewhere

STUB = r"""#!/bin/bash
echo "aws $*" >> "$STUB_LOG"
# region is the value after --region
region=""
prev=""
for a in "$@"; do
  [[ "$prev" == "--region" ]] && region="$a"
  prev="$a"
done
if [[ "$1" == "kms" ]]; then
  [[ -n "${STUB_KEY_FAIL:-}" ]] && { echo "AccessDenied: no" >&2; exit 254; }
  printf '%s' "${STUB_KEY_STATE:-Enabled}"
  exit 0
fi
var="STUB_OUT_${region//-/_}"
fail="STUB_FAIL_${region//-/_}"
if [[ -n "${!fail:-}" ]]; then echo "AccessDenied: no" >&2; exit 254; fi
printf '%s' "${!var:-}"
"""


@pytest.fixture
def run(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    aws = bindir / "aws"
    aws.write_text(STUB)
    aws.chmod(aws.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.log"

    def go(*args, **env):
        e = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "STUB_LOG": str(log),
             "AWS_REGION": "", "AWS_DEFAULT_REGION": "", **env}
        # The stock bash runs this repo's own script with the test's arguments.
        r = subprocess.run([STOCK_BASH, str(SCRIPT), *args],  # noqa: S603  # nosec B603
                           capture_output=True, text=True, env=e)
        calls = log.read_text().splitlines() if log.exists() else []
        return r, calls
    return go


def test_nothing_left_is_said_plainly_and_exits_zero(run):
    r, calls = run("ax_demo", "us-east-1")
    assert r.returncode == 0
    assert "Nothing tagged agentexpress:app=ax_demo remains." in r.stdout


def test_it_asks_for_exactly_this_stacks_tag_value_and_nothing_else(run):
    """The ownership rule: another deployment's resources have another value, so are never asked for."""
    r, calls = run("ax_demo", "us-east-1")
    assert calls == ["aws resourcegroupstaggingapi get-resources --region us-east-1 "
                     "--tag-filters Key=agentexpress:app,Values=ax_demo "
                     "--query ResourceTagMappingList[].ResourceARN --output text"]


def test_it_only_reads(run):
    r, calls = run("ax_demo", "eu-west-1", STUB_OUT_eu_west_1="arn:aws:logs:eu-west-1:1:log-group:/x")
    verbs = {c.split()[2] for c in calls}
    assert verbs == {"get-resources"}
    import re
    # Whole command names only (the flag `--output` contains "put"): nothing that changes anything.
    assert not any(re.match(r"(delete|remove|terminate|destroy|update|put|create|tag|untag)-", c.split()[2])
                   for c in calls)


def test_leftovers_are_listed_with_a_count_and_exit_ten(run):
    out = "arn:aws:logs:us-east-1:1:log-group:/aws/lambda/AgentCoreSignup-ax_demo\tarn:aws:cognito-idp:us-east-1:1:userpool/p"
    r, calls = run("ax_demo", "us-east-1", STUB_OUT_us_east_1=out)
    assert r.returncode == 10
    assert "(2)" in r.stdout and "none of them was deleted" in r.stdout
    assert "  arn:aws:logs:us-east-1:1:log-group:/aws/lambda/AgentCoreSignup-ax_demo" in r.stdout
    assert "  arn:aws:cognito-idp:us-east-1:1:userpool/p" in r.stdout


def test_a_global_resource_returned_from_two_regions_is_listed_once(run):
    arn = "arn:aws:iam::1:role/AgentCoreBFF-ax_demo"
    r, calls = run("ax_demo", "eu-west-1", STUB_OUT_eu_west_1=arn, STUB_OUT_us_east_1=arn)
    assert r.returncode == 10 and r.stdout.count(arn) == 1 and "(1)" in r.stdout
    assert len(calls) == 2                               # the region asked for, and us-east-1 for globals


def test_us_east_1_alone_is_asked_once(run):
    r, calls = run("ax_demo", "us-east-1")
    assert len(calls) == 1


def test_a_failure_to_look_is_not_reported_as_nothing_left(run):
    r, calls = run("ax_demo", "us-east-1", STUB_FAIL_us_east_1="1")
    assert r.returncode == 1
    assert "Nothing tagged" not in r.stdout
    assert "could not look in us-east-1" in r.stderr


def test_one_region_failing_still_reports_what_the_other_found(run):
    r, calls = run("ax_demo", "eu-west-1", STUB_FAIL_eu_west_1="1", STUB_OUT_us_east_1="arn:aws:iam::1:role/r")
    assert r.returncode == 10 and "arn:aws:iam::1:role/r" in r.stdout
    assert "could not look in eu-west-1" in r.stderr


def test_no_agent_name_is_a_usage_error_that_asks_nothing(run):
    r, calls = run()
    assert r.returncode == 1 and "usage:" in r.stderr and calls == []


def test_a_none_answer_from_the_cli_is_not_a_leftover(run):
    r, calls = run("ax_demo", "us-east-1", STUB_OUT_us_east_1="None")
    assert r.returncode == 0 and "Nothing tagged" in r.stdout


KEY = "arn:aws:kms:us-east-1:1:key/ef9ffbb4-4768-4418-b67f-df4099f8c48a"


def test_a_kms_key_scheduled_for_deletion_is_not_a_leftover(run):
    """Seen in a live destroy: the data key stays tagged for its 7-day waiting period."""
    r, calls = run("ax_demo", "us-east-1", STUB_OUT_us_east_1=KEY, STUB_KEY_STATE="PendingDeletion")
    assert r.returncode == 0
    assert "Scheduled for deletion by KMS" in r.stdout and KEY in r.stdout
    assert "Nothing tagged agentexpress:app=ax_demo remains." in r.stdout
    assert "Still tagged" not in r.stdout


def test_an_enabled_kms_key_is_still_a_leftover(run):
    r, calls = run("ax_demo", "us-east-1", STUB_OUT_us_east_1=KEY, STUB_KEY_STATE="Enabled")
    assert r.returncode == 10 and "(1)" in r.stdout and "Scheduled" not in r.stdout


def test_a_kms_key_whose_state_cannot_be_read_is_still_a_leftover(run):
    r, calls = run("ax_demo", "us-east-1", STUB_OUT_us_east_1=KEY, STUB_KEY_FAIL="1")
    assert r.returncode == 10 and KEY in r.stdout and "Scheduled" not in r.stdout


def test_a_scheduled_key_does_not_hide_a_real_leftover(run):
    pool = "arn:aws:cognito-idp:us-east-1:1:userpool/p"
    r, calls = run("ax_demo", "us-east-1", STUB_OUT_us_east_1=f"{KEY}\t{pool}", STUB_KEY_STATE="PendingDeletion")
    assert r.returncode == 10 and "(1)" in r.stdout
    assert "Scheduled for deletion by KMS" in r.stdout and pool in r.stdout


def test_the_only_extra_call_is_the_read_only_key_describe(run):
    r, calls = run("ax_demo", "us-east-1", STUB_OUT_us_east_1=KEY, STUB_KEY_STATE="PendingDeletion")
    assert [c.split()[1] for c in calls] == ["resourcegroupstaggingapi", "kms"]
    assert calls[1].split()[2] == "describe-key"


def test_teardown_runs_it_after_the_destroy_and_carries_on_either_way():
    t = (ORCH / "terraform" / "teardown.sh").read_text()
    destroy = t.index("terraform destroy -auto-approve")
    call = t.index('../leftovers.sh "$AGENT_NAME" "$REGION" || true')
    assert destroy < call < t.index("# --- 2. revert Transaction Search")
    assert SCRIPT.stat().st_mode & stat.S_IEXEC
