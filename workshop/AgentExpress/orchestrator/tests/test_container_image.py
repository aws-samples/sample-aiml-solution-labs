"""The container image must not inherit the builder's umask.

WHY THIS TEST EXISTS
--------------------
`COPY` runs as root whatever `USER` says, and it PRESERVES the build context's directory
modes — which come from the umask of whoever ran the build. On a machine with
`umask 077` the app/ tree arrives 0700 root-owned, the unprivileged runtime user cannot
traverse `app/orchestrator/`, and the container dies at start with:

    ModuleNotFoundError: No module named 'app.orchestrator.runtime'

That message names a FILE, so it sends you hunting through `.dockerignore` for something
that was present the whole time. Meanwhile the image built, pushed and deployed without a
single warning, and `cdk deploy` reported success — the run only failed when a user
pressed Start run. Observed exactly that way against a live deployment.

A full build-and-inspect would be the honest test, but it needs a container engine and
two minutes, and this suite is meant for a pre-commit hook. So this asserts the three
properties of the Dockerfile that together make the modes independent of the builder,
each of which was individually absent when the bug shipped.
"""

import re
from pathlib import Path

import pytest

DOCKERFILE = Path(__file__).resolve().parent.parent / "Dockerfile"


@pytest.fixture(scope="module")
def lines() -> list[str]:
    return [ln.strip() for ln in DOCKERFILE.read_text().splitlines()
            if ln.strip() and not ln.strip().startswith("#")]


def _index_of(lines: list[str], pattern: str) -> int:
    for i, ln in enumerate(lines):
        if re.search(pattern, ln):
            return i
    return -1


def test_the_app_tree_is_copied_with_an_explicit_owner(lines):
    """Ownership, so the runtime user can read what it is about to import."""
    copy = _index_of(lines, r"^COPY\b.*\bapp\b\s+\./app")
    assert copy >= 0, "the Dockerfile no longer copies app/ — this test needs updating"
    assert "--chown=" in lines[copy], (
        "COPY app ./app must set --chown. COPY runs as root regardless of USER, so "
        "without it the tree is root-owned and unreadable to the runtime user."
    )


def test_the_app_tree_modes_are_set_explicitly(lines):
    """Modes, so a restrictive umask on the build machine cannot leak into the image."""
    assert any(re.search(r"^RUN\s+chmod\s+-R\b.*\/app\/app", ln) for ln in lines), (
        "the Dockerfile must chmod -R the copied tree. --chown alone only works while "
        "the runtime user happens to own it; an explicit chmod makes the image correct "
        "whatever umask built it."
    )


def test_privileges_are_dropped_after_the_copy_not_before(lines):
    """`USER` before `COPY` is what made this silent: the copy still ran as root, so the
    files landed root-owned while looking like they were written by the runtime user."""
    copy = _index_of(lines, r"^COPY\b.*\bapp\b\s+\./app")
    chmod = _index_of(lines, r"^RUN\s+chmod\s+-R\b.*\/app\/app")
    user = _index_of(lines, r"^USER\s+bedrock_agentcore")
    assert user >= 0, "the image must not run as root"
    assert copy < user, "COPY app ./app must come before USER, so the chmod can still run"
    assert chmod < user, "the chmod needs root, so it must come before USER"


def test_the_image_still_runs_as_an_unprivileged_user(lines):
    """The fix must not have been 'run as root', which would make all of this moot."""
    users = [ln for ln in lines if ln.startswith("USER ")]
    assert users, "no USER directive: the container would run as root"
    assert users[-1] == "USER bedrock_agentcore", (
        f"the last USER directive is {users[-1]!r}; the container must end up "
        f"unprivileged"
    )
