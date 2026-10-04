"""app/common/runtime_secret.py: the runtime's credentials come from Secrets Manager,
not from its environment variables."""

import json

import pytest

from app.common import runtime_secret


class SM:
    def __init__(self, value):
        self.value, self.asked = value, []

    def get_secret_value(self, SecretId):
        self.asked.append(SecretId)
        return {"SecretString": json.dumps(self.value)}


@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    monkeypatch.setattr(runtime_secret, "_loaded", False)
    for k in (*runtime_secret.KEYS, "RUNTIME_SECRET_ARN", "PATH_INJECTED"):
        monkeypatch.delenv(k, raising=False)
    yield


def test_copies_only_the_known_keys_and_encodes_objects(monkeypatch):
    monkeypatch.setenv("RUNTIME_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret:x")
    sm = SM({"GATEWAY_CLIENT_SECRET": "s3cr3t", "A2A_TOKENS": {"credit": "tok"},
             "PATH_INJECTED": "nope"})
    assert sorted(runtime_secret.load(sm)) == ["A2A_TOKENS", "GATEWAY_CLIENT_SECRET"]
    import os
    assert os.environ["GATEWAY_CLIENT_SECRET"] == "s3cr3t"
    assert json.loads(os.environ["A2A_TOKENS"]) == {"credit": "tok"}
    assert "PATH_INJECTED" not in os.environ
    # Once per process.
    assert runtime_secret.load(sm) == [] and len(sm.asked) == 1


def test_an_explicit_environment_value_wins(monkeypatch):
    monkeypatch.setenv("RUNTIME_SECRET_ARN", "arn:x")
    monkeypatch.setenv("GATEWAY_CLIENT_SECRET", "local")
    runtime_secret.load(SM({"GATEWAY_CLIENT_SECRET": "remote"}))
    import os
    assert os.environ["GATEWAY_CLIENT_SECRET"] == "local"


def test_nothing_happens_without_an_arn():
    assert runtime_secret.load(SM({"GATEWAY_CLIENT_SECRET": "x"})) == []


def test_an_unreadable_secret_stops_the_container(monkeypatch):
    monkeypatch.setenv("RUNTIME_SECRET_ARN", "arn:x")

    class Denied:
        def get_secret_value(self, SecretId):
            raise PermissionError("AccessDenied")
    with pytest.raises(PermissionError):
        runtime_secret.load(Denied())
