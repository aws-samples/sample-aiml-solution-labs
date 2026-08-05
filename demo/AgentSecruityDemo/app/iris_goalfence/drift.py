"""Semantic-drift scorer — the ONE part of the fence that makes a network call.

This lives OUTSIDE the pure engine on purpose. The engine only compares a float
(action.drift_score) to the charter threshold; computing that float happens here.

WHY DUAL ANCHORING (not cosine-to-one-goal-string):
    A single goal sentence is one point in embedding space, and every "support about
    orders" trajectory — legit OR malicious — lands in the same neighborhood (shared
    vocabulary: order, customer, refund, records). Measured live, cosine-to-goal gave a
    separation gap of -0.04: on-goal (0.67–0.74) and off-goal (0.70–0.78) OVERLAPPED, so
    normal requests false-flagged. Embeddings capture TOPIC, not INTENT.

    Dual anchoring fixes the GEOMETRY: embed a small set of ON-GOAL exemplars (centroid)
    and a set of ANTI-GOAL exemplars (centroid), and score by which the trajectory is
    closer to:

        drift = 0.5 + (cos(traj, anti_centroid) - cos(traj, goal_centroid)) / 2

    >0.5 leans off-goal, <0.5 leans on-goal. Live this separated 6/6 with a clear margin.

COST: both centroids embed ONCE (lazy) and cache forever — they never change. Per invoke
we embed only the trajectory (user+assistant turns, not system/tool plumbing), so it's one
embedding call per user turn, ~180 ms, off the tool-call path.

Titan Text Embeddings V2 at 256 dims (~97% of full accuracy, smaller/faster vector). No
prompt caching exists for embedding models (that's a generative-model feature); the
centroid cache above is our own and is all we need.
"""
from __future__ import annotations

import json
import math
import os

_DEFAULT_MODEL = os.environ.get("DRIFT_EMBED_MODEL", "amazon.titan-embed-text-v2:0")
_DEFAULT_DIMS = int(os.environ.get("DRIFT_EMBED_DIMS", "256"))


class DriftScorer:
    """Dual-anchor semantic drift. Reusable across invokes; centroids cached lazily.

    embed_fn lets tests inject a deterministic stub (str -> list[float]) so unit tests need
    no AWS creds. In production it's None and we lazily build a Bedrock client.
    """

    def __init__(self, examples, anti_examples, *, embed_fn=None, client=None,
                 region: str | None = None, model_id: str = _DEFAULT_MODEL,
                 dims: int = _DEFAULT_DIMS):
        if not examples or not anti_examples:
            raise ValueError("DriftScorer needs both on-goal examples and anti-examples")
        self.examples = tuple(examples)
        self.anti_examples = tuple(anti_examples)
        self.model_id = model_id
        self.dims = dims
        self._embed_fn = embed_fn
        self._client = client
        self._region = region or os.environ.get("AWS_REGION", "us-east-1")
        self._goal_centroid: list[float] | None = None
        self._anti_centroid: list[float] | None = None

    # -- embedding plumbing -------------------------------------------------------------
    def _bedrock(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("bedrock-runtime", region_name=self._region)
        return self._client

    def _embed(self, text: str) -> list[float]:
        if self._embed_fn is not None:          # test / custom path
            return self._embed_fn(text)
        body = {"inputText": text, "dimensions": self.dims, "normalize": True}
        resp = self._bedrock().invoke_model(
            modelId=self.model_id, body=json.dumps(body),
            accept="application/json", contentType="application/json")
        return json.loads(resp["body"].read())["embedding"]

    # -- math ---------------------------------------------------------------------------
    @staticmethod
    def _centroid(vecs: list[list[float]]) -> list[float]:
        n = len(vecs[0])
        return [sum(v[i] for v in vecs) / len(vecs) for i in range(n)]

    @staticmethod
    def _cosine(a: list[float], b: list[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(y * y for y in b))
        if na == 0 or nb == 0:
            return 0.0
        return dot / (na * nb)

    def _centroids(self) -> tuple[list[float], list[float]]:
        if self._goal_centroid is None:
            self._goal_centroid = self._centroid([self._embed(t) for t in self.examples])
            self._anti_centroid = self._centroid([self._embed(t) for t in self.anti_examples])
        return self._goal_centroid, self._anti_centroid

    # -- input --------------------------------------------------------------------------
    @staticmethod
    def trajectory_text(turns: list[tuple[str, str]]) -> str:
        """Text to embed from (role, content) turns — ONLY user + assistant (drop
        system / tool plumbing, which would anchor everything near the goal)."""
        keep = [c for role, c in turns if role in ("user", "assistant") and c]
        return "\n".join(keep).strip()

    # -- scoring ------------------------------------------------------------------------
    def score(self, turns: list[tuple[str, str]]) -> float:
        """drift in [0,1] for this trajectory; >0.5 leans off-goal. One embedding call
        (centroids are cached). 0.5 (neutral) when there's nothing to score."""
        traj = self.trajectory_text(turns)
        if not traj:
            return 0.5
        goal_c, anti_c = self._centroids()
        v = self._embed(traj)
        margin = self._cosine(v, anti_c) - self._cosine(v, goal_c)  # >0 => off-goal
        return max(0.0, min(1.0, 0.5 + margin / 2))
