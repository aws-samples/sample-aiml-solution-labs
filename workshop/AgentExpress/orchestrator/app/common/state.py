"""Shared graph state.

Dict-typed channels use a merge reducer so that agents running in parallel
(e.g. the research group) can update different keys concurrently without conflicts.
"""

from typing import Annotated, TypedDict


def merge_dict(a: dict, b: dict) -> dict:
    return {**(a or {}), **(b or {})}


class State(TypedDict, total=False):
    topic: str                               # the user's request; consumed by the intake agent
    subject_id: str                          # optional grouping key; scopes long-term memory
    user: str                                # authenticated user (Cognito email/sub); for cost attribution
    status: Annotated[dict, merge_dict]      # agent_id -> status
    outputs: Annotated[dict, merge_dict]     # agent_id -> output text
    decisions: Annotated[dict, merge_dict]   # agent_id / group_id -> approve | deny | revise (HITL gates)
    feedback: Annotated[dict, merge_dict]    # agent_id -> reviewer feedback for a re-run
    group_rerun: Annotated[dict, merge_dict]  # group_id -> [agent ids to re-run this cycle]
    branch: Annotated[dict, merge_dict]      # branch step name -> step name chosen from the agent's output
    history: Annotated[dict, merge_dict]     # agent_id -> [{version, at, comment, output}] per re-run
