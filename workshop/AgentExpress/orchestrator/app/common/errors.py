"""Failure types for unavailable dependencies.

This framework does NOT fabricate data. When a model call or a tool call cannot be
completed, the agent raises and the run fails with the reason attached to the agent
that failed (app/orchestrator/nodes.py turns it into a `failed` node status and a
timeline line).

That is deliberate. A simulated answer is worse than an error: it looks like real
evidence, flows into every downstream agent, and lands in the final report with no
signal that the underlying tool never ran. An operator reading a report cannot tell
the difference; an error is unmissable.

The consequence to be aware of: there is no offline mode. Running the graph requires
working Bedrock credentials, and any tool an agent is bound to must be reachable.
"""


class DependencyUnavailable(RuntimeError):
    """Base: a dependency the agent needs is not usable."""


class ModelUnavailable(DependencyUnavailable):
    """The Bedrock model call failed (credentials, access, throttling, model id)."""


class ToolUnavailable(DependencyUnavailable):
    """A Gateway tool could not be called, or is not published by its target."""


class ToolDenied(DependencyUnavailable):
    """The Cedar policy engine refused the tool call.

    Distinct from ToolUnavailable: this is the authorization layer working as
    configured, not an outage. It still fails the run, because the agent cannot
    produce grounded output without the evidence it was denied.
    """


class RemoteAgentUnavailable(DependencyUnavailable):
    """An agent this workflow does not operate could not be reached or did not finish.

    Its own class because the blast radius is different from every other failure
    here: a `runtime: "a2a"` step is somebody else's service, so the cause is
    usually outside this deployment entirely — their endpoint moved, their auth
    rejected us, their task failed or never left a working state. The message
    carries whatever they told us, because that is the only diagnostic available on
    this side of the boundary.
    """


class ModelOutputUnusable(DependencyUnavailable):
    """The model answered, but the response could not be used.

    Raised instead of emitting an empty-but-well-formed asset: a downstream agent
    cannot tell an empty asset from a genuine "nothing to report", so it would
    reason over a gap as if it were a finding.
    """
