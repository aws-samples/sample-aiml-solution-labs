"""
Generic tool call throttle hook for Strands Agents.

Enforces per-tool call budgets per agent invocation to prevent runaway
tool call loops where the LLM calls the same tool hundreds of times
instead of batching parameters or using higher-level tools.

Usage:
    from tool_throttle_hook import ToolThrottleHook

    hook = ToolThrottleHook(
        max_calls_per_tool=10,       # default budget per tool
        total_max_calls=50,          # total budget across all tools
    )

    agent = Agent(
        model=model,
        tools=tools,
        hooks=[hook],
    )
"""

import logging
from collections import defaultdict
from strands.hooks.registry import HookProvider, HookRegistry
from strands.hooks.events import BeforeInvocationEvent, BeforeToolCallEvent

logger = logging.getLogger(__name__)


class ToolThrottleHook(HookProvider):
    """Enforces per-tool and total tool call budgets per agent invocation.

    When a tool exceeds its budget, the call is cancelled with an error
    message returned to the model, nudging it to use a different approach.

    Args:
        max_calls_per_tool: Max times any single tool can be called per invocation.
        total_max_calls: Max total tool calls across all tools per invocation.
        tool_budgets: Optional dict of {tool_name: max_calls} for per-tool overrides.
    """

    def __init__(
        self,
        max_calls_per_tool: int = 10,
        total_max_calls: int = 50,
        tool_budgets: dict[str, int] | None = None,
    ):
        self.max_calls_per_tool = max_calls_per_tool
        self.total_max_calls = total_max_calls
        self.tool_budgets = tool_budgets or {}
        self._tool_counts: dict[str, int] = defaultdict(int)
        self._total_count: int = 0

    def register_hooks(self, registry: HookRegistry, **kwargs) -> None:
        """Register callbacks with the Strands hook registry."""
        registry.add_callback(BeforeInvocationEvent, self._on_invocation_start)
        registry.add_callback(BeforeToolCallEvent, self._on_before_tool_call)

    def _on_invocation_start(self, event: BeforeInvocationEvent) -> None:
        """Reset counters at the start of each agent invocation."""
        self._tool_counts.clear()
        self._total_count = 0
        logger.info("ToolThrottleHook: counters reset for new invocation")

    def _on_before_tool_call(self, event: BeforeToolCallEvent) -> None:
        """Check and enforce tool call budgets before each tool execution."""
        tool_name = event.tool_use.get("name", "unknown")
        budget = self.tool_budgets.get(tool_name, self.max_calls_per_tool)

        # Check per-tool budget
        if self._tool_counts[tool_name] >= budget:
            msg = (
                f"Tool '{tool_name}' has been called {self._tool_counts[tool_name]} times "
                f"(budget: {budget}). Call cancelled. "
                f"Try a different approach — use a batch/what-if tool, combine parameters "
                f"into fewer calls, or produce your final answer with the data you have."
            )
            logger.warning(f"THROTTLED: {msg}")
            event.cancel_tool = msg
            return

        # Check total budget
        if self._total_count >= self.total_max_calls:
            msg = (
                f"Total tool call limit reached ({self.total_max_calls} calls). "
                f"Call to '{tool_name}' cancelled. "
                f"Produce your final answer with the data you have."
            )
            logger.warning(f"THROTTLED: {msg}")
            event.cancel_tool = msg
            return

        # Allow the call and increment counters
        self._tool_counts[tool_name] += 1
        self._total_count += 1
        logger.info(
            f"ToolThrottleHook: {tool_name} "
            f"({self._tool_counts[tool_name]}/{budget}, "
            f"total: {self._total_count}/{self.total_max_calls})"
        )
