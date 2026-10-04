"""AgentCore Memory — long-term semantic recall and store.

TWO memory resources are provisioned (terraform/main.tf), for two different jobs:
  * MEMORY_ID          — the LangGraph CHECKPOINTER (short-term run state; this is
                         what makes HITL pause/resume durable). Used by
                         AgentCoreMemorySaver in app/orchestrator/runtime.py.
  * SEMANTIC_MEMORY_ID — LONG-TERM memory with extraction strategies. Agents that
                         produce reusable knowledge store it here; future runs
                         recall relevant past insights automatically.

Scoping (per agent, per subject): the semantic strategy on the memory resource
uses the namespace template "insights/{actorId}". At run time the app sets
actorId = "<agentId>-<subjectSlug>", so recall/store land in a namespace like
"insights/analysis-acme". Each agent's insights stay isolated, and they persist
across runs for the SAME subject. The subject comes from `subject_id` in the
invoke payload (threaded through the graph state into AgentContext) — use it for
whatever your domain groups knowledge by (a customer, brand, product, project).
Omit it and memory is scoped per agent only.

Config in workflow.json (per agent):
  "memory": { "shortTerm": true, "longTerm": ["semantic"] }
  (longTerm truthy enables recall/store; the strategies themselves are
   provisioned once in terraform/main.tf as awscc_bedrockagentcore_memory.semantic.)

Environment:
  SEMANTIC_MEMORY_ID - the long-term memory resource (separate from MEMORY_ID)

Key files:
  app/features/memory/client.py  — recall() and store() implementations
  app/common/context.py          — memory_recall()/memory_store() (namespace + telemetry)
  app/orchestrator/nodes.py      — calls both around agent.run(), so enabling
                                   memory on ANY agent is a workflow.json edit
"""

from app.features.memory.client import recall, store  # noqa: F401
