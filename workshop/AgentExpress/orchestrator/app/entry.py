"""Container entrypoint dispatcher.

One image serves both roles; which one is chosen at container start:
  * AGENT_ID set   -> a per-agent runtime (app.subagent_runtime) that hosts a
                      single dedicated agent.
  * AGENT_ID unset -> the main orchestrator runtime (app.orchestrator.runtime).

This lets every dedicated agent get its own AgentCore Runtime from the same
image — the Terraform just sets AGENT_ID per runtime.
"""

import os

if os.getenv("AGENT_ID"):
    from app.subagent_runtime import app
else:
    from app.orchestrator.runtime import app

if __name__ == "__main__":
    app.run()
