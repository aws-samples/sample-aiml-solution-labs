# Demo script (about 10 minutes)

Before: the build is deployed (README, "Load and deploy") and you are signed in to its app as
the owner. Run it once beforehand so the Knowledge Base, the harnesses and the dedicated runtime
are warm and the memory has something to recall.

**1. The build (Builder, Design tab).** "One claim, handled end to end. Each box is a step: task
steps with no model, agents in this container, one in its own runtime, two managed harnesses,
and an outside agent over A2A. The Tools tab has a knowledge base, web search, an MCP server, a
REST API from an OpenAPI document, a function we wrote here, the code sandbox, the browser and an
AWS API from its Smithy model." Open **Policies**: "This forbid refuses claim CLM-2002, another
customer's." Open **Guardrails**: "This one masks phone numbers in the reviewer's output."

**2. Start the run.** Open the app, keep the pre-filled request, **Start run**. Optional:
Advanced → Model override → Battery specialist → Nova Lite.

**3. Graph.** "Claim intake ran Python in the Code Interpreter and decided the route: standard,
so Fraud review is skipped. Nine steps now run at once." Point at the harness rows and the
dedicated runtime.

**4. Timeline.** Find `Refused by Cedar policy: claimsDb___get_claim(...)`: "The drafter tried to
read CLM-2002. The Gateway refused it; the model never saw that record." Point at
`[EMAIL REDACTED]` in the CLM-1001 result: "The response interceptor did that."

**5. The question.** The drafter asks which language to write in. Type `English` (or `Spanish`).

**6. Flow.** "Every call in order: which agent called which tool, through the Gateway, and
what came back."

**7. Sign-off.** Open the reviewer's `finalLetter`: the phone number reads `{PHONE}`. "The named
guardrail masked it." Show the compliance reviewer's answer from the A2A agent. **Approve**.

**8. Files.** `claim-facts.json` from intake, the sandbox's code and log, and
`claim-report-<session>.md`.

**9. Observability.** Cost per step; the reviewer's calls show `route 1` (Nova Pro, because
the request mentions a battery); the evaluation scores; the guardrail intervention.

**10. If there is time.** Live config → Policy check → change the prompt → save, activate →
start another run: it shows `config v1`. Or start a run with `BLOCKED_DEMO_TERM` in the request:
the deployment guardrail stops it at Claim intake.
