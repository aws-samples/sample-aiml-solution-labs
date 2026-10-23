# Interceptors

An interceptor is a Lambda function the AgentCore Gateway calls on every tool call: one before the request reaches the tool, one after the tool answers. A Cedar policy allows or denies a call; an interceptor can also change it, redact it or log it. Use interceptors for audit logs, argument checks, or masking personal data in tool results. Here you turn them on from templates on the **Interceptors** tab and learn the rules to watch for.

# Read the Interceptors tab

The tab's first line says: _Gateway level. Functions that run before each tool request and after each answer, for every agent._ Both interceptors need the Gateway, so a build with no tools has none. A build has at most one request and one response interceptor.

The Interceptors tab with both interceptors on:

![The Interceptors tab: Before each request and After each answer, each switched On, with Publish to library, The function set to Write it here or Yours by ARN, Pass the request headers, the templates under What it does, and the sections The code, What it may reach, Check and run, and Test the deployed interceptor](images/cp-tab-interceptors.png)

# Set up an interceptor

1. Turn on **Before each request** or **After each answer**.
1. Under **The function**, choose **Write it here** or **Yours, by ARN** (a Lambda of yours).
1. Under **What it does**, tick the templates you want. Each becomes a section of `handler.py`, with its own settings.
1. Open **The code** to read or edit the result.
1. Under **What it may reach**, grant only what the function needs.
1. Use **Check and run** to run a syntax check, lint and a security scan, then each test event in an AgentCore Code Interpreter sandbox.
1. After a deploy, use **Test the deployed interceptor**.

# Pick the templates

| Before each request | After each answer |
|---|---|
| **Audit log**: one line per request in CloudWatch Logs, argument names only | **Audit log**: one line per answer, never the content |
| **Block tools**: for every agent, or only the agents listed | **Redact personal data**: email, phone, US social security and card numbers |
| **Argument guard**: a size cap and denied patterns, such as an SQL DROP | **Hide tools** from the list agents are offered |
| **Inject run context**: session, agent or user into arguments | **Cap result size** |
| **Your own check**: return a reason to refuse the call | **Your own change**: change the answer and return it |

In `workflow.json` the interceptors are `orchestrator.interceptors`:

```json
"interceptors": {
  "request":  { "code": {}, "templates": { "audit": {}, "argumentGuard": { "maxArgumentChars": 20000 } } },
  "response": { "code": {}, "templates": { "redactPii": { "types": ["email"], "mask": "[EMAIL REDACTED]" } } }
}
```

# Know the rules

- **Pass the request headers** sends the run's session, agent and user headers to the function. They include the agent's Gateway token, so never log headers.
- The Gateway may call an interceptor twice for one request. Keep it free of side effects you would not want repeated.
- Only the Gateway's role may invoke an interceptor. One written in the build is deployed like a code tool, with its own role.
- A harness agent's tool calls carry no run headers.
- A refusal shows on the run's timeline as _Refused by interceptor: &lt;reason>_.

# See interceptors in a run

- A call the request interceptor refuses is a timeline line naming the reason. The agent is told why it was refused.
- An audit log writes one JSON line per request or answer to CloudWatch Logs.
- Analytics counts blocked calls per tool. See [Analytics](Analytics.md).
- On the canvas, an interceptor has no target on any agent card: it applies to every tool call at the Gateway.

# Share an interceptor

**Publish to library** keeps an interceptor for other builds. **Add from library** at the top of the tab adds one. Unlike other library items, a build that adds an interceptor takes a copy, so a later change in the library never changes another build. See [Library](Library.md).

# Ask the Assistant

The AgentExpress Assistant can turn either interceptor on or off, pick its templates and their settings, write the code of **Your own check** or **Your own change**, use a Lambda of yours by ARN, pass the request headers, and set what the function may reach or take its code from Git or S3. Attach your interceptor code and it uses the file as it is. Requests you can paste:

- "Add a request interceptor that writes an audit log and refuses arguments over 20,000 characters"
- "Redact email addresses and phone numbers in every tool answer"
- "Use my Lambda &lt;its ARN> as the response interceptor"
- "Turn off the request interceptor"

* The Assistant keeps code you edited by hand. Ask it to regenerate the code to rebuild it from the templates.
* **Check and run**, **Test the deployed interceptor**, **Add from library** and **Publish to library** are on the Interceptors tab.
* With **Review changes** on **Auto**, interceptor changes are checked by a second model before they are saved.

More requests are on [Ask the Assistant](Ask_the_Assistant.md).

# Related pages

- [Tools tab and the Gateway](Tools_and_Gateway.md): where interceptors run
- [Policies (Cedar)](Policies_and_Cedar.md): allow or deny each call
- [Analytics](Analytics.md): block and denial rates per tool
- [Triggers](Triggers.md): the next configuration tab
