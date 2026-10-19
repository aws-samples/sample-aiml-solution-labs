"""AgentCore Policy: Cedar authorization, enforced at the Gateway.

AgentCore Policy is enforced server-side by the AgentCore Gateway: a policy engine with
Cedar permit/forbid rules is attached to the Gateway (terraform/policy.tf, gateway.tf;
the CDK tool plane), and the Gateway evaluates every tool call against it, in LOG_ONLY or
ENFORCE mode (workflow.json orchestrator.policy). Actions are AgentCore::Action::
"<target>___<tool>" (or a whole target), the resource is the Gateway.
https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/policy-understanding-cedar.html

AgentCore has no API that returns a Cedar decision for agent code outside a Gateway call,
so this framework offers none. (An earlier `is_authorized` helper called an operation that
does not exist and so always allowed; it is gone.) To govern an action, put it behind a
Gateway tool, where the attached engine decides.
"""
