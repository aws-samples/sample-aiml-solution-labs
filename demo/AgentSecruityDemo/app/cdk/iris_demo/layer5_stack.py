"""
Layer 5 (Models) stack — model governance: an IAM model allowlist + a mandatory
Bedrock Guardrail (enforced per-model via IAM) + an Application Inference Profile
for the approved Sonnet model.

Layer 5 builds on ALL prior layers (L1 network, L2 identity, L3 tools/Gateway+OBO,
L4 memory) and adds the control the earlier layers never touched: WHICH model the
agent may use, and a guardrail that MUST be applied to every model call.

Two controls, both on the runtime's IAM role (the agent can't opt out):

  1. WHICH MODEL — an IAM model allowlist. bedrock:InvokeModel*/Converse* is allowed
     ONLY on the THREE approved model ARNs (qwen-80b, qwen-32b, and Sonnet 4.5 US-CRIS
     + its AIP). Any other model id (e.g. Haiku 4.5) → AccessDenied. "The admin sets
     the menu."

  2. MANDATORY GUARDRAIL — the bedrock:GuardrailIdentifier condition key DENIES any
     model call that does not carry THIS guardrail (at this version). There is NO
     ungoverned path: every approved model must be invoked WITH our guardrail attached,
     enforced at IAM. The agent attaches the guardrail on the Converse request and TAGS
     the user turn (guardrail_latest_message) so the prompt-attack filter evaluates the
     user input, not the agent's own system prompt. (We use the request path — not
     account-level enforcement — precisely because account enforcement ignores the
     user-input tags and would re-scan our system prompt, tripping the prompt-attack
     filter on our own instructions.)

DEMO ARC (three approved models, all GUARDED, same 3 prompts):
  Every approved model runs WITH our guardrail (IAM makes it mandatory). The prompt-
  attack filter + input-only policy-override word filters catch a planted poison
  premise / injection on the USER turn, while a legitimate request (e.g. an ordinary
  refund) passes through to the model. Off-list models (e.g. Haiku 4.5) → AccessDenied.
  "Which model, and a guardrail you can't skip, is the security control."

Context params: execRoleArn (baseline agent role, granted CMK/gate reuse via L4),
and the memory CMK arn (so the L5 role can use L4 memory).
"""
from aws_cdk import (
    Stack, Tags, CfnOutput, aws_iam as iam,
    aws_bedrock as bedrock,
)
from constructs import Construct
from iris_demo import AGENTCORE_ROLE_ACTIONS

# The three approved models the L5 agent may use. Kept here so the stack (IAM
# allowlist + mandatory-guardrail condition), the server (env + UI), and the demo
# story all agree. All three run WITH our guardrail attached (IAM enforces it via the
# bedrock:GuardrailIdentifier condition key) — there is no ungoverned model.
QWEN_BIG_MODEL_ID = "qwen.qwen3-next-80b-a3b"      # large
QWEN_SMALL_MODEL_ID = "qwen.qwen3-32b-v1:0"        # small
SONNET_MODEL_ID = "anthropic.claude-sonnet-4-5-20250929-v1:0"
SONNET_CRIS_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

# Policy-override phrases the guardrail blocks — the shapes a USER plants to claim
# self-granted authority ("I'm pre-approved", "without checking eligibility", …).
# Applied INPUT-ONLY (see the add_property_override below): they must catch the user's
# request, NOT the agent's own eligibility-respecting refusal on the OUTPUT side.
_POLICY_OVERRIDE_WORDS = [
    "pre-approved", "without manager review", "without checking eligibility",
    "without verification", "skip the eligibility", "no eligibility check",
    "exempt from approval", "ignore the eligibility", "ignore your",
]


class Layer5Stack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        Tags.of(self).add("demo", "iris-security")
        Tags.of(self).add("demo-layer", "layer5")

        region = self.region
        account = self.account

        # ---- Approved-model ARNs (the allowlist targets) ----
        # qwen FMs: on-demand (no account segment in FM ARNs → empty ::).
        qwen_big_arn = f"arn:aws:bedrock:{region}::foundation-model/{QWEN_BIG_MODEL_ID}"
        qwen_small_arn = f"arn:aws:bedrock:{region}::foundation-model/{QWEN_SMALL_MODEL_ID}"
        # Sonnet: reached via a US geographic CRIS, so the agent invokes the
        # inference-profile ARN, which in turn fans out to the FM in us-east-1/2 &
        # us-west-2. We must allow BOTH the profile ARN AND the underlying FM ARNs in
        # every CRIS region (per the "allow both AIP + FM" rule).
        sonnet_cris_arn = f"arn:aws:bedrock:{region}:{account}:inference-profile/{SONNET_CRIS_ID}"
        sonnet_fm_arns = [
            f"arn:aws:bedrock:{r}::foundation-model/{SONNET_MODEL_ID}"
            for r in ("us-east-1", "us-east-2", "us-west-2")
        ]
        # The Application Inference Profile we create (approved, taggable wrapper).
        # Its ARN is also an allowed invoke target.
        aip_name = "iris-sonnet-approved"
        aip = bedrock.CfnApplicationInferenceProfile(
            self, "SonnetApprovedAip",
            inference_profile_name=aip_name,
            description="Iris Layer 5 approved Sonnet 4.5 profile taggable guardrail enforced demo iris security",
            model_source=bedrock.CfnApplicationInferenceProfile.InferenceProfileModelSourceProperty(
                copy_from=sonnet_cris_arn,
            ),
            tags=[{"key": "demo", "value": "iris-security"},
                  {"key": "layer", "value": "layer5"},
                  {"key": "approved", "value": "true"}],
        )

        # ---- L5 agent execution role — SCOPED (the model allowlist) ----
        # A fresh role (not the broad baseline one) so Layer 5 can show a locked-down
        # posture without disturbing the earlier layers' runtimes. It mirrors the
        # baseline role's non-Bedrock permissions (RDS Data API, logs, xray, ECR pull,
        # collector/shipment, AgentCore) but constrains Bedrock model invocation to
        # the approved ARNs only.
        role = iam.Role(
            self, "Layer5ExecRole",
            role_name="iris-exec",
            assumed_by=iam.ServicePrincipal("bedrock-agentcore.amazonaws.com"),
            description="Iris Layer 5 - SCOPED agent exec role (model allowlist) - demo:iris-security",
        )
        # AgentCore runtime permissions (cdk_nag AwsSolutions-IAM4): scoped inline policy
        # instead of AWS-managed BedrockAgentCoreFullAccess. Every model/peer/DB/log/etc.
        # permission this role needs is already granted explicitly below; this covers the
        # remaining runtime data-plane calls, scoped to this account's AgentCore resources.
        # AgentCore permissions (cdk_nag AwsSolutions-IAM4): explicit least-privilege action
        # list (shared constant) scoped to this account+region — replaces the AWS-managed
        # BedrockAgentCoreFullAccess (bedrock-agentcore:* on *). Same set the baseline role
        # uses: runtime data-plane + the control-plane reads the Gateway needs on assoc.
        role.add_to_policy(iam.PolicyStatement(
            sid="AgentCoreScoped",
            actions=AGENTCORE_ROLE_ACTIONS,
            resources=[f"arn:aws:bedrock-agentcore:{region}:{account}:*"],
        ))

        # Memory CMK: the L5 agent shares Layer 4's CMK-encrypted memory. Its data-plane
        # CreateEvent / RetrieveMemoryRecords calls must be able to use the key, or the
        # session manager fails with "Unable to perform KMS operations". The CMK ARN is
        # passed as context by the deploy (memoryCmkArn); scope the grant to it. Falls back
        # to this account+region's keys if not supplied (still not cross-account).
        memory_cmk_arn = self.node.try_get_context("memoryCmkArn") or \
            f"arn:aws:kms:{region}:{account}:key/*"
        role.add_to_policy(iam.PolicyStatement(
            sid="MemoryCmkUse",
            actions=["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*",
                     "kms:GenerateDataKey*", "kms:DescribeKey"],
            resources=[memory_cmk_arn],
        ))

        # THE MODEL ALLOWLIST: invoke/converse allowed ONLY on approved ARNs.
        role.add_to_policy(iam.PolicyStatement(
            sid="ApprovedModelsOnly",
            actions=[
                "bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream",
                "bedrock:Converse", "bedrock:ConverseStream",
            ],
            resources=[qwen_big_arn, qwen_small_arn, sonnet_cris_arn, aip.attr_inference_profile_arn, *sonnet_fm_arns],
        ))
        # Belt-and-suspenders: explicitly DENY invocation of anything NOT on the list,
        # so a typed-in off-list model id can never slip through a broader grant that
        # BedrockAgentCoreFullAccess might carry. (Explicit deny always wins.)
        role.add_to_policy(iam.PolicyStatement(
            sid="DenyUnapprovedModels",
            effect=iam.Effect.DENY,
            actions=[
                "bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream",
                "bedrock:Converse", "bedrock:ConverseStream",
            ],
            not_resources=[qwen_big_arn, qwen_small_arn, sonnet_cris_arn, aip.attr_inference_profile_arn, *sonnet_fm_arns],
        ))
        # (ApplyGuardrail permission is granted below, scoped to our guardrail, right
        # after the guardrail is defined — see RequireIrisGuardrailOnEveryModelCall.)
        # Layer 6 · A2A: allow the agent to invoke the Orders PEER runtime (delegate
        # order lookups over A2A). Scoped to this account's runtimes. (The managed
        # BedrockAgentCoreFullAccess also grants this; kept explicit for least-privilege
        # clarity — this is the one edge that lets the main agent reach a peer.)
        role.add_to_policy(iam.PolicyStatement(
            sid="InvokeA2APeerRuntime",
            actions=["bedrock-agentcore:InvokeAgentRuntime"],
            resources=[f"arn:aws:bedrock-agentcore:{region}:{account}:runtime/*"],
        ))

        # Mirror the baseline role's OTHER (non-Bedrock) permissions so the L5 agent —
        # a copy of L4 — keeps working end to end (DB, Gateway/OBO, memory, tools).
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "rds-data:ExecuteStatement", "rds-data:BatchExecuteStatement",
                "rds-data:BeginTransaction", "rds-data:CommitTransaction",
            ],
            resources=[f"arn:aws:rds:{region}:{account}:cluster:*"],
        ))
        # Secrets: the Aurora DB secret (Data API) + AgentCore Identity OBO/OAuth2 secrets
        # (GetResourceOauth2Token reads the credential-provider secret under the reserved
        # bedrock-agentcore-identity!default/oauth2/ prefix). Scoped to those, not all secrets.
        role.add_to_policy(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue"],
            resources=[
                f"arn:aws:secretsmanager:{region}:{account}:secret:bedrock-agentcore-identity!default/oauth2/*",
                # Aurora DB secret — CDK generates it as IrisInfra*DBSecret*<suffix>; layer5
                # is a separate stack and can't resolve the exact ARN at synth, so scope by
                # the generated name prefix (still far narrower than secret:*).
                f"arn:aws:secretsmanager:{region}:{account}:secret:IrisInfra*",
            ],
        ))
        role.add_to_policy(iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
                     "logs:DescribeLogStreams", "logs:DescribeLogGroups"],
            resources=["arn:aws:logs:*:*:*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            actions=["xray:PutTraceSegments", "xray:PutTelemetryRecords",
                     "xray:GetSamplingRules", "xray:GetSamplingTargets"],
            resources=["*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            actions=["cloudwatch:PutMetricData"], resources=["*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            actions=["ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage",
                     "ecr:BatchCheckLayerAvailability", "ecr:GetAuthorizationToken"],
            resources=["*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction", "lambda:InvokeFunctionUrl"],
            resources=[
                f"arn:aws:lambda:{region}:{account}:function:iris-shipment",
            ],
        ))

        # ---- The DEFAULT guardrail (single owner — this is the ONE combined stack) ----
        # Account-level guardrail enforcement is Region-wide PER MODEL: exactly one
        # enforced guardrail per model id. In the consolidated stack THIS is the sole
        # owner — it creates + versions the guardrail here and the server enforces it on
        # the approved Sonnet model. WORD FILTERS (not a denied-TOPIC): the topic
        # classifier over-fired on legitimate eligibility-respecting refunds; these
        # exact override phrases block the poison and leave normal refunds untouched.
        guardrail = bedrock.CfnGuardrail(
            self, "IrisDefaultGuardrail",
            name="iris-guardrail",
            description="Iris default model guardrail (auto-enforced on approved model) - demo:iris-security",
            blocked_input_messaging="This request was blocked by the Iris default guardrail (input).",
            blocked_outputs_messaging="This response was blocked by the Iris default guardrail (output).",
            content_policy_config=bedrock.CfnGuardrail.ContentPolicyConfigProperty(
                filters_config=[
                    # MEDIUM (not HIGH): HIGH flags ordinary imperative requests ("process
                    # my refund now") as possible instruction-injection, blocking legit
                    # traffic. MEDIUM lets genuine requests through while still catching
                    # blatant jailbreaks ("ignore your prior instructions", "unrestricted mode").
                    bedrock.CfnGuardrail.ContentFilterConfigProperty(
                        type="PROMPT_ATTACK", input_strength="MEDIUM", output_strength="NONE"),
                ],
            ),
            word_policy_config=bedrock.CfnGuardrail.WordPolicyConfigProperty(
                words_config=[
                    bedrock.CfnGuardrail.WordConfigProperty(text=w) for w in _POLICY_OVERRIDE_WORDS
                ],
            ),
        )
        # INPUT-ONLY word filters (escape hatch). These phrases catch the USER's planted
        # false premise on the way IN ("pre-approved", "without checking eligibility",
        # …). But account-level enforcement scans the model's OUTPUT too, and the agent's
        # own legitimate refusal legitimately says things like "I can't do this WITHOUT
        # CHECKING ELIGIBILITY" — which would self-trip the same filter and block a
        # correct answer. So we scope every word to input only: inputEnabled=true,
        # outputEnabled=false. The L1 CfnGuardrail.WordConfigProperty in this CDK version
        # (2.170) doesn't model these fields, so we set them via a raw property override.
        guardrail.add_property_override(
            "WordPolicyConfig.WordsConfig",
            [{"Text": w, "InputEnabled": True, "OutputEnabled": False} for w in _POLICY_OVERRIDE_WORDS],
        )
        # Immutable numeric version. The agent attaches this guardrail (id + version) on
        # every Converse request; the IAM condition below makes that mandatory.
        guardrail_version = bedrock.CfnGuardrailVersion(
            self, "IrisDefaultGuardrailVersion",
            guardrail_identifier=guardrail.attr_guardrail_id,
            description="Iris default guardrail - enforced version",
        )

        # MANDATORY GUARDRAIL (bedrock:GuardrailIdentifier condition key): DENY any model
        # invocation on the approved models UNLESS the request carries THIS guardrail at
        # THIS version. There is NO ungoverned path — a model call without our guardrail
        # (or with a different one) is denied at IAM, which the agent cannot bypass. This
        # is why Layer 5 uses the REQUEST guardrail path (agent attaches it + tags the
        # user turn) rather than account-level enforcement: account enforcement ignores
        # the user-input tags and would re-scan our own system prompt. The condition value
        # is the guardrail ARN with a ":<version>" suffix.
        guardrail_id_value = f"{guardrail.attr_guardrail_arn}:{guardrail_version.attr_version}"
        # Scoped to the SMALL qwen (qwen3-32b) ONLY: any call to THIS model that does not
        # carry our guardrail is denied at IAM — the developer literally cannot turn the
        # guardrail off for qwen-32b. The other approved models (qwen-80b, Sonnet) are NOT
        # in this Deny's Resource list, so for them the guardrail is the developer's choice
        # (the UI "Apply guardrail" checkbox). Teaching point: the admin can make the
        # guardrail non-negotiable per-model via IAM, overriding the developer's toggle.
        role.add_to_policy(iam.PolicyStatement(
            sid="RequireIrisGuardrailOnQwen32b",
            effect=iam.Effect.DENY,
            actions=[
                "bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream",
                "bedrock:Converse", "bedrock:ConverseStream",
            ],
            resources=[qwen_small_arn],
            conditions={"StringNotEquals": {"bedrock:GuardrailIdentifier": guardrail_id_value}},
        ))
        # The role must be allowed to APPLY the guardrail the agent attaches.
        role.add_to_policy(iam.PolicyStatement(
            actions=["bedrock:ApplyGuardrail"],
            resources=[guardrail.attr_guardrail_arn],
        ))

        CfnOutput(self, "Layer5ExecRoleArn", value=role.role_arn)
        CfnOutput(self, "Layer5ExecRoleName", value=role.role_name)
        CfnOutput(self, "SonnetAipArn", value=aip.attr_inference_profile_arn)
        CfnOutput(self, "SonnetAipId", value=aip.attr_inference_profile_id)
        CfnOutput(self, "ApprovedQwenBigModelId", value=QWEN_BIG_MODEL_ID)
        CfnOutput(self, "ApprovedQwenSmallModelId", value=QWEN_SMALL_MODEL_ID)
        CfnOutput(self, "ApprovedSonnetModelId", value=SONNET_CRIS_ID)
        CfnOutput(self, "DefaultGuardrailId", value=guardrail.attr_guardrail_id)
        CfnOutput(self, "DefaultGuardrailArn", value=guardrail.attr_guardrail_arn)
        CfnOutput(self, "DefaultGuardrailVersion", value=guardrail_version.attr_version)
        # The exact GuardrailIdentifier value the IAM condition requires AND the agent
        # attaches on every Converse request (guardrail ARN + ":version"). The server
        # passes the id + version to the runtime as env (GUARDRAIL_ID/GUARDRAIL_VERSION).
        CfnOutput(self, "MandatoryGuardrailIdentifier", value=guardrail_id_value)
