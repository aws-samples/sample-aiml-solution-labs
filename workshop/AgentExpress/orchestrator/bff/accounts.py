"""Connected AWS accounts: where a user deploys their builds, instead of this console's.

Connecting is one CloudFormation stack the user launches IN THEIR ACCOUNT. It creates one
IAM role, `AgentExpressDeploy-<id>`, that:

  * trusts only this console's deploy project (and this BFF, to verify the connection),
  * requires an External ID unique to that user and account, so no other user of this
    console can point a deploy at it — the classic confused-deputy guard,
  * holds what a deploy needs and no more (see `template`): ReadOnlyAccess, the
    Terraform path's documented permissions (terraform/deploy-role-policy.json, the
    same file this console's own deploy project carries), and what the CDK path needs
    to bootstrap the account and hand off to CDK's own roles (default qualifier,
    cdk-hnb659fds-*). The role itself has no AdministratorAccess, but CDK deploys run as
    the bootstrap's CloudFormation execution role, which is AdministratorAccess unless
    the account was bootstrapped with --cloudformation-execution-policies. Bootstrap it
    yourself with a narrower policy before connecting if that matters to you.

The deploy runner assumes it for that one job; nothing is stored but the role's ARN and
the External ID. Deleting the stack in their account disconnects it for good.

Connections are private to the user, like builds and runs.
"""

from __future__ import annotations

import json
import os
import secrets
import urllib.parse
from pathlib import Path

import boto3
import buildstore
from boto3.dynamodb.conditions import Key

REGION = os.environ.get("AWS_REGION", "us-east-1")
#: The roles a connected account's role must trust: the deploy project's, and this
#: BFF's (to verify). Set by the IaC.
DEPLOY_ROLE_ARN = os.environ.get("DEPLOY_ROLE_ARN", "")
BFF_ROLE_ARN = os.environ.get("BFF_ROLE_ARN", "")


#: IAM's limit for one managed policy, counted without whitespace. Packed a little under.
POLICY_LIMIT = 6144
_POLICY_BUDGET = 6000


CONSOLE_ONLY_SIDS = {"BuilderPlane"}


def _deploy_statements() -> list[dict]:
    """terraform/deploy-role-policy.json: staged next to this module in the BFF package,
    in ../terraform in a source checkout."""
    here = Path(__file__).resolve().parent
    for path in (here / "deploy-role-policy.json",
                 here.parent / "terraform" / "deploy-role-policy.json"):
        if path.exists():
            # Less the statement for deploying a console's own Builder: a build never
            # creates one, so a connected account never needs it.
            return [st for st in json.loads(path.read_text())["Statement"]
                    if st.get("Sid") not in CONSOLE_ONLY_SIDS]
    raise FileNotFoundError("deploy-role-policy.json is not in the BFF package")


#: What the CDK path needs in a connected account beyond the Terraform set. The runner
#: bootstraps an account that is not (CloudFormation stack CDKToolkit and the cdk-*
#: roles, bucket, repository and SSM parameter it creates), then `cdk deploy` works
#: through those cdk-* roles, so the role here only has to be able to assume them.
#: Plus the two things the runner does itself after a deploy or destroy: invite the
#: build's owner to its user pool, and delete the log groups a destroy leaves behind
#: (named by the build's ax_<id>, which nothing else in the account carries).
CDK_AND_RUNNER_STATEMENTS = [
    {"Sid": "CdkBootstrapStack", "Effect": "Allow", "Action": "cloudformation:*",
     "Resource": "arn:aws:cloudformation:*:*:stack/CDKToolkit/*"},
    {"Sid": "CdkBootstrapRoles", "Effect": "Allow",
     "Action": ["iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:TagRole",
                "iam:UntagRole", "iam:PutRolePolicy", "iam:GetRolePolicy",
                "iam:DeleteRolePolicy", "iam:AttachRolePolicy", "iam:DetachRolePolicy",
                "iam:UpdateAssumeRolePolicy", "iam:PassRole"],
     "Resource": "arn:aws:iam::*:role/cdk-hnb659fds-*"},
    {"Sid": "CdkBootstrapPolicies", "Effect": "Allow",
     "Action": ["iam:CreatePolicy", "iam:DeletePolicy", "iam:GetPolicy",
                "iam:CreatePolicyVersion", "iam:DeletePolicyVersion",
                "iam:ListPolicyVersions"],
     "Resource": "arn:aws:iam::*:policy/cdk-hnb659fds-*"},
    {"Sid": "CdkBootstrapStorage", "Effect": "Allow", "Action": ["s3:*", "ecr:*"],
     "Resource": ["arn:aws:s3:::cdk-hnb659fds-*", "arn:aws:ecr:*:*:repository/cdk-hnb659fds-*"]},
    {"Sid": "CdkBootstrapVersion", "Effect": "Allow",
     "Action": ["ssm:PutParameter", "ssm:GetParameter", "ssm:GetParameters",
                "ssm:DeleteParameter", "ssm:AddTagsToResource",
                "ssm:RemoveTagsFromResource"],
     "Resource": "arn:aws:ssm:*:*:parameter/cdk-bootstrap/*"},
    {"Sid": "CdkDeployThroughBootstrapRoles", "Effect": "Allow", "Action": "sts:AssumeRole",
     "Resource": "arn:aws:iam::*:role/cdk-hnb659fds-*"},
    {"Sid": "InviteOwner", "Effect": "Allow",
     "Action": ["cognito-idp:ListGroups", "cognito-idp:AdminCreateUser",
                "cognito-idp:AdminAddUserToGroup", "cognito-idp:AdminGetUser"],
     "Resource": "arn:aws:cognito-idp:*:*:userpool/*",
     # Only a build's pool (tagged agentexpress:app=ax_<id> by both IaC paths).
     "Condition": {"StringLike": {"aws:ResourceTag/agentexpress:app": "ax_*"}}},
    {"Sid": "DeleteLeftoverBuildLogs", "Effect": "Allow",
     "Action": ["logs:DeleteLogGroup"],
     "Resource": ["arn:aws:logs:*:*:log-group:*ax_*", "arn:aws:logs:*:*:log-group:*ax-*"]},
]


def _size(statements: list[dict]) -> int:
    doc = {"Version": "2012-10-17", "Statement": statements}
    return len("".join(json.dumps(doc, separators=(",", ":")).split()))


def policy_documents() -> list[dict]:
    """Every statement the role needs, packed into as few managed policies as fit."""
    docs, current = [], []
    for statement in _deploy_statements() + CDK_AND_RUNNER_STATEMENTS:
        if current and _size([*current, statement]) > _POLICY_BUDGET:
            docs.append(current)
            current = []
        current.append(statement)
    if current:
        docs.append(current)
    return [{"Version": "2012-10-17", "Statement": c} for c in docs]


def _clients():
    import builds
    return builds._table, builds._s3, builds.BUILDS_BUCKET, builds.BuildError


def template(external_id: str) -> dict:
    """The CloudFormation template a user launches in their account to connect it."""
    principals = [a for a in (DEPLOY_ROLE_ARN, BFF_ROLE_ARN) if a]
    policies = {f"DeployPolicy{i + 1}": {
        "Type": "AWS::IAM::ManagedPolicy",
        "Properties": {"Description": f"AgentExpress deploy permissions, part {i + 1}.",
                       "PolicyDocument": doc}}
        for i, doc in enumerate(policy_documents())}
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Lets an AgentExpress console deploy and destroy your builds in this "
                       "account. Delete this stack to disconnect.",
        "Resources": {
            "DeployRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": buildstore.connect_role_name(external_id),
                    "Description": "Assumed by an AgentExpress console to deploy builds here.",
                    "MaxSessionDuration": 3600,
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [{
                            "Effect": "Allow",
                            "Principal": {"AWS": principals},
                            "Action": "sts:AssumeRole",
                            "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
                        }],
                    },
                    # Read everything (plan, diff, describe), change only what a build
                    # creates. See the module docstring.
                    "ManagedPolicyArns": ["arn:aws:iam::aws:policy/ReadOnlyAccess",
                                          *({"Ref": name} for name in policies)],
                    "Tags": [{"Key": "agentexpress:purpose", "Value": "deploy-role"}],
                },
            },
            **policies,
        },
        "Outputs": {"RoleArn": {"Value": {"Fn::GetAtt": ["DeployRole", "Arn"]}}},
    }


LABEL_MAX = 64


def _label(v) -> str:
    """A name the user gives a connection ("Team sandbox"): printable, one line."""
    text = " ".join(str(v or "").split())
    if len(text) > LABEL_MAX:
        raise _clients()[3](400, f"an account name is at most {LABEL_MAX} characters")
    return "".join(ch for ch in text if ch.isprintable())


def _public(item: dict) -> dict:
    return {k: item[k] for k in ("accountId", "label", "region", "status", "roleArn",
                                 "created", "verifiedAt", "updatedAt", "stackName")
            if k in item}


def list_accounts(owner: str) -> list[dict]:
    """The user's connections, each with the builds of theirs deployed there (so the
    page can say what disconnecting would strand)."""
    import builds
    table, *_ = _clients()
    items = table.query(KeyConditionExpression=Key("pk").eq(f"USER#{owner}")
                        & Key("sk").begins_with("ACCOUNT#")).get("Items", [])
    if not items:
        return []
    placed: dict[str, list[dict]] = {}
    for b in builds.list_builds(owner):
        if b.get("account"):
            placed.setdefault(str(b["account"]), []).append(
                {"id": b.get("id"), "name": b.get("name"), "region": b.get("region", "")})
    return sorted(({**_public(i), "builds": placed.get(str(i.get("accountId")), [])}
                   for i in items), key=lambda a: (a.get("label") or "~", a["accountId"]))


def _launch(item: dict, s3, bucket: str) -> dict:
    """A fresh CloudFormation quick-create link for a pending connection."""
    key = f"connect/{item['externalId']}.json"
    url = s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key},
                                    ExpiresIn=3600)
    region = item["region"]
    q = urllib.parse.urlencode({"templateURL": url, "stackName": item["stackName"]})
    launch = (f"https://{region}.console.aws.amazon.com/cloudformation/home?region={region}"
              f"#/stacks/quickcreate?{q}")
    # `deploy` creates the stack or updates it, so the same command serves both.
    cli = (f"aws cloudformation deploy --region {region} --stack-name {item['stackName']} "
           f"--capabilities CAPABILITY_NAMED_IAM --template-file agentexpress-connect.json")
    return {**_public(item), "launchUrl": launch, "templateUrl": url, "cli": cli}


def connect(owner: str, account: str, region: str, label: str = "") -> dict:
    """Start a connection: a pending record, and the stack for the user to launch."""
    table, s3, bucket, BuildError = _clients()
    label = _label(label)
    if not buildstore.ACCOUNT_RE.match(account or ""):
        raise BuildError(400, "an AWS account ID is 12 digits")
    if not buildstore.REGION_RE.match(region or ""):
        raise BuildError(400, "invalid region")
    existing = table.get_item(Key=buildstore.account_key(owner, account)).get("Item")
    if existing and existing.get("status") == "connected":
        # Connecting again UPDATES the role: same stack, same External ID, the current
        # permissions. It stays connected meanwhile. Its name and default region are
        # changed with update(), not here.
        return {**launch_link(owner, account), "update": True}
    external_id = (existing or {}).get("externalId") or secrets.token_hex(16)
    item = {**buildstore.account_key(owner, account), "accountId": account, "region": region,
            "externalId": external_id, "status": "pending", "created": buildstore.now(),
            "roleArn": f"arn:aws:iam::{account}:role/{buildstore.connect_role_name(external_id)}",
            "stackName": f"AgentExpressConnect-{external_id[:8]}"}
    if label:
        item["label"] = label
    table.put_item(Item=item)
    s3.put_object(Bucket=bucket, Key=f"connect/{external_id}.json",
                  Body=json.dumps(template(external_id), indent=2).encode(),
                  ContentType="application/json")
    return _launch(item, s3, bucket)


def launch_link(owner: str, account: str) -> dict:
    """The stack to launch — or to UPDATE, for a connected account: the template is
    rewritten from this console's current permissions first, so updating the stack in
    the account brings its role up to date after the console is upgraded."""
    table, s3, bucket, BuildError = _clients()
    item = table.get_item(Key=buildstore.account_key(owner, account)).get("Item")
    if not item:
        raise BuildError(404, "unknown account")
    s3.put_object(Bucket=bucket, Key=f"connect/{item['externalId']}.json",
                  Body=json.dumps(template(item["externalId"]), indent=2).encode(),
                  ContentType="application/json")
    return _launch(item, s3, bucket)


def update(owner: str, account: str, label=None, region=None) -> dict:
    """Rename a connection or change its default region. A build already deployed
    there keeps the region it was deployed to (it is stored on the build), so changing
    the default only affects the next new deploy."""
    table, _s3, _b, BuildError = _clients()
    key = buildstore.account_key(owner, account)
    item = table.get_item(Key=key).get("Item")
    if not item:
        raise BuildError(404, "unknown account")
    sets, names, values = ["updatedAt = :t"], {}, {":t": buildstore.now()}
    if label is not None:
        values[":l"] = _label(label)
        sets.append("#l = :l")
        names["#l"] = "label"
    if region is not None:
        region = str(region).strip()
        if not buildstore.REGION_RE.match(region):
            raise BuildError(400, "invalid region")
        values[":r"] = region
        sets.append("#r = :r")
        names["#r"] = "region"
    if len(sets) == 1:
        raise BuildError(400, "nothing to change: give a label or a region")
    new = table.update_item(Key=key, UpdateExpression="SET " + ", ".join(sets),
                            ExpressionAttributeNames=names, ExpressionAttributeValues=values,
                            ReturnValues="ALL_NEW")["Attributes"]
    return _public(new)


def _assume(item: dict, session_name: str):
    sts = boto3.client("sts", region_name=REGION)
    creds = sts.assume_role(RoleArn=item["roleArn"], RoleSessionName=session_name,
                            ExternalId=item["externalId"], DurationSeconds=900)["Credentials"]
    return boto3.Session(aws_access_key_id=creds["AccessKeyId"],
                         aws_secret_access_key=creds["SecretAccessKey"],
                         aws_session_token=creds["SessionToken"], region_name=item["region"])


def verify(owner: str, account: str) -> dict:
    """Prove the role exists, trusts this console with this External ID, and is in the
    account the user named."""
    table, _s3, _b, BuildError = _clients()
    item = table.get_item(Key=buildstore.account_key(owner, account)).get("Item")
    if not item:
        raise BuildError(404, "unknown account")
    try:
        who = _assume(item, "agentexpress-verify").client("sts").get_caller_identity()
    except Exception as e:
        # STS says WHY (no such role, trust or External ID mismatch, an SCP): log it and
        # show it, rather than only the exception class. It names ARNs, never a secret.
        err = getattr(e, "response", {}).get("Error", {})
        why = f"{err.get('Code') or type(e).__name__}: {err.get('Message') or e}"[:400]
        print(f"accounts.verify: assume {item['roleArn']} failed: {why}")
        raise BuildError(409, "could not assume the role yet — launch the stack in that "
                              "account and wait until it shows CREATE_COMPLETE "
                              f"({why})") from e
    if who.get("Account") != account:
        raise BuildError(409, "the role is in a different account than the one named")
    table.update_item(Key=buildstore.account_key(owner, account),
                      UpdateExpression="SET #s = :c, verifiedAt = :t",
                      ExpressionAttributeNames={"#s": "status"},
                      ExpressionAttributeValues={":c": "connected", ":t": buildstore.now()})
    return _public({**item, "status": "connected", "verifiedAt": buildstore.now()})


def require_connected(owner: str, account: str) -> dict:
    table, _s3, _b, BuildError = _clients()
    item = table.get_item(Key=buildstore.account_key(owner, account)).get("Item")
    if not item or item.get("status") != "connected":
        raise BuildError(409, f"account {account} is not connected — connect and verify it first")
    return item


def disconnect(owner: str, account: str) -> None:
    """Forget a connection. Refused while any of the user's builds is deployed there."""
    import builds
    table, s3, bucket, BuildError = _clients()
    item = table.get_item(Key=buildstore.account_key(owner, account)).get("Item")
    if not item:
        raise BuildError(404, "unknown account")
    in_use = [b["name"] for b in builds.list_builds(owner) if str(b.get("account") or "") == account]
    if in_use:
        raise BuildError(409, f"destroy the builds deployed there first: {', '.join(in_use)}")
    table.delete_item(Key=buildstore.account_key(owner, account))
    s3.delete_object(Bucket=bucket, Key=f"connect/{item['externalId']}.json")
