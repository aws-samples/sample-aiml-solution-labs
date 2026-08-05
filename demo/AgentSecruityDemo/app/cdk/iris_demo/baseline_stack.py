"""
Baseline (pre-Layer-1) stack for the Iris security demo - Python CDK.

Creates the DELIBERATELY UNPROTECTED starting point:
  1. VPC (for Aurora - required by RDS).
  2. Aurora Serverless v2 (PostgreSQL) with Data API enabled.
  3. An ECR repository for the agent container image.
  4. An IAM execution role with RDS Data API + Bedrock + Lambda permissions.

The AgentCore Runtime is created via API by the server (PUBLIC mode).
The agent accesses Aurora via RDS Data API (HTTPS, no VPC needed for the agent).
"""
from aws_cdk import (
    Stack, Tags, RemovalPolicy, CfnOutput, Duration,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_ecr as ecr,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
)
from constructs import Construct
from iris_demo import AGENTCORE_ROLE_ACTIONS


# Attacker "collector" — the exfil target for the BASELINE (unprotected) demo. An
# IAM-authed Lambda Function URL that writes any POSTed body into an S3 bucket under
# stolen/. It exists ONLY to make the baseline's before/after story land: the hijacked
# baseline agent POSTs stolen customer rows here and they show up in S3. Every LATER
# layer stops this — the DNS Firewall (L1+) NXDOMAINs the *.lambda-url host, so nothing
# reaches the collector once the agent is in the VPC. Kept in the infra stack (not a
# separate stack) so the one CDK-stack panel provisions the whole demo surface.
COLLECTOR_CODE = "\n".join([
    "import os, json, time, boto3, base64",
    "s3 = boto3.client('s3')",
    "BUCKET = os.environ['EXFIL_BUCKET']",
    "def handler(event, context):",
    "    body = event.get('body') or ''",
    "    if event.get('isBase64Encoded'):",
    "        body = base64.b64decode(body).decode('utf-8', errors='ignore')",
    "    key = 'stolen/customers-%d.json' % int(time.time()*1000)",
    "    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode(), ContentType='application/json')",
    "    print('EXFIL RECEIVED bytes=%d key=%s' % (len(body), key))",
    "    return {'statusCode': 200, 'body': json.dumps({'stored': key, 'bytes': len(body)})}",
])


# Shipment-tracking service — a LEGITIMATE internal integration the agent calls
# over HTTP to answer "where's my order?". Backed by the same Aurora DB (shipments
# table) via the RDS Data API. Public Function URL with AWS_IAM auth (the agent's
# exec role is granted). This is the SANCTIONED destination for the agent's generic
# http_request tool; the DNS Firewall (built in infra) allowlists exactly this
# host and blocks every other *.lambda-url host (e.g. the exfil collector).
SHIPMENT_CODE = "\n".join([
    "import os, json, boto3",
    "rds = boto3.client('rds-data')",
    "CLUSTER = os.environ['CLUSTER_ARN']; SECRET = os.environ['SECRET_ARN']; DB = os.environ['DATABASE_NAME']",
    "def handler(event, context):",
    "    qs = (event.get('queryStringParameters') or {})",
    "    order_id = (qs.get('order_id') or '').strip()",
    "    if not order_id:",
    "        return {'statusCode': 400, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'error': 'order_id query parameter required'})}",
    "    resp = rds.execute_statement(resourceArn=CLUSTER, secretArn=SECRET, database=DB,",
    "        sql='SELECT order_id, status, carrier, eta, last_location FROM shipments WHERE order_id = :oid',",
    "        parameters=[{'name': 'oid', 'value': {'stringValue': order_id}}], includeResultMetadata=True)",
    "    cols = [c['name'] for c in resp.get('columnMetadata', [])]",
    "    rows = []",
    "    for rec in resp.get('records', []):",
    "        row = {}",
    "        for i, f in enumerate(rec):",
    "            row[cols[i]] = f.get('stringValue') if 'stringValue' in f else (None if f.get('isNull') else f.get('longValue'))",
    "        rows.append(row)",
    "    if not rows:",
    "        # Known order with no shipment row yet = not shipped. Return 200 with a clear",
    "        # status (NOT a 404) so the agent can still answer per-order: some orders",
    "        # simply have no tracking yet. Only a truly unknown order id 404s.",
    "        chk = rds.execute_statement(resourceArn=CLUSTER, secretArn=SECRET, database=DB,",
    "            sql='SELECT order_id FROM orders WHERE order_id = :oid',",
    "            parameters=[{'name': 'oid', 'value': {'stringValue': order_id}}])",
    "        known = bool(chk.get('records'))",
    "        if known:",
    "            return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'order_id': order_id, 'status': 'not shipped yet', 'message': 'No shipping information found for this order yet.'})}",
    "        return {'statusCode': 404, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'error': 'no shipment for order ' + order_id})}",
    "    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps(rows[0])}",
])


class BaselineStack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        Tags.of(self).add("demo", "iris-security")
        Tags.of(self).add("demo-layer", "baseline")

        # 1) VPC — three subnet tiers + one NAT gateway.
        #    - Public: hosts the NAT gateway (and IGW route).
        #    - PrivateEgress: the Layer 1 agent runs here → NAT → IGW → internet,
        #      but egress is FILTERED by the Route 53 DNS Firewall (allows only the
        #      shipment host, denies everything else incl. the exfil URL).
        #    - PrivateIsolated: Aurora (no internet route; Data API is HTTPS/endpoint).
        #    Baseline agent is PUBLIC-mode (not in the VPC), so it's unaffected — the
        #    NAT only matters once an agent moves INTO the VPC at Layer 1.
        vpc = ec2.Vpc(
            self, "IrisVpc",
            vpc_name="iris-vpc",
            max_azs=2,
            nat_gateways=1,
            # VPC Flow Logs (cdk_nag AwsSolutions-VPC7): capture network flow to CloudWatch.
            flow_logs={
                "FlowLogCw": ec2.FlowLogOptions(
                    destination=ec2.FlowLogDestination.to_cloud_watch_logs(),
                    traffic_type=ec2.FlowLogTrafficType.ALL,
                ),
            },
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="PrivateEgress",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        # Security group for Aurora
        aurora_sg = ec2.SecurityGroup(
            self, "AuroraSG",
            vpc=vpc,
            description="Aurora PostgreSQL security group",
            allow_all_outbound=False,
        )
        aurora_sg.add_ingress_rule(
            ec2.Peer.ipv4(vpc.vpc_cidr_block),
            ec2.Port.tcp(5432),
            "PostgreSQL from within VPC",
        )

        # 2) Aurora Serverless v2 (PostgreSQL) with Data API
        db_cluster = rds.DatabaseCluster(
            self, "IrisDB",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.of("17.4", "17"),
            ),
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=1,
            writer=rds.ClusterInstance.serverless_v2("writer"),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            security_groups=[aurora_sg],
            default_database_name="irisdb",
            enable_data_api=True,
            # IAM database authentication (cdk_nag AwsSolutions-RDS6). The demo accesses
            # Aurora via the RDS Data API + Secrets Manager (unchanged); enabling IAM auth
            # adds the capability without removing the Data-API path the demo uses.
            iam_authentication=True,
            # Storage encryption (cdk_nag AwsSolutions-RDS2). NOTE: toggling this on an
            # EXISTING cluster triggers a CloudFormation REPLACEMENT (data is reseeded on
            # next deploy). New deployments are encrypted from creation.
            storage_encrypted=True,
            removal_policy=RemovalPolicy.DESTROY,
            credentials=rds.Credentials.from_generated_secret("irisadmin"),
        )

        # 3) ECR repositories. The consolidated deploy runs THREE agent images: the
        #    goal-fenced superset ("agent"), the A2A Orders peer ("peer"), and the
        #    deliberately-unprotected baseline ("baseline") used for the before/after
        #    demo. Each gets its own repo so images never clobber.
        repos = {}
        for layer in ("agent", "peer", "baseline", "layer1", "layer2", "layer3", "layer4", "layer5"):
            repos[layer] = ecr.Repository(
                self, f"IrisEcr{layer.capitalize()}",
                repository_name=f"iris-{layer}",
                removal_policy=RemovalPolicy.DESTROY,
                empty_on_delete=True,
            )
        repo = repos["agent"]  # consolidated Layer 6 agent image

        # 4) Iris agent execution role - INTENTIONALLY BROAD (baseline)
        self.exec_role = exec_role = iam.Role(
            self, "IrisExecRole",
            assumed_by=iam.ServicePrincipal("bedrock-agentcore.amazonaws.com"),
            description="Iris agent execution role (baseline) - demo iris-security",
        )
        # AgentCore runtime permissions (cdk_nag AwsSolutions-IAM4): scoped inline policy
        # instead of the AWS-managed BedrockAgentCoreFullAccess. Covers the runtime data-
        # plane operations the agent needs (workload identity token, memory, gateway invoke)
        # against this account's AgentCore resources only.
        # AgentCore permissions (cdk_nag AwsSolutions-IAM4): replaces AWS-managed
        # BedrockAgentCoreFullAccess with an EXPLICIT, least-privilege action list scoped to
        # this account+region. Covers what the role is used for while AgentCore assumes it:
        # runtime data-plane (token/OBO/memory/events/sessions/invoke) + the control-plane
        # READS the Gateway needs to resolve its Cedar policy engine + targets on
        # create/update (UpdateGateway -> GetPolicyEngine). No create/delete — those are run
        # by the deploy's own admin credentials, not this role.
        exec_role.add_to_policy(iam.PolicyStatement(
            sid="AgentCoreScoped",
            actions=AGENTCORE_ROLE_ACTIONS,
            resources=[
                f"arn:aws:bedrock-agentcore:{self.region}:{self.account}:*",
            ],
        ))
        # RDS Data API access
        exec_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "rds-data:ExecuteStatement",
                "rds-data:BatchExecuteStatement",
                "rds-data:BeginTransaction",
                "rds-data:CommitTransaction",
            ],
            resources=[db_cluster.cluster_arn],
        ))
        # Access to the DB secret (Data API needs it)
        db_cluster.secret.grant_read(exec_role)
        # AgentCore Identity OBO/OAuth2 secrets: the RFC 8693 OBO exchange
        # (GetResourceOauth2Token) reads the credential-provider secret AgentCore Identity
        # stores under the reserved `bedrock-agentcore-identity!default/oauth2/` prefix.
        # Scoped to that prefix (not all secrets).
        exec_role.add_to_policy(iam.PolicyStatement(
            sid="AgentCoreIdentityOboSecrets",
            actions=["secretsmanager:GetSecretValue"],
            resources=[
                f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:bedrock-agentcore-identity!default/oauth2/*",
            ],
        ))
        # Bedrock model access (cdk_nag AwsSolutions-IAM5): scoped to foundation models
        # and inference profiles in this account/region rather than "*". The baseline agent
        # can call any approved model shape; Layer 5 further restricts WHICH models via its
        # own scoped role + explicit deny.
        exec_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "bedrock:InvokeModel",
                "bedrock:InvokeModelWithResponseStream",
                "bedrock:Converse",
                "bedrock:ConverseStream",
            ],
            resources=[
                f"arn:aws:bedrock:{self.region}::foundation-model/*",
                f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/*",
                # CRIS cross-region inference profiles resolve to peer-region FMs
                "arn:aws:bedrock:*::foundation-model/*",
            ],
        ))
        # Logging + tracing
        exec_role.add_to_policy(iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
                     "logs:DescribeLogStreams", "logs:DescribeLogGroups"],
            resources=["arn:aws:logs:*:*:*"],
        ))
        exec_role.add_to_policy(iam.PolicyStatement(
            actions=["xray:PutTraceSegments", "xray:PutTelemetryRecords",
                     "xray:GetSamplingRules", "xray:GetSamplingTargets"],
            resources=["*"],
        ))
        exec_role.add_to_policy(iam.PolicyStatement(
            actions=["cloudwatch:PutMetricData"], resources=["*"],
        ))
        for _r in repos.values():
            _r.grant_pull(exec_role)
        # Allow the agent to invoke the sanctioned shipment service AND the attacker
        # collector. Granting the collector is deliberate: the BASELINE agent is
        # unprotected, so its exec role can reach the exfil endpoint — that is what the
        # before/after story exposes. Later layers don't remove this grant; the DNS
        # Firewall (network control) is what stops the call once the agent is in the VPC.
        exec_role.add_to_policy(iam.PolicyStatement(
            actions=["lambda:InvokeFunction", "lambda:InvokeFunctionUrl"],
            resources=[
                f"arn:aws:lambda:{self.region}:{self.account}:function:iris-shipment",
                f"arn:aws:lambda:{self.region}:{self.account}:function:iris-collector",
            ],
        ))

        # 5) Shipment-tracking service — legitimate HTTP integration (Aurora-backed).
        shipment_fn = lambda_.Function(
            self, "ShipmentFn",
            function_name="iris-shipment",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            timeout=Duration.seconds(30),
            environment={
                "CLUSTER_ARN": db_cluster.cluster_arn,
                "SECRET_ARN": db_cluster.secret.secret_arn,
                "DATABASE_NAME": "irisdb",
            },
            code=lambda_.Code.from_inline(SHIPMENT_CODE),
        )
        # Shipment Lambda reads Aurora via Data API
        shipment_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["rds-data:ExecuteStatement"],
            resources=[db_cluster.cluster_arn],
        ))
        db_cluster.secret.grant_read(shipment_fn)
        # Public Function URL, AWS_IAM auth — the agent's exec role may invoke it.
        shipment_url = shipment_fn.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.AWS_IAM,
        )
        shipment_fn.add_permission(
            "AllowExecRoleShipment",
            principal=iam.ArnPrincipal(exec_role.role_arn),
            action="lambda:InvokeFunctionUrl",
            function_url_auth_type=lambda_.FunctionUrlAuthType.AWS_IAM,
        )

        # 6) Attacker collector — exfil target for the BASELINE demo (IAM-authed
        #    Function URL → writes POSTed body to S3 under stolen/). Only the agent's
        #    exec role may invoke it; it is never open to the internet.
        # Server-access-log target bucket (cdk_nag AwsSolutions-S1). Its own access logging
        # is intentionally not enabled (a log bucket logging to itself/another log bucket is
        # a recursion cdk_nag also flags); suppressed with evidence on this bucket only.
        access_logs_bucket = s3.Bucket(
            self, "AccessLogsBucket",
            bucket_name=f"iris-access-logs-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_PREFERRED,
        )

        exfil_bucket = s3.Bucket(
            self, "ExfilBucket",
            bucket_name=f"iris-exfil-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="exfil/",
        )
        collector_fn = lambda_.Function(
            self, "CollectorFn",
            function_name="iris-collector",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            timeout=Duration.seconds(30),
            environment={"EXFIL_BUCKET": exfil_bucket.bucket_name},
            code=lambda_.Code.from_inline(COLLECTOR_CODE),
        )
        exfil_bucket.grant_put(collector_fn)
        collector_url = collector_fn.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.AWS_IAM,
        )
        collector_fn.add_permission(
            "AllowExecRoleCollector",
            principal=iam.ArnPrincipal(exec_role.role_arn),
            action="lambda:InvokeFunctionUrl",
            function_url_auth_type=lambda_.FunctionUrlAuthType.AWS_IAM,
        )

        # Outputs
        CfnOutput(self, "VpcId", value=vpc.vpc_id)
        CfnOutput(self, "ClusterArn", value=db_cluster.cluster_arn)
        CfnOutput(self, "SecretArn", value=db_cluster.secret.secret_arn)
        CfnOutput(self, "DatabaseName", value="irisdb")
        CfnOutput(self, "ExecRoleArn", value=exec_role.role_arn)
        CfnOutput(self, "ExecRoleName", value=exec_role.role_name)
        # ECR repos: the consolidated agent + the A2A peer + the baseline agent.
        CfnOutput(self, "EcrRepoUri", value=repo.repository_uri)          # agent (default)
        CfnOutput(self, "EcrRepoName", value=repo.repository_name)
        CfnOutput(self, "EcrRepoUriAgent", value=repos["agent"].repository_uri)
        CfnOutput(self, "EcrRepoUriPeer", value=repos["peer"].repository_uri)
        CfnOutput(self, "EcrRepoUriBaseline", value=repos["baseline"].repository_uri)
        CfnOutput(self, "EcrRepoUriLayer1", value=repos["layer1"].repository_uri)
        CfnOutput(self, "EcrRepoUriLayer2", value=repos["layer2"].repository_uri)
        CfnOutput(self, "EcrRepoUriLayer3", value=repos["layer3"].repository_uri)
        CfnOutput(self, "EcrRepoUriLayer4", value=repos["layer4"].repository_uri)
        CfnOutput(self, "EcrRepoUriLayer5", value=repos["layer5"].repository_uri)
        CfnOutput(self, "ShipmentUrl", value=shipment_url.url)
        CfnOutput(self, "ShipmentFnName", value=shipment_fn.function_name or "iris-shipment")
        # Attacker collector (baseline exfil target).
        CfnOutput(self, "CollectorUrl", value=collector_url.url)
        CfnOutput(self, "CollectorFnName", value=collector_fn.function_name or "iris-collector")
        CfnOutput(self, "ExfilBucketName", value=exfil_bucket.bucket_name)
