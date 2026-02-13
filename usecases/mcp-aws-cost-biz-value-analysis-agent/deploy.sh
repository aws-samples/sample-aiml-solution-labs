#!/bin/bash
set -e

# ═══════════════════════════════════════════════════════════
# AWS TCO & Business Value Analysis - Full Deployment Script
# ═══════════════════════════════════════════════════════════
# Usage: ./deploy.sh [--skip-cfn] [--skip-agent] [--skip-kb-policy] [--skip-frontend]

REGION="${AWS_REGION:-us-west-2}"
STACK_NAME="aws-tco-biz-value-analysis"
AGENT_DIR="agents"
FRONTEND_DIR="chatbot-ui"

SKIP_CFN=false
SKIP_AGENT=false
SKIP_KB_POLICY=false
SKIP_FRONTEND=false

for arg in "$@"; do
  case $arg in
    --skip-cfn) SKIP_CFN=true ;;
    --skip-agent) SKIP_AGENT=true ;;
    --skip-kb-policy) SKIP_KB_POLICY=true ;;
    --skip-frontend) SKIP_FRONTEND=true ;;
  esac
done

echo "═══════════════════════════════════════════════════════════"
echo "  AWS TCO & BVA - Full Stack Deployment"
echo "  Region: $REGION"
echo "═══════════════════════════════════════════════════════════"

# ─── Step 1: Deploy CloudFormation ───
if [ "$SKIP_CFN" = false ]; then
  echo ""
  echo ">>> Step 1: Deploying CloudFormation stack..."
  aws cloudformation deploy \
    --template-file cfn/aws-tco-biz-value-analysis.yaml \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION" \
    --no-fail-on-empty-changeset

  echo "✓ CloudFormation stack deployed"
else
  echo ">>> Step 1: Skipping CloudFormation (--skip-cfn)"
fi

# Get stack outputs
echo ""
echo ">>> Fetching stack outputs..."
OUTPUTS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" --query "Stacks[0].Outputs" --output json)

get_output() {
  echo "$OUTPUTS" | python3 -c "import sys,json; items=json.load(sys.stdin); print(next((i['OutputValue'] for i in items if i['OutputKey']=='$1'),''))"
}

KB_ID=$(get_output "KnowledgeBaseId")
FRONTEND_BUCKET=$(get_output "FrontendBucketName")
CF_DOMAIN=$(get_output "CloudFrontDomainName")
CF_DIST_ID=$(get_output "CloudFrontDistributionId")
IDENTITY_POOL_ID=$(get_output "IdentityPoolId")

echo "  Knowledge Base ID: $KB_ID"
echo "  Frontend Bucket: $FRONTEND_BUCKET"
echo "  CloudFront Domain: $CF_DOMAIN"
echo "  CloudFront Dist ID: $CF_DIST_ID"
echo "  Identity Pool ID: $IDENTITY_POOL_ID"

# ─── Step 2: Deploy AgentCore Runtime ───
if [ "$SKIP_AGENT" = false ]; then
  echo ""
  echo ">>> Step 2: Deploying AgentCore runtime..."
  cd "$AGENT_DIR"

  export STRANDS_KNOWLEDGE_BASE_ID="$KB_ID"
  export AWS_REGION="$REGION"

  python3 deployment_helper.py --region "$REGION"

  cd ..
  echo "✓ AgentCore runtime deployed"
else
  echo ">>> Step 2: Skipping AgentCore deployment (--skip-agent)"
fi

# ─── Step 3: Add KB policy to AgentCore role ───
if [ "$SKIP_KB_POLICY" = false ]; then
  echo ""
  echo ">>> Step 3: Adding KB policy to AgentCore execution role..."

  # Discover the AgentCore runtime ID (with retry — runtime may take time to reach READY)
  echo "  Waiting for AgentCore runtime to be READY..."
  AGENT_RUNTIME_ID=""
  for i in $(seq 1 30); do
    AGENT_RUNTIME_ID=$(aws bedrock-agentcore-control list-agent-runtimes --region "$REGION" \
      --query "agentRuntimes[?agentRuntimeName=='aws_tco_biz_value_analyst' && status=='READY'].agentRuntimeId | [0]" \
      --output text 2>/dev/null || echo "")
    if [ -n "$AGENT_RUNTIME_ID" ] && [ "$AGENT_RUNTIME_ID" != "None" ]; then
      echo "  Runtime ID: $AGENT_RUNTIME_ID"
      break
    fi
    echo "  Attempt $i/30 — runtime not READY yet, waiting 30s..."
    sleep 30
  done

  if [ -n "$AGENT_RUNTIME_ID" ] && [ "$AGENT_RUNTIME_ID" != "None" ]; then
    # Get the execution role from the runtime (field is "roleArn" per API docs)
    EXEC_ROLE_ARN=$(aws bedrock-agentcore-control get-agent-runtime --agent-runtime-id "$AGENT_RUNTIME_ID" --region "$REGION" \
      --query "roleArn" --output text 2>/dev/null || echo "")

    if [ -n "$EXEC_ROLE_ARN" ] && [ "$EXEC_ROLE_ARN" != "None" ]; then
      cd "$AGENT_DIR"
      python3 add_kb_policy_to_role.py --role-arn "$EXEC_ROLE_ARN" --kb-id "$KB_ID" --region "$REGION"
      cd ..
      echo "✓ KB policy added to role"
    else
      echo "⚠ Could not determine execution role ARN. Run manually:"
      echo "  python3 agents/add_kb_policy_to_role.py --role-arn <ROLE_ARN> --kb-id $KB_ID --region $REGION"
    fi
  else
    echo "⚠ AgentCore runtime not found. Deploy agent first, then run:"
    echo "  python3 agents/add_kb_policy_to_role.py --role-arn <ROLE_ARN> --kb-id $KB_ID --region $REGION"
  fi
else
  echo ">>> Step 3: Skipping KB policy (--skip-kb-policy)"
fi

# ─── Step 4: Build and deploy frontend to CloudFront ───
if [ "$SKIP_FRONTEND" = false ]; then
  echo ""
  echo ">>> Step 4: Building and deploying frontend..."

  cd "$FRONTEND_DIR"

  # Install dependencies
  echo "  Installing npm dependencies..."
  npm install

  # Update authConfig with CloudFront URL for redirects
  CF_URL="https://$CF_DOMAIN"
  cat > src/authConfig.js << AUTHEOF
export const cognitoConfig = {
  userPoolId: "${COGNITO_USER_POOL_ID:-us-west-2_6AuG0cghr}",
  userPoolClientId: "${COGNITO_CLIENT_ID:-7sueti9imhqmlrubqvj97lqvse}",
  region: "$REGION",
  domain: "${COGNITO_DOMAIN:-us-west-26aug0cghr}",
  redirectSignIn: "$CF_URL/",
  redirectSignOut: "$CF_URL/",
  responseType: "code",
  scopes: ["email", "openid", "profile", "aws.cognito.signin.user.admin"],
};

export const getHostedUIUrl = () => {
  const { domain, userPoolClientId, redirectSignIn, scopes, responseType, region } = cognitoConfig;
  const scopeString = scopes.join(" ");
  return (
    \`https://\${domain}.auth.\${region}.amazoncognito.com/oauth2/authorize\` +
    \`?identity_provider=FederateOIDC\` +
    \`&client_id=\${userPoolClientId}\` +
    \`&response_type=\${responseType}\` +
    \`&scope=\${encodeURIComponent(scopeString)}\` +
    \`&redirect_uri=\${encodeURIComponent(redirectSignIn)}\`
  );
};

export const getLogoutUrl = () => {
  const { domain, userPoolClientId, redirectSignOut, region } = cognitoConfig;
  return (
    \`https://\${domain}.auth.\${region}.amazoncognito.com/logout\` +
    \`?client_id=\${userPoolClientId}\` +
    \`&logout_uri=\${encodeURIComponent(redirectSignOut)}\`
  );
};
AUTHEOF

  # Build React app
  REACT_APP_IDENTITY_POOL_ID="$IDENTITY_POOL_ID" npm run build

  # Upload to S3
  aws s3 sync build/ "s3://$FRONTEND_BUCKET/" --delete --region "$REGION"

  # Invalidate CloudFront cache
  aws cloudfront create-invalidation \
    --distribution-id "$CF_DIST_ID" \
    --paths "/*" \
    --region us-east-1

  cd ..
  echo "✓ Frontend deployed to CloudFront"
  echo ""
  echo "═══════════════════════════════════════════════════════════"
  echo "  DEPLOYMENT COMPLETE"
  echo "═══════════════════════════════════════════════════════════"
  echo ""
  echo "  Application URL: https://$CF_DOMAIN"
  echo ""
  echo "  IMPORTANT: Add the CloudFront URL to your Cognito app"
  echo "  client's allowed callback and sign-out URLs:"
  echo "    https://$CF_DOMAIN/"
  echo ""
else
  echo ">>> Step 4: Skipping frontend deployment (--skip-frontend)"
fi
