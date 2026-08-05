#!/usr/bin/env bash
# Start the Iris Security Demo locally.
# Prereqs: AWS creds in env (sandbox account), Node + Python 3.10+, cdk deps installed.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "Region: ${AWS_REGION:-us-east-1}"
aws sts get-caller-identity --query Account --output text 2>/dev/null \
  && echo "creds OK" || echo "WARNING: no AWS creds detected — deploy/destroy will no-op"

# the CDK CLI itself is Node (cdk stacks are Python) — ensure it's available
if ! command -v cdk >/dev/null 2>&1 && ! npx --no-install cdk --version >/dev/null 2>&1; then
  echo "installing aws-cdk CLI (npm, one-time)…"; npm install -g aws-cdk || echo "  (install cdk manually: npm i -g aws-cdk)"
fi

# python deps: server + CDK library
python3 -m pip install -q -r "$HERE/server/requirements.txt"
python3 -m pip install -q -r "$HERE/cdk/requirements.txt"

echo "open http://localhost:8000"
echo "detailed logs → $HERE/logs/server.log (wiped each startup)"
# NOTE: --reload is off so the startup log-truncation happens once per real start
( cd "$HERE/server" && python3 -m uvicorn app:app --port 8000 )
