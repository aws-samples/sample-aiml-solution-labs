#!/usr/bin/env bash
#
# reset-demo-state.sh — reset the Iris demo's mutable state back to a clean slate.
#
# Targets (pick one):
#   s3      Empty the exfil / payload-delivery S3 bucket (attacker collector infra).
#   memory  Delete ALL AgentCore memory — short-term events + long-term records —
#           for EVERY actor, across every deployed memory (L4/L5/L6).
#   data    Drop + reseed the Aurora demo tables (customers/orders/shipments/refunds)
#           back to the original data/*.json (via the server's own seed functions).
#   all     s3 + memory + data.
#
# This resets DEMO DATA only — it never deletes agents, runtimes, gateways, or infra.
# (For full teardown use the app's "Delete resources" screen.)
#
# Usage:
#   ./scripts/reset-demo-state.sh <s3|memory|data|all>
#   AWS_REGION=us-east-1 ./scripts/reset-demo-state.sh all
#
# Reads region from AWS_REGION (default us-east-1) and resource ids from
# CloudFormation outputs + the server's state.json — no hardcoded ids.
set -euo pipefail

TARGET="${1:-}"
case "$TARGET" in
  s3|memory|data|all) ;;
  *)
    echo "usage: $0 <s3|memory|data|all>" >&2
    exit 2 ;;
esac

# --- locate the app dir (this script lives in app/scripts/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVER_DIR="$APP_DIR/server"
export APP_DIR SERVER_DIR
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"

echo "== Iris demo reset =="
echo "   target : $TARGET"
echo "   region : $AWS_REGION"
echo "   app    : $APP_DIR"

# Fail fast if credentials are missing/expired (a common demo-time gotcha).
if ! aws sts get-caller-identity >/dev/null 2>&1; then
  echo "ERROR: AWS credentials are missing or expired. Run 'aws sts get-caller-identity' and refresh, then retry." >&2
  exit 1
fi

PY="${PYTHON:-python3}"

# ---------------------------------------------------------------------------
# S3 — empty the exfil bucket (attacker collector). Delete objects + all
# versions/delete-markers so a versioned bucket truly empties. Never deletes
# the bucket itself.
# ---------------------------------------------------------------------------
reset_s3() {
  echo
  echo "-- S3: emptying exfil bucket --"
  local bucket
  bucket="$(aws cloudformation describe-stacks --stack-name IrisDemoInfraCollector \
            --query "Stacks[0].Outputs[?OutputKey=='ExfilBucketName'].OutputValue" \
            --output text 2>/dev/null || true)"
  if [[ -z "$bucket" || "$bucket" == "None" ]]; then
    # fall back to the server's tracked state
    bucket="$($PY - <<'PYEOF' 2>/dev/null || true
import json, os
p = os.path.join(os.environ["APP_DIR"], "state.json")
try:
    d = json.load(open(p))
    print(next((r["id"] for r in d.get("resources", []) if r.get("kind") == "s3-bucket"), ""))
except Exception:
    print("")
PYEOF
)"
  fi
  if [[ -z "$bucket" || "$bucket" == "None" ]]; then
    echo "   (no exfil bucket found — collector stack not deployed; skipping)"
    return 0
  fi
  echo "   bucket: $bucket"
  # current objects
  aws s3 rm "s3://$bucket" --recursive >/dev/null 2>&1 || true
  # versions + delete markers (no-op on unversioned buckets)
  local versions
  versions="$(aws s3api list-object-versions --bucket "$bucket" \
      --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
      --output json 2>/dev/null || echo '{}')"
  echo "$versions" | $PY - "$bucket" <<'PYEOF'
import json, subprocess, sys
bucket = sys.argv[1]
raw = sys.stdin.read().strip() or "{}"
data = json.loads(raw)
items = (data.get("Objects") or []) + (data.get("DeleteMarkers") or [])
items = [i for i in items if i and i.get("Key")]
if not items:
    print("   no versioned objects to purge"); sys.exit(0)
# delete in batches of 1000 (S3 delete-objects limit)
for i in range(0, len(items), 1000):
    batch = {"Objects": [{"Key": x["Key"], "VersionId": x["VersionId"]} for x in items[i:i+1000]]}
    subprocess.run(["aws", "s3api", "delete-objects", "--bucket", bucket,
                    "--delete", json.dumps(batch)], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print(f"   purged {len(items)} versioned object(s)/marker(s)")
PYEOF
  echo "   S3 bucket emptied."
}

# ---------------------------------------------------------------------------
# MEMORY — delete every short-term event + long-term record for EVERY actor,
# across every deployed AgentCore memory (L4/L5/L6). Reuses the server's
# state.json to find memory ids; enumerates actors via list_actors.
# ---------------------------------------------------------------------------
reset_memory() {
  echo
  echo "-- Memory: wiping ALL actors (short-term + long-term) --"
  APP_DIR="$APP_DIR" SERVER_DIR="$SERVER_DIR" "$PY" - <<'PYEOF'
import json, os, sys
import boto3

region = os.environ.get("AWS_REGION", "us-east-1")
app_dir = os.environ["APP_DIR"]

# memory ids from the server's tracked state (kind == agentcore-memory)
try:
    st = json.load(open(os.path.join(app_dir, "state.json")))
    mems = [(r["id"], r.get("phase", "?")) for r in st.get("resources", [])
            if r.get("kind") == "agentcore-memory"]
except Exception as e:
    print(f"   ERROR reading state.json: {e}"); sys.exit(1)

if not mems:
    print("   (no AgentCore memory in state.json — nothing to wipe)"); sys.exit(0)

data = boto3.client("bedrock-agentcore", region_name=region)

def list_actor_ids(mem_id):
    ids, tok = [], None
    while True:
        kw = {"memoryId": mem_id, "maxResults": 100}
        if tok: kw["nextToken"] = tok
        r = data.list_actors(**kw)
        ids += [a.get("actorId") for a in r.get("actorSummaries", []) if a.get("actorId")]
        tok = r.get("nextToken")
        if not tok: break
    return ids

def wipe_actor(mem_id, actor_id):
    ev_del = rec_del = 0
    # short-term: every event in every session for this actor
    sessions, tok = [], None
    while True:
        kw = {"memoryId": mem_id, "actorId": actor_id, "maxResults": 100}
        if tok: kw["nextToken"] = tok
        r = data.list_sessions(**kw)
        sessions += [s.get("sessionId") for s in r.get("sessionSummaries", []) if s.get("sessionId")]
        tok = r.get("nextToken")
        if not tok: break
    for sid in sessions:
        tok = None
        while True:
            kw = {"memoryId": mem_id, "actorId": actor_id, "sessionId": sid,
                  "maxResults": 100, "includePayloads": False}
            if tok: kw["nextToken"] = tok
            r = data.list_events(**kw)
            for ev in r.get("events", []) or []:
                eid = ev.get("eventId")
                if not eid: continue
                try:
                    data.delete_event(memoryId=mem_id, actorId=actor_id, sessionId=sid, eventId=eid)
                    ev_del += 1
                except data.exceptions.ResourceNotFoundException:
                    pass
            tok = r.get("nextToken")
            if not tok: break
    # long-term: every record under /facts/{actorId}/ (re-list each pass; deletes shift pages)
    ns = f"/facts/{actor_id}/"
    seen = set()
    for _ in range(30):
        r = data.list_memory_records(memoryId=mem_id, namespacePath=ns, maxResults=100)
        recs = [x.get("memoryRecordId") for x in (r.get("memoryRecordSummaries") or []) if x.get("memoryRecordId")]
        fresh = [rid for rid in recs if rid not in seen]
        if not fresh: break
        for rid in fresh:
            seen.add(rid)
            try:
                data.delete_memory_record(memoryId=mem_id, memoryRecordId=rid)
                rec_del += 1
            except data.exceptions.ResourceNotFoundException:
                pass
    return ev_del, rec_del

total_ev = total_rec = 0
for mem_id, phase in mems:
    try:
        actors = list_actor_ids(mem_id)
    except Exception as e:
        print(f"   {mem_id} ({phase}): could not list actors — {e}"); continue
    ev = rec = 0
    for a in actors:
        try:
            e, r = wipe_actor(mem_id, a)
            ev += e; rec += r
        except Exception as ex:
            print(f"      actor {a}: {ex}")
    total_ev += ev; total_rec += rec
    print(f"   {mem_id} ({phase}): {len(actors)} actor(s) · {ev} event(s) · {rec} record(s) deleted")
print(f"   memory wipe complete — {total_ev} event(s), {total_rec} long-term record(s) removed.")
PYEOF
}

# ---------------------------------------------------------------------------
# DATA — drop + reseed the Aurora demo tables from data/*.json, reusing the
# server's own seed functions (single source of truth for schema + data).
# ---------------------------------------------------------------------------
reset_data() {
  echo
  echo "-- Data: dropping + reseeding Aurora demo tables --"
  APP_DIR="$APP_DIR" SERVER_DIR="$SERVER_DIR" "$PY" - <<'PYEOF'
import os, sys
sys.path.insert(0, os.environ["SERVER_DIR"])
# importing app is safe: no network at import time (only a FastAPI() + config)
import app as A

outs = A._read_cfn_outputs("IrisInfra") or A._read_cfn_outputs("IrisDemoInfra")
ca, sa = outs.get("ClusterArn"), outs.get("SecretArn")
db = outs.get("DatabaseName", "irisdb")
if not ca or not sa:
    print("   ERROR: Aurora not deployed (no ClusterArn/SecretArn in IrisInfra outputs)."); sys.exit(1)
n  = A._seed_customers(cluster_arn=ca, secret_arn=sa, database=db)
ns = A._seed_shipments(cluster_arn=ca, secret_arn=sa, database=db)
no = A._seed_orders(cluster_arn=ca, secret_arn=sa, database=db)
A._seed_refunds(cluster_arn=ca, secret_arn=sa, database=db)
print(f"   reseeded: {n} customers · {ns} shipments · {no} orders · refunds reset")
PYEOF
}

case "$TARGET" in
  s3)     reset_s3 ;;
  memory) reset_memory ;;
  data)   reset_data ;;
  all)    reset_s3; reset_memory; reset_data ;;
esac

echo
echo "== done: $TARGET reset =="
