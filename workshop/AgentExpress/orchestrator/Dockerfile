# ARM64 container for the orchestrator on Amazon Bedrock AgentCore Runtime.
#
# The official Python image, pulled through its ECR Public mirror rather than Docker Hub.
# Builds run in CodeBuild, whose shared IPs hit Docker Hub's anonymous pull limit: a
# deploy failed with "429 Too Many Requests" on `FROM python:...`. Same image, no limit.
#
# Pinned by digest (the multi-arch index), so a rebuild of the same source gets the same
# base. To take a newer one:
#   TOKEN=$(curl -s "https://public.ecr.aws/token/?scope=repository:docker/library/python:pull" | jq -r .token)
#   curl -sI -H "Authorization: Bearer $TOKEN" \
#     -H "Accept: application/vnd.oci.image.index.v1+json" \
#     https://public.ecr.aws/v2/docker/library/python/manifests/3.13-slim-bookworm | grep -i digest
FROM public.ecr.aws/docker/library/python:3.13-slim-bookworm@sha256:5024f48ba9441d4b13a95d3945abc6365538e3a31109833367a1923523c6efed
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DOCKER_CONTAINER=1 \
    AWS_REGION=us-east-1 \
    AWS_DEFAULT_REGION=us-east-1

COPY requirements.txt requirements.lock ./
# aws-opentelemetry-distro (ADOT) auto-instruments boto3/bedrock-runtime so model
# calls emit gen_ai spans. Its bundled LangChain instrumentation is disabled at
# runtime via OTEL_PYTHON_DISABLED_INSTRUMENTATIONS (set by Terraform) — see the
# note in terraform/main.tf.
# requirements.lock pins every transitive package as well, so two builds of the same
# source install the same code.
RUN pip install --no-cache-dir -r requirements.txt -c requirements.lock \
    && pip install --no-cache-dir aws-opentelemetry-distro==0.19.0 -c requirements.lock

EXPOSE 8080

RUN useradd -m -u 1000 bedrock_agentcore

# Ship the application package (agents, graph, runtime, config).
#
# THE PERMISSIONS HERE ARE NOT INCIDENTAL. `COPY` runs as root whatever `USER` says, and
# it PRESERVES the build context's directory modes — and those come from the umask of
# whoever ran the build. On a machine with `umask 077` (common on hardened corporate
# setups and in some CI) every directory arrives 0700 root-owned, so the unprivileged
# bedrock_agentcore user cannot traverse app/orchestrator/ to reach its own code.
#
# The image builds and pushes cleanly; the failure lands at container START as
# `ModuleNotFoundError: No module named 'app.orchestrator.runtime'`. That reads like a
# missing file, so it sends you hunting through .dockerignore for something that was
# present all along. Observed exactly that way.
#
# --chown alone would be enough today, but only because the runtime user happens to own
# the tree. The explicit chmod makes the image correct whatever the builder's umask and
# whoever runs it: `u=rwX,go=rX` gives directories 755 and files 644, because a capital X
# applies the execute bit to directories only.
COPY --chown=bedrock_agentcore:bedrock_agentcore app ./app
RUN chmod -R u=rwX,go=rX /app/app

USER bedrock_agentcore

# Dispatches on AGENT_ID: orchestrator (unset) or a single dedicated agent (set).
CMD ["opentelemetry-instrument", "python", "-m", "app.entry"]
