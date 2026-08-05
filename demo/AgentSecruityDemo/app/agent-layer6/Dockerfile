# Base image from ECR Public (public.ecr.aws), the AWS-managed mirror of the official
# Python image, rather than a third-party public registry. Content-security review
# requires container base images to come from ECR Public or ECR Private so the supply
# chain is one AWS controls.
FROM --platform=linux/arm64 public.ecr.aws/docker/library/python:3.12-slim-bookworm

# uv came preinstalled in the previous third-party base image. Install it from PyPI at
# a PINNED version instead, so the build stays reproducible and the toolchain that
# resolves our dependencies is explicit rather than implied by a floating base tag.
ARG UV_VERSION=0.12.1
RUN pip install --no-cache-dir "uv==${UV_VERSION}"

WORKDIR /app

# Copy dependency manifest (no lock — resolved fresh so new AG-UI deps are picked up)
COPY pyproject.toml ./

# Install dependencies (strands-agents + AG-UI server stack)
RUN uv sync --no-cache

# Copy agent file + the vendored goal-fence package (synced into this build context by
# the build step — see _vendor_goalfence in server/app.py). The package is pure-stdlib,
# so no extra deps are needed; boto3 (for the drift embedding) is already pulled in by the
# strands/bedrock stack.
COPY agent.py ./
COPY iris_goalfence ./iris_goalfence

# AG-UI servers listen on port 8080 (/invocations + /ping)
EXPOSE 8080

# Run the AG-UI FastAPI server with OpenTelemetry instrumentation for observability
CMD ["uv", "run", "opentelemetry-instrument", "python", "agent.py"]
