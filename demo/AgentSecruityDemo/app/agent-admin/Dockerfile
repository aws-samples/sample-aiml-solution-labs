# Base image from ECR Public (public.ecr.aws), the AWS-managed mirror of the official
# Python image, rather than a third-party public registry. Content-security review
# requires container base images to come from ECR Public or ECR Private so the supply
# chain is one AWS controls.
FROM --platform=linux/arm64 public.ecr.aws/docker/library/python:3.11-slim-bookworm

# uv came preinstalled in the previous third-party base image. Install it from PyPI at
# a PINNED version instead, so the build stays reproducible and the toolchain that
# resolves our dependencies is explicit rather than implied by a floating base tag.
ARG UV_VERSION=0.12.1
RUN pip install --no-cache-dir "uv==${UV_VERSION}"

WORKDIR /app

# Copy uv files
COPY pyproject.toml uv.lock ./

# Install dependencies (including strands-agents)
RUN uv sync --frozen --no-cache

# Copy agent file
COPY agent.py ./

# Expose port
EXPOSE 8080

# Run application with OpenTelemetry instrumentation for AgentCore observability
CMD ["uv", "run", "opentelemetry-instrument", "python", "agent.py"]