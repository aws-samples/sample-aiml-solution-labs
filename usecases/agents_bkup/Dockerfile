# Use ARM64 Python base image (required by AgentCore)
FROM --platform=linux/arm64 python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements_docker.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy agent files
COPY agent_entrypoint.py ./
COPY aws_tco_bva_analyst_minimal.py ./
COPY calculator_bva.py ./
COPY calculator_bedrock.py ./
COPY calculator_agentcore.py ./
COPY pricing_search_assistant.py ./
COPY system_prompt.py ./

# Expose port
EXPOSE 8080

# Run application using uvicorn to serve the Starlette app
CMD ["uvicorn", "agent_entrypoint:app", "--host", "0.0.0.0", "--port", "8080"]
