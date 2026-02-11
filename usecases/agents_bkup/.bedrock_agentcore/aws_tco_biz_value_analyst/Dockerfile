# Use ARM64 Python base image (required by AgentCore)
FROM --platform=linux/arm64 python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements_minimal.txt ./
RUN pip install --no-cache-dir -r requirements_minimal.txt

# Copy agent files
COPY aws_tco_bva_analyst_minimal.py ./
COPY aws_tco_bva_analyst_agentcore_minimal.py ./
COPY calculator_bva.py ./
COPY calculator_bedrock.py ./
COPY calculator_agentcore.py ./
COPY pricing_search_assistant.py ./
COPY system_prompt.py ./

# Expose port
EXPOSE 8080

# Run application
CMD ["python", "-m", "uvicorn", "aws_tco_bva_analyst_agentcore_minimal:app", "--host", "0.0.0.0", "--port", "8080"]
