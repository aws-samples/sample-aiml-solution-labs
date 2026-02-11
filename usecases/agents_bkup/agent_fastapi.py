"""
FastAPI application for AgentCore deployment.
"""

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
import json
from aws_tco_bva_analyst_minimal import AwsTcoBvaAnalystMinimal

app = FastAPI(title="AWS TCO & BVA Analyst", version="1.0.0")

# Initialize analyst (lazy)
analyst = None


def get_analyst():
    """Get or create analyst instance."""
    global analyst
    if analyst is None:
        analyst = AwsTcoBvaAnalystMinimal()
    return analyst


@app.post("/invocations")
async def invoke_agent(request: Request):
    """Handle agent invocations - accepts raw JSON payload from AgentCore."""
    try:
        # Read raw body
        body = await request.body()
        payload = json.loads(body)
        
        # Extract prompt from payload
        user_message = payload.get("prompt", "")
        if not user_message:
            raise HTTPException(
                status_code=400,
                detail="No prompt found in payload"
            )

        # Get response from analyst
        result = get_analyst().analyze(user_message)
        
        # Return response in AgentCore format
        return {
            "output": {
                "message": result,
                "timestamp": datetime.utcnow().isoformat()
            }
        }

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent processing failed: {str(e)}")


@app.get("/ping")
async def ping():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
