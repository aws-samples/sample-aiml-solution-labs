const express = require("express");
const cors = require("cors");
const {
  BedrockAgentCoreClient,
  InvokeAgentRuntimeCommand,
} = require("@aws-sdk/client-bedrock-agentcore");
const {
  BedrockAgentCoreControlClient,
  ListAgentRuntimesCommand,
} = require("@aws-sdk/client-bedrock-agentcore-control");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, PutCommand, QueryCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");
const { fromNodeProviderChain } = require("@aws-sdk/credential-providers");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "5mb" }));

const REGION = process.env.AWS_REGION || "us-west-2";
const AGENT_NAME = "aws_tco_biz_value_analyst";
const TABLE_NAME = "tco-bva-chat-sessions";

const ddbClient = DynamoDBDocumentClient.from(
  new DynamoDBClient({ region: REGION, credentials: fromNodeProviderChain() })
);

let cachedArn = process.env.AGENTCORE_RUNTIME_ARN || null;

async function getRuntimeArn() {
  if (cachedArn) return cachedArn;
  const controlClient = new BedrockAgentCoreControlClient({
    region: REGION, credentials: fromNodeProviderChain(),
  });
  const resp = await controlClient.send(new ListAgentRuntimesCommand({}));
  const runtimes = resp.agentRuntimes || [];
  const match = runtimes.find(
    (rt) => rt.agentRuntimeName === AGENT_NAME && rt.status === "READY"
  );
  if (!match) throw new Error(`No READY runtime found for agent "${AGENT_NAME}" in ${REGION}`);
  cachedArn = match.agentRuntimeArn;
  console.log(`Discovered runtime ARN: ${cachedArn}`);
  return cachedArn;
}

const CHART_KEYWORDS = /\b(bar\s*graph|bar\s*chart|pie\s*chart|line\s*chart|area\s*chart|stacked\s*bar|grouped\s*bar|radar|spider|donut|doughnut|heat\s*map|heatmap|chart|graph|visuali[sz]e|plot)\b/i;

function rewritePromptForCharts(prompt) {
  if (!CHART_KEYWORDS.test(prompt)) return prompt;
  return (
    prompt +
    "\n\nIMPORTANT: The client application can render charts. " +
    "Please return the data as a JSON array of objects with numeric values suitable for charting. " +
    "Include a 'chart_type' field (bar, line, pie, donut, area, stacked_bar, grouped_bar, radar, or heatmap) and a 'chart_data' array. " +
    "Each item in chart_data should have a 'name' field (label) and one or more numeric fields for values. " +
    "Also include a 'chart_title' field. Do NOT say you cannot create charts — just provide the data."
  );
}

function getESTTimestamp() {
  const d = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false,
  }).formatToParts(d);
  const get = (type) => parts.find((p) => p.type === type).value;
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")}:${get("second")}`;
}

// ─── Save a message to DynamoDB ───
async function saveMessage(userId, sessionId, sessionTitle, role, content) {
  const now = getESTTimestamp();
  const msg = { role, content, timestamp: now };

  // Try to update existing session by appending message
  try {
    await ddbClient.send(new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { userId, sessionId },
      UpdateExpression: "SET messages = list_append(messages, :msg), updatedAt = :now",
      ConditionExpression: "attribute_exists(userId)",
      ExpressionAttributeValues: { ":msg": [msg], ":now": now },
    }));
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") {
      // First message in session — create the item
      await ddbClient.send(new PutCommand({
        TableName: TABLE_NAME,
        Item: {
          userId,
          sessionId,
          title: sessionTitle || content.slice(0, 80),
          messages: [msg],
          createdAt: now,
          updatedAt: now,
        },
      }));
    } else {
      throw err;
    }
  }
}

// ─── List sessions for a user ───
app.get("/api/sessions", async (req, res) => {
  const userId = req.query.userId;
  if (!userId) return res.status(400).json({ error: "userId required" });

  try {
    const result = await ddbClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      KeyConditionExpression: "userId = :uid",
      ExpressionAttributeValues: { ":uid": userId },
      ProjectionExpression: "sessionId, title, createdAt, updatedAt",
      ScanIndexForward: false,
    }));
    res.json({ sessions: result.Items || [] });
  } catch (err) {
    console.error("List sessions error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Get messages for a session ───
app.get("/api/sessions/:sessionId", async (req, res) => {
  const userId = req.query.userId;
  if (!userId) return res.status(400).json({ error: "userId required" });

  try {
    const result = await ddbClient.send(new QueryCommand({
      TableName: TABLE_NAME,
      KeyConditionExpression: "userId = :uid AND sessionId = :sid",
      ExpressionAttributeValues: { ":uid": userId, ":sid": req.params.sessionId },
    }));
    const item = (result.Items || [])[0];
    res.json({ messages: item ? item.messages : [], title: item?.title });
  } catch (err) {
    console.error("Get session error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Chat endpoint (with DynamoDB persistence) ───
app.post("/api/chat", async (req, res) => {
  const { prompt, sessionId, userId, sessionTitle } = req.body;
  if (!prompt) return res.status(400).json({ error: "prompt is required" });

  try {
    // Save user message
    if (userId && sessionId) {
      await saveMessage(userId, sessionId, sessionTitle, "user", prompt);
    }

    const runtimeArn = await getRuntimeArn();
    const client = new BedrockAgentCoreClient({
      region: REGION,
      credentials: fromNodeProviderChain(),
      requestHandler: { requestTimeout: 300000 },
    });

    // Build prompt with conversation history for context
    let fullPrompt = rewritePromptForCharts(prompt);
    if (userId && sessionId) {
      try {
        const histResult = await ddbClient.send(new QueryCommand({
          TableName: TABLE_NAME,
          KeyConditionExpression: "userId = :uid AND sessionId = :sid",
          ExpressionAttributeValues: { ":uid": userId, ":sid": sessionId },
        }));
        const item = (histResult.Items || [])[0];
        if (item && item.messages && item.messages.length > 1) {
          // Exclude the last message (the one we just saved above)
          const history = item.messages.slice(0, -1);
          // Take last 20 messages max to avoid token limits
          const recent = history.slice(-20);
          const historyText = recent.map((m) =>
            `${m.role === "user" ? "User" : "Assistant"}: ${m.content.slice(0, 2000)}`
          ).join("\n\n");
          fullPrompt = "Here is our conversation history for context:\n\n" + historyText + "\n\n---\nUser\'s new message: " + fullPrompt;
        }
      } catch (histErr) {
        console.error("Failed to load history for context:", histErr);
        // Continue without history
      }
    }

    const command = new InvokeAgentRuntimeCommand({
      agentRuntimeArn: runtimeArn,
      runtimeSessionId: sessionId,
      payload: JSON.stringify({ prompt: fullPrompt }),
    });

    const response = await client.send(command);

    let body = "";
    if (response.response) {
      const chunks = [];
      if (typeof response.response[Symbol.asyncIterator] === "function") {
        for await (const chunk of response.response) {
          chunks.push(typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk));
        }
      } else if (response.response instanceof Uint8Array) {
        chunks.push(new TextDecoder().decode(response.response));
      } else if (typeof response.response === "string") {
        chunks.push(response.response);
      } else if (Buffer.isBuffer(response.response)) {
        chunks.push(response.response.toString("utf-8"));
      } else {
        chunks.push(JSON.stringify(response.response));
      }
      body = chunks.join("");
    }

    let parsed;
    try { parsed = JSON.parse(body); } catch { parsed = body; }

    let responseText;
    if (typeof parsed === "string") {
      responseText = parsed;
    } else if (typeof parsed === "object" && parsed !== null) {
      const textField = parsed.message || parsed.response || parsed.text || parsed.answer;
      if (typeof textField === "string" && textField.length > 20) {
        responseText = textField;
        const otherKeys = Object.keys(parsed).filter(
          (k) => !["message", "response", "text", "answer"].includes(k)
        );
        if (otherKeys.length > 0) {
          const extra = {};
          otherKeys.forEach((k) => (extra[k] = parsed[k]));
          responseText += "\n\n```json\n" + JSON.stringify(extra, null, 2) + "\n```";
        }
      } else {
        responseText = JSON.stringify(parsed, null, 2);
      }
    } else {
      responseText = String(parsed);
    }

    // Save assistant response
    if (userId && sessionId) {
      await saveMessage(userId, sessionId, sessionTitle, "assistant", responseText);
    }

    res.json({ response: responseText });
  } catch (err) {
    console.error("AgentCore invocation error:", err);
    res.status(500).json({ error: err.message || "Failed to invoke agent" });
  }
});

// Health check
app.get("/api/health", async (req, res) => {
  try {
    const arn = await getRuntimeArn();
    res.json({ status: "ok", runtimeArn: arn, region: REGION });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

const SERVER_PORT = process.env.SERVER_PORT || 3001;
app.listen(SERVER_PORT, () => {
  console.log(`Proxy server running on http://localhost:${SERVER_PORT}`);
  console.log(`Region: ${REGION}`);
});
