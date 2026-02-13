/**
 * Browser-side AWS service layer.
 * Uses Cognito Identity Pool to get temporary AWS credentials,
 * then calls DynamoDB and BedrockAgentCore directly from the browser.
 */
import { CognitoIdentityClient, GetIdCommand, GetCredentialsForIdentityCommand } from "@aws-sdk/client-cognito-identity";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, QueryCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import {
  BedrockAgentCoreClient,
  InvokeAgentRuntimeCommand,
} from "@aws-sdk/client-bedrock-agentcore";
import {
  BedrockAgentCoreControlClient,
  ListAgentRuntimesCommand,
} from "@aws-sdk/client-bedrock-agentcore-control";
import { cognitoConfig } from "./authConfig";

const REGION = cognitoConfig.region;
const TABLE_NAME = "tco-bva-chat-sessions";
const AGENT_NAME = "aws_tco_biz_value_analyst";

// Identity Pool ID — set during build via deploy.sh or .env
const IDENTITY_POOL_ID = process.env.REACT_APP_IDENTITY_POOL_ID || "";

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

class AwsService {
  constructor() {
    this._credentials = null;
    this._credentialsExpiry = null;
    this._ddbClient = null;
    this._cachedRuntimeArn = null;
  }

  /**
   * Get temporary AWS credentials from Cognito Identity Pool.
   */
  async getCredentials(idToken) {
    // Reuse if not expired (refresh 5 min before expiry)
    if (this._credentials && this._credentialsExpiry && Date.now() < this._credentialsExpiry - 300000) {
      return this._credentials;
    }

    const cognitoIdentity = new CognitoIdentityClient({ region: REGION });
    const providerName = `cognito-idp.${REGION}.amazonaws.com/${cognitoConfig.userPoolId}`;

    // Step 1: Get Identity ID
    const idResp = await cognitoIdentity.send(new GetIdCommand({
      IdentityPoolId: IDENTITY_POOL_ID,
      Logins: { [providerName]: idToken },
    }));

    // Step 2: Get credentials
    const credResp = await cognitoIdentity.send(new GetCredentialsForIdentityCommand({
      IdentityId: idResp.IdentityId,
      Logins: { [providerName]: idToken },
    }));

    this._credentials = {
      accessKeyId: credResp.Credentials.AccessKeyId,
      secretAccessKey: credResp.Credentials.SecretKey,
      sessionToken: credResp.Credentials.SessionToken,
    };
    this._credentialsExpiry = credResp.Credentials.Expiration.getTime();
    this._ddbClient = null; // Reset clients on new creds
    return this._credentials;
  }

  getDdbClient(credentials) {
    if (!this._ddbClient) {
      this._ddbClient = DynamoDBDocumentClient.from(
        new DynamoDBClient({ region: REGION, credentials })
      );
    }
    return this._ddbClient;
  }

  /**
   * Discover the AgentCore runtime ARN.
   */
  async getRuntimeArn(credentials) {
    if (this._cachedRuntimeArn) return this._cachedRuntimeArn;
    const controlClient = new BedrockAgentCoreControlClient({ region: REGION, credentials });
    const resp = await controlClient.send(new ListAgentRuntimesCommand({}));
    const runtimes = resp.agentRuntimes || [];
    const match = runtimes.find(
      (rt) => rt.agentRuntimeName === AGENT_NAME && rt.status === "READY"
    );
    if (!match) throw new Error(`No READY runtime found for agent "${AGENT_NAME}" in ${REGION}`);
    this._cachedRuntimeArn = match.agentRuntimeArn;
    return this._cachedRuntimeArn;
  }

  /**
   * Save a message to DynamoDB.
   */
  async saveMessage(credentials, userId, sessionId, sessionTitle, role, content) {
    const ddb = this.getDdbClient(credentials);
    const now = getESTTimestamp();
    const msg = { role, content, timestamp: now };

    try {
      await ddb.send(new UpdateCommand({
        TableName: TABLE_NAME,
        Key: { userId, sessionId },
        UpdateExpression: "SET messages = list_append(messages, :msg), updatedAt = :now",
        ConditionExpression: "attribute_exists(userId)",
        ExpressionAttributeValues: { ":msg": [msg], ":now": now },
      }));
    } catch (err) {
      if (err.name === "ConditionalCheckFailedException") {
        await ddb.send(new PutCommand({
          TableName: TABLE_NAME,
          Item: {
            userId, sessionId,
            title: sessionTitle || content.slice(0, 80),
            messages: [msg],
            createdAt: now, updatedAt: now,
          },
        }));
      } else {
        throw err;
      }
    }
  }

  /**
   * List sessions for a user.
   */
  async listSessions(credentials, userId) {
    const ddb = this.getDdbClient(credentials);
    const result = await ddb.send(new QueryCommand({
      TableName: TABLE_NAME,
      KeyConditionExpression: "userId = :uid",
      ExpressionAttributeValues: { ":uid": userId },
      ProjectionExpression: "sessionId, title, createdAt, updatedAt",
      ScanIndexForward: false,
    }));
    return result.Items || [];
  }

  /**
   * Get messages for a session.
   */
  async getSessionMessages(credentials, userId, sessionId) {
    const ddb = this.getDdbClient(credentials);
    const result = await ddb.send(new QueryCommand({
      TableName: TABLE_NAME,
      KeyConditionExpression: "userId = :uid AND sessionId = :sid",
      ExpressionAttributeValues: { ":uid": userId, ":sid": sessionId },
    }));
    const item = (result.Items || [])[0];
    return { messages: item ? item.messages : [], title: item?.title };
  }

  /**
   * Send a chat message — calls AgentCore directly from the browser.
   */
  async chat(credentials, { prompt, sessionId, userId, sessionTitle }) {
    // Save user message
    if (userId && sessionId) {
      await this.saveMessage(credentials, userId, sessionId, sessionTitle, "user", prompt);
    }

    const runtimeArn = await this.getRuntimeArn(credentials);
    const client = new BedrockAgentCoreClient({ region: REGION, credentials });

    // Build prompt with conversation history
    let fullPrompt = rewritePromptForCharts(prompt);
    if (userId && sessionId) {
      try {
        const { messages: history } = await this.getSessionMessages(credentials, userId, sessionId);
        if (history && history.length > 1) {
          const prev = history.slice(0, -1).slice(-20);
          const historyText = prev.map((m) =>
            `${m.role === "user" ? "User" : "Assistant"}: ${m.content.slice(0, 2000)}`
          ).join("\n\n");
          fullPrompt = "Here is our conversation history for context:\n\n" + historyText + "\n\n---\nUser's new message: " + fullPrompt;
        }
      } catch (histErr) {
        console.error("Failed to load history for context:", histErr);
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
      await this.saveMessage(credentials, userId, sessionId, sessionTitle, "assistant", responseText);
    }

    return responseText;
  }
}

const awsService = new AwsService();
export default awsService;
