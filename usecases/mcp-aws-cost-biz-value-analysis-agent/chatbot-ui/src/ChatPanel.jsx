import React, { useState, useRef, useEffect, useCallback } from "react";
import {
  Box,
  Button,
  SpaceBetween,
  Textarea,
  StatusIndicator,
  Alert,
  Select,
} from "@cloudscape-design/components";
import MessageContent from "./MessageContent";
import awsService from "./awsService";

function getETTimestamp() {
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

const ALL_MODEL_OPTIONS = [
  { label: "Claude Sonnet 4.5 (default)", value: "" },
  { label: "Claude Opus 4.6", value: "us.anthropic.claude-opus-4-6-v1" },
  { label: "Claude Opus 4.5", value: "us.anthropic.claude-opus-4-5-20251101-v1:0" },
  { label: "Claude Opus 4", value: "us.anthropic.claude-opus-4-20250514-v1:0" },
  { label: "Claude Sonnet 4", value: "us.anthropic.claude-sonnet-4-20250514-v1:0" },
  { label: "Claude Haiku 4.5", value: "us.anthropic.claude-haiku-4-5-20251001-v1:0" },
  { label: "Nova Premier", value: "us.amazon.nova-premier-v1:0" },
  { label: "Nova Pro", value: "us.amazon.nova-pro-v1:0" },
  { label: "Nova 2 Lite", value: "us.amazon.nova-2-lite-v1:0" },
  { label: "Nova Lite", value: "us.amazon.nova-lite-v1:0" },
  { label: "Nova Micro", value: "us.amazon.nova-micro-v1:0" },
];

const USER_MODEL_OPTIONS = [
  { label: "Claude Sonnet 4.5 (default)", value: "" },
  { label: "Claude Opus 4.6", value: "us.anthropic.claude-opus-4-6-v1" },
  { label: "Claude Opus 4.5", value: "us.anthropic.claude-opus-4-5-20251101-v1:0" },
  { label: "Claude Haiku 4.5", value: "us.anthropic.claude-haiku-4-5-20251001-v1:0" },
];

export default function ChatPanel({ sessionId, userId, messages: initialMessages, onFirstMessage, getCredentials, isAdmin }) {
  const [messages, setMessages] = useState(initialMessages || []);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedModel, setSelectedModel] = useState(ALL_MODEL_OPTIONS[0]);
  const modelOptions = isAdmin ? ALL_MODEL_OPTIONS : USER_MODEL_OPTIONS;
  const messagesEndRef = useRef(null);
  const titleSentRef = useRef(false);

  useEffect(() => {
    setMessages(initialMessages || []);
    titleSentRef.current = false;
  }, [sessionId, initialMessages]);

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, []);

  useEffect(() => { scrollToBottom(); }, [messages, loading, scrollToBottom]);

  const sendMessage = async () => {
    const prompt = input.trim();
    if (!prompt || loading) return;

    setInput("");
    setError(null);
    const ts = getETTimestamp();
    setMessages((prev) => [...prev, { role: "user", content: prompt, timestamp: ts }]);
    setLoading(true);

    const isFirst = messages.length === 0 && !titleSentRef.current;
    if (isFirst) {
      titleSentRef.current = true;
      if (onFirstMessage) onFirstMessage(prompt);
    }

    try {
      const creds = await getCredentials();
      const responseText = await awsService.chat(creds, {
        prompt, sessionId, userId,
        sessionTitle: isFirst ? prompt.slice(0, 80) : undefined,
        modelId: selectedModel.value || undefined,
      });

      setMessages((prev) => [...prev, { role: "assistant", content: responseText, timestamp: getETTimestamp() }]);
    } catch (err) {
      const msg = err.message || "Failed to invoke agent";
      setError(msg);
      setMessages((prev) => [...prev, { role: "error", content: msg }]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.detail.key === "Enter" && !e.detail.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  return (
    <div className="chat-container">
      <div className="chat-messages">
        {messages.length === 0 && (
          <Box textAlign="center" padding={{ top: "xxxl" }}>
            <SpaceBetween size="s">
              <Box variant="h2" color="text-status-inactive">AWS TCO & Business Value Analyst</Box>
              <Box variant="p" color="text-body-secondary">Ask me about AWS pricing, cost analysis, or business value assessments.</Box>
              <Box variant="small" color="text-status-inactive">Powered by Amazon Bedrock AgentCore</Box>
            </SpaceBetween>
          </Box>
        )}

        {messages.map((msg, i) => (
          <div key={i} className="chat-message-wrapper">
            <div className={`chat-message ${msg.role}`}>
              {msg.role === "assistant" ? <MessageContent content={msg.content} /> : msg.content}
            </div>
            {msg.timestamp && (
              <div className={`chat-timestamp ${msg.role}`}>{msg.timestamp} ET</div>
            )}
          </div>
        ))}

        {loading && (
          <div className="typing-indicator"><span /><span /><span /></div>
        )}

        <div ref={messagesEndRef} />
      </div>

      <div className="chat-input-area">
        <SpaceBetween size="xs">
          {error && (
            <Alert type="error" dismissible onDismiss={() => setError(null)}>{error}</Alert>
          )}
          <Textarea
            value={input}
            onChange={({ detail }) => setInput(detail.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about AWS costs, pricing, or TCO analysis..."
            rows={3}
            disabled={loading}
            autoFocus
          />
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <div style={{ width: 240 }}>
              <Select
                selectedOption={selectedModel}
                onChange={({ detail }) => setSelectedModel(detail.selectedOption)}
                options={modelOptions}
                placeholder="Model"
              />
            </div>
            <div style={{ flex: 1 }} />
            <Button variant="primary" onClick={sendMessage} loading={loading} disabled={!input.trim()} iconName="send">
              Send
            </Button>
          </div>
          <Box variant="small" color="text-status-inactive" textAlign="center">
            <StatusIndicator type="success">Connected</StatusIndicator>
            &nbsp;·&nbsp;Session: {sessionId ? sessionId.slice(0, 8) + "..." : "—"}
          </Box>
        </SpaceBetween>
      </div>
    </div>
  );
}
