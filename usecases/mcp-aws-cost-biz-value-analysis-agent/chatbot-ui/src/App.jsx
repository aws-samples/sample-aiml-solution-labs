import { useEffect, useState, useCallback, useRef } from "react";
import TopNavigation from "@cloudscape-design/components/top-navigation";
import { Box, Button, Spinner, SpaceBetween } from "@cloudscape-design/components";
import { v4 as uuidv4 } from "uuid";
import ChatPanel from "./ChatPanel";
import authService from "./authService";
import awsService from "./awsService";

export default function App() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const credentialsRef = useRef(null);

  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search);
    const code = urlParams.get("code");

    if (code) {
      authService.handleCallback(code).then(async (authData) => {
        authService.storeAuth(authData);
        // Get AWS credentials from Identity Pool
        try {
          credentialsRef.current = await awsService.getCredentials(authData.token);
        } catch (err) {
          console.error("Failed to get AWS credentials:", err);
        }
        setUser(authData.user);
        setLoading(false);
        window.history.replaceState({}, document.title, "/");
      }).catch((err) => {
        console.error("Auth callback failed:", err);
        console.error("Error details:", err.response?.data || err.message);
        setError(`Authentication failed: ${err.response?.data?.error || err.message}`);
        setLoading(false);
      });
    } else {
      const stored = authService.getStoredAuth();
      if (stored && stored.user && stored.token) {
        // Get AWS credentials from stored token
        awsService.getCredentials(stored.token).then((creds) => {
          credentialsRef.current = creds;
          setUser(stored.user);
          setLoading(false);
        }).catch((err) => {
          console.error("Credentials expired, re-login:", err);
          authService.login();
        });
      } else {
        authService.login();
      }
    }
  }, []);

  const getCredentials = useCallback(async () => {
    if (credentialsRef.current) return credentialsRef.current;
    const stored = authService.getStoredAuth();
    if (stored && stored.token) {
      credentialsRef.current = await awsService.getCredentials(stored.token);
      return credentialsRef.current;
    }
    throw new Error("No credentials available");
  }, []);

  if (loading) {
    return (
      <div style={centerStyle}>
        <Box textAlign="center">
          <Spinner size="large" />
          <Box variant="p" margin={{ top: "s" }}>Authenticating via Midway...</Box>
        </Box>
      </div>
    );
  }

  if (error) {
    return (
      <div style={centerStyle}>
        <Box textAlign="center" color="text-status-error">{error}</Box>
      </div>
    );
  }

  if (!user) return null;
  return <AuthenticatedApp user={user} getCredentials={getCredentials} />;
}

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

function AuthenticatedApp({ user, getCredentials }) {
  const userId = user.email || user.username || user.sub;
  const [sessions, setSessions] = useState([]);
  const [activeSessionId, setActiveSessionId] = useState(null);
  const [activeMessages, setActiveMessages] = useState([]);
  const [loadingSessions, setLoadingSessions] = useState(true);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [isAdmin, setIsAdmin] = useState(false);

  const fetchSessions = useCallback(async () => {
    try {
      const creds = await getCredentials();
      const [items, admin] = await Promise.all([
        awsService.listSessions(creds, userId),
        awsService.isAdmin(creds, userId),
      ]);
      setIsAdmin(admin);
      const sorted = items.sort((a, b) =>
        (b.updatedAt || b.createdAt || "").localeCompare(a.updatedAt || a.createdAt || "")
      );
      setSessions(sorted);
    } catch (err) {
      console.error("Failed to load sessions:", err);
    } finally {
      setLoadingSessions(false);
    }
  }, [userId, getCredentials]);

  useEffect(() => { fetchSessions(); }, [fetchSessions]);

  const startNewSession = () => {
    const newId = uuidv4() + "-session";
    setActiveSessionId(newId);
    setActiveMessages([]);
  };

  useEffect(() => {
    if (!activeSessionId) startNewSession();
  }, []);

  const loadSession = async (sessionId) => {
    setActiveSessionId(sessionId);
    try {
      const creds = await getCredentials();
      const data = await awsService.getSessionMessages(creds, userId, sessionId);
      setActiveMessages(data.messages || []);
    } catch (err) {
      console.error("Failed to load session:", err);
      setActiveMessages([]);
    }
  };

  const handleFirstMessage = (prompt) => {
    setSessions((prev) => [
      { sessionId: activeSessionId, title: prompt.slice(0, 80), createdAt: getETTimestamp(), updatedAt: getETTimestamp() },
      ...prev,
    ]);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh" }}>
      <TopNavigation
        identity={{ href: "#", title: "AWS TCO & Business Value Analyst" }}
        utilities={[
          {
            type: "button",
            text: "Pre-Beta Release - AWS Internal Only",
            disableTextCollapse: true,
          },
          {
            type: "menu-dropdown",
            text: user.email || user.username || "User",
            iconName: "user-profile",
            items: [{ id: "signout", text: "Sign out" }],
            onItemClick: ({ detail }) => {
              if (detail.id === "signout") authService.logout();
            },
          },
        ]}
      />
      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {/* Sidebar */}
        <div className={`sidebar ${sidebarOpen ? "open" : "closed"}`}>
          <div className="sidebar-header">
            <Button variant="primary" onClick={() => { startNewSession(); }} iconName="add-plus" fullWidth>
              New Conversation
            </Button>
            <button className="sidebar-toggle" onClick={() => setSidebarOpen(!sidebarOpen)} title={sidebarOpen ? "Collapse" : "Expand"}>
              {sidebarOpen ? "◀" : "▶"}
            </button>
          </div>
          {sidebarOpen && (
            <>
              <div className="sidebar-sessions">
                {loadingSessions ? (
                  <Box textAlign="center" padding="l"><Spinner /></Box>
                ) : sessions.length === 0 ? (
                  <Box textAlign="center" padding="l" color="text-status-inactive" variant="small">
                    No conversations yet
                  </Box>
                ) : (
                  <SpaceBetween size="xxs">
                    {sessions.map((s) => (
                      <div
                        key={s.sessionId}
                        className={`session-item ${s.sessionId === activeSessionId ? "active" : ""}`}
                        onClick={() => loadSession(s.sessionId)}
                      >
                        <div className="session-title">{s.title || "Untitled"}</div>
                        <div className="session-date">
                          {s.updatedAt || s.createdAt}
                        </div>
                      </div>
                    ))}
                  </SpaceBetween>
                )}
              </div>
              <div className="sidebar-links">
                <div className="sidebar-links-title">Quick Links</div>
                <a href="https://aws.amazon.com/bedrock/pricing/" target="_blank" rel="noopener noreferrer" className="sidebar-link">Bedrock Pricing</a>
                <a href="https://calculator.aws/#/createCalculator/bedrock" target="_blank" rel="noopener noreferrer" className="sidebar-link">Bedrock Calculator</a>
                <a href="https://aws.amazon.com/bedrock/agentcore/pricing/" target="_blank" rel="noopener noreferrer" className="sidebar-link">AgentCore Pricing</a>
                <a href="https://calculator.aws/#/createCalculator/bedrockagentcore" target="_blank" rel="noopener noreferrer" className="sidebar-link">AgentCore Calculator</a>
              </div>
            </>
          )}
        </div>

        {/* Chat area */}
        <div style={{ flex: 1, overflow: "hidden" }}>
          {activeSessionId && (
            <ChatPanel
              key={activeSessionId}
              sessionId={activeSessionId}
              userId={userId}
              messages={activeMessages}
              onFirstMessage={handleFirstMessage}
              getCredentials={getCredentials}
              isAdmin={isAdmin}
            />
          )}
        </div>
      </div>
    </div>
  );
}

const centerStyle = {
  display: "flex",
  justifyContent: "center",
  alignItems: "center",
  height: "100vh",
};
