import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { ExpandableSection, Table as CloudscapeTable } from "@cloudscape-design/components";
import { tryRenderChart } from "./AutoChart";

/**
 * Renders assistant messages with full markdown formatting.
 * - Markdown tables render via remark-gfm
 * - ```json blocks render in a collapsible expander
 * - Pure JSON responses auto-render arrays as Cloudscape tables
 */
export default function MessageContent({ content }) {
  // Check if the entire response is pure JSON (no markdown)
  const trimmed = content.trim();
  const parsed = tryParseJson(trimmed);
  if (parsed !== null) {
    // If JSON has a markdown string field, extract and render as markdown
    const mdText = extractMarkdownString(parsed);
    if (mdText) {
      const { clean, verification } = extractVerification(mdText);
      return (
        <>
          <MarkdownBlock text={clean} />
          <ExpandableSection headerText="View JSON data" variant="footer">
            <pre className="md-code-block">
              <code>{prettyJson(trimmed)}</code>
            </pre>
          </ExpandableSection>
          {verification && (
            <ExpandableSection headerText="Verification details" variant="footer">
              <MarkdownBlock text={verification} />
            </ExpandableSection>
          )}
        </>
      );
    }
    return <JsonAutoTable data={parsed} raw={trimmed} />;
  }

  // Split content into segments: text vs json code blocks
  const segments = splitJsonBlocks(content);
  const mdSegments = segments.filter((s) => s.type === "md");
  const jsonSegments = segments.filter((s) => s.type === "json");

  // Extract verification from all markdown portions
  const rawMd = mdSegments.map((s) => s.value).join("");
  const { clean: cleanMd, verification } = extractVerification(rawMd);

  return (
    <>
      {cleanMd && <MarkdownBlock text={cleanMd} />}
      {jsonSegments.map((seg, i) => (
        <JsonSection key={`json-${i}`} code={seg.value} />
      ))}
      {verification && (
        <ExpandableSection headerText="Verification details" variant="footer">
          <MarkdownBlock text={verification} />
        </ExpandableSection>
      )}
    </>
  );
}

/**
 * Reusable markdown renderer component.
 */
function MarkdownBlock({ text }) {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        table: ({ children }) => (
          <table className="md-table">{children}</table>
        ),
        code: ({ inline, className, children }) => {
          if (inline) {
            return <code className="md-inline-code">{children}</code>;
          }
          return (
            <pre className="md-code-block">
              <code>{children}</code>
            </pre>
          );
        },
      }}
    >
      {text}
    </ReactMarkdown>
  );
}

/**
 * Renders a JSON code block as both a Cloudscape table (if array found) and a collapsible raw view.
 */
function JsonSection({ code }) {
  const parsed = tryParseJson(code);
  const array = parsed ? findFirstArray(parsed) : null;
  const textFields = parsed ? collectTextFields(parsed) : [];
  const chart = parsed ? tryRenderChart(parsed) : null;

  return (
    <>
      {chart}
      {!chart && array && array.length > 0 && <AutoTable rows={array} />}
      {textFields.length > 0 && <JsonTextFields fields={textFields} data={parsed} />}
      <ExpandableSection headerText="View JSON data" variant="footer">
        <pre className="md-code-block">
          <code>{prettyJson(code)}</code>
        </pre>
      </ExpandableSection>
    </>
  );
}

/**
 * Handles a pure-JSON response: renders table + collapsible raw JSON.
 */
function JsonAutoTable({ data, raw }) {
  const array = findFirstArray(data);
  const textFields = collectTextFields(data);
  const chart = tryRenderChart(data);

  return (
    <>
      {chart}
      {!chart && array && array.length > 0 && <AutoTable rows={array} />}
      {textFields.length > 0 && <JsonTextFields fields={textFields} data={data} />}
      <ExpandableSection headerText="View JSON data" variant="footer">
        <pre className="md-code-block">
          <code>{prettyJson(raw)}</code>
        </pre>
      </ExpandableSection>
    </>
  );
}

/**
 * Renders structured JSON data as grouped sections with tables.
 * Top-level keys become section headers, nested objects become tables of key-value pairs,
 * and long text fields (like calculation explanations) go into collapsible expanders.
 */
function JsonTextFields({ fields, data }) {
  if (!data || typeof data !== "object") {
    // Fallback to flat list if no structured data
    return (
      <div style={{ margin: "8px 0", padding: "8px 12px", background: "#f2f3f3", borderRadius: 8 }}>
        {fields.map((f, i) => (
          <p key={i} style={{ margin: "4px 0", fontSize: 14 }}>
            <strong>{f.label}:</strong> {f.value}
          </p>
        ))}
      </div>
    );
  }
  return <StructuredJsonView data={data} />;
}

function StructuredJsonView({ data }) {
  const topKeys = Object.keys(data);
  return (
    <div style={{ margin: "8px 0" }}>
      {topKeys.map((key) => {
        const val = data[key];
        if (val === null || val === undefined) return null;
        // Primitive top-level value — render as a highlighted summary
        if (typeof val !== "object") {
          return (
            <div key={key} style={{ padding: "8px 12px", margin: "4px 0", background: "#e3f2fd", borderRadius: 6, fontSize: 14, fontWeight: 600 }}>
              {formatHeader(key)}: {formatValue(val)}
            </div>
          );
        }
        // Object or array — render as a section
        return <JsonSectionBlock key={key} title={formatHeader(key)} value={val} />;
      })}
    </div>
  );
}

function JsonSectionBlock({ title, value }) {
  if (Array.isArray(value)) {
    // Array of objects → table
    if (value.length > 0 && typeof value[0] === "object") {
      return (
        <div style={{ margin: "8px 0" }}>
          <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 4, color: "#0f1b2d" }}>{title}</div>
          <AutoTable rows={value} />
        </div>
      );
    }
    // Array of primitives
    return (
      <div style={{ padding: "6px 12px", margin: "4px 0", fontSize: 13 }}>
        <strong>{title}:</strong> {value.join(", ")}
      </div>
    );
  }

  // Object — collect leaf values into a table and sub-objects recursively
  const leafRows = [];
  const subSections = [];
  const explanations = [];

  for (const [k, v] of Object.entries(value)) {
    if (v === null || v === undefined) continue;
    const label = formatHeader(k);
    if (typeof v === "object" && !Array.isArray(v)) {
      subSections.push({ key: k, label, value: v });
    } else if (typeof v === "string" && (k.toLowerCase().includes("explanation") || k.toLowerCase().includes("calculation") || v.length > 150)) {
      explanations.push({ key: k, label, value: v });
    } else if (Array.isArray(v)) {
      if (v.length > 0 && typeof v[0] === "object") {
        subSections.push({ key: k, label, value: v });
      } else {
        leafRows.push({ metric: label, value: v.join(", ") });
      }
    } else {
      leafRows.push({ metric: label, value: formatValue(v) });
    }
  }

  return (
    <div style={{ margin: "10px 0", border: "1px solid #e9ebed", borderRadius: 8, overflow: "hidden" }}>
      <div style={{ padding: "8px 12px", background: "#f2f3f3", fontWeight: 600, fontSize: 14, borderBottom: "1px solid #e9ebed" }}>
        {title}
      </div>
      <div style={{ padding: "8px 12px" }}>
        {leafRows.length > 0 && <MetricsTable rows={leafRows} />}
        {subSections.map((s) => (
          <JsonSectionBlock key={s.key} title={s.label} value={s.value} />
        ))}
        {explanations.map((e) => (
          <ExplanationExpander key={e.key} label={e.label} text={e.value} />
        ))}
      </div>
    </div>
  );
}

function ExplanationExpander({ label, text }) {
  const [open, setOpen] = React.useState(false);
  const steps = splitExplanationSteps(text);
  return (
    <div style={{ margin: "6px 0" }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          background: "none", border: "none", cursor: "pointer", padding: "4px 0",
          fontSize: 13, color: "#0972d3", display: "flex", alignItems: "center", gap: 4,
        }}
      >
        <span style={{ fontSize: 10 }}>{open ? "▼" : "▶"}</span>
        {label}
      </button>
      {open && (
        <div style={{ padding: "6px 0 6px 16px", fontSize: 13, lineHeight: 1.6 }}>
          {steps.map((step, idx) => (
            <div key={idx} style={{ padding: "4px 8px", margin: "2px 0", background: idx % 2 === 0 ? "#f8f9fa" : "#fff", borderRadius: 4 }}>
              {step}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function MetricsTable({ rows }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, margin: "4px 0" }}>
      <tbody>
        {rows.map((r, i) => {
          // Detect long calculation explanation strings and render them specially
          const isLongCalc = typeof r.value === "string" && r.value.length > 120 &&
            (r.metric.toLowerCase().includes("explanation") || r.metric.toLowerCase().includes("calculation") ||
             /\d+\/\s/.test(r.value) || /=.*\*|=.*\+/.test(r.value));
          if (isLongCalc) {
            return (
              <tr key={i} style={{ borderBottom: "1px solid #e9ebed" }}>
                <td colSpan={2} style={{ padding: 0 }}>
                  <ExplanationExpander label={r.metric} text={r.value} />
                </td>
              </tr>
            );
          }
          return (
            <tr key={i} style={{ borderBottom: "1px solid #e9ebed" }}>
              <td style={{ padding: "5px 8px", color: "#5f6b7a", width: "50%" }}>{r.metric}</td>
              <td style={{ padding: "5px 8px", fontWeight: 500, textAlign: "left" }}>{r.value}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function formatValue(val) {
  if (typeof val === "number") {
    // Format with commas, and add $ prefix if it looks like a cost
    if (Number.isInteger(val) && Math.abs(val) >= 1000) return val.toLocaleString();
    if (!Number.isInteger(val)) return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return String(val);
  }
  return String(val);
}

/**
 * Splits a calculation explanation string into individual step lines.
 * Handles two patterns from the agent:
 *   1) Numbered: "1/ step_one = ..., 2/ step_two = ..."
 *   2) Unnumbered: "Step A (123) = formula, Step B (456) = formula2"
 * Returns an array of step strings for rendering as separate elements.
 */
function splitExplanationSteps(text) {
  if (!text || text.length < 20) return [text];

  // Pattern 1: numbered steps like "1/ ..., 2/ ..., 3/ ..."
  // Split on ", " followed by "digit/"
  if (/\d+\/\s/.test(text)) {
    const steps = text.split(/,\s*(?=\d+\/\s)/).map(s => s.trim()).filter(Boolean);
    if (steps.length > 1) return steps;
  }

  // Pattern 2: unnumbered steps separated by "), Capital"
  // e.g. "Total cost ($9,000.00) = input_cost ($6,500.00) + output_cost ($2,500.00), Input millions..."
  const steps2 = text.split(/\),\s*(?=[A-Z])/).map((s, i, arr) =>
    i < arr.length - 1 ? s.trim() + ")" : s.trim()
  ).filter(Boolean);
  if (steps2.length > 1) return steps2;

  // Pattern 3: fallback — split on ", " before a capitalized word followed by space
  const steps3 = text.split(/,\s+(?=[A-Z][a-z]+\s)/).map(s => s.trim()).filter(Boolean);
  if (steps3.length > 1) return steps3;

  return [text];
}

/**
 * Renders an array of objects as a Cloudscape Table.
 */
function AutoTable({ rows }) {
  if (!rows.length) return null;

  // Derive columns from keys of the first object
  const keys = Object.keys(rows[0]);
  const columnDefinitions = keys.map((key) => ({
    id: key,
    header: formatHeader(key),
    cell: (item) => {
      const val = item[key];
      if (val === null || val === undefined) return "—";
      if (typeof val === "object") return JSON.stringify(val);
      return String(val);
    },
    sortingField: key,
    width: Math.max(140, formatHeader(key).length * 10),
  }));

  return (
    <div style={{ margin: "8px 0" }}>
      <CloudscapeTable
        items={rows}
        columnDefinitions={columnDefinitions}
        variant="embedded"
        stripedRows
        wrapLines
        contentDensity="compact"
      />
    </div>
  );
}

/**
 * Converts snake_case or camelCase keys to Title Case headers.
 */
function formatHeader(key) {
  return key
    .replace(/_/g, " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Recursively finds the first array of objects in a JSON structure.
 */
function findFirstArray(obj) {
  if (Array.isArray(obj) && obj.length > 0 && typeof obj[0] === "object") {
    return obj;
  }
  if (obj && typeof obj === "object") {
    for (const val of Object.values(obj)) {
      const found = findFirstArray(val);
      if (found) return found;
    }
  }
  return null;
}

/**
 * Recursively collects string/number/boolean leaf values from a JSON object,
 * skipping arrays (which are rendered as tables). Returns [{label, value}].
 */
function collectTextFields(obj, prefix = "") {
  const fields = [];
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return fields;

  for (const [key, val] of Object.entries(obj)) {
    const label = prefix ? `${prefix} › ${formatHeader(key)}` : formatHeader(key);
    if (val === null || val === undefined) continue;
    if (Array.isArray(val)) {
      // Skip arrays — they're rendered as tables
      // But handle arrays of primitives (strings/numbers)
      if (val.length > 0 && typeof val[0] !== "object") {
        fields.push({ label, value: val.join(", ") });
      }
      continue;
    }
    if (typeof val === "object") {
      fields.push(...collectTextFields(val, label));
    } else {
      fields.push({ label, value: String(val) });
    }
  }
  return fields;
}

/**
 * Checks if a parsed JSON object is essentially a wrapper around a markdown string.
 * Detects patterns like {"message": "...markdown..."} or {"response": "...markdown..."}.
 * Returns the markdown string if found, null otherwise.
 */
function extractMarkdownString(obj) {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;

  const keys = Object.keys(obj);
  // Look for a single long string field that contains markdown indicators
  for (const key of keys) {
    const val = obj[key];
    if (typeof val === "string" && val.length > 50 && looksLikeMarkdown(val)) {
      return val;
    }
  }
  return null;
}

/**
 * Heuristic: does this string contain markdown formatting?
 */
function looksLikeMarkdown(str) {
  const mdPatterns = [
    /\*\*[^*]+\*\*/,       // **bold**
    /^#{1,3}\s/m,           // # heading
    /\n\d+\.\s/,            // numbered list
    /\n[-*]\s/,             // bullet list
    /\|.*\|.*\|/,           // table row
    /```/,                  // code block
    /\[.*\]\(.*\)/,         // link
    /`[^`]+`/,              // inline code
  ];
  return mdPatterns.some((p) => p.test(str));
}

function tryParseJson(str) {
  try {
    const parsed = JSON.parse(str);
    return typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function prettyJson(str) {
  try {
    return JSON.stringify(JSON.parse(str), null, 2);
  } catch {
    return str;
  }
}

/**
 * Splits markdown content so that ```json ... ``` blocks become separate segments.
 */
function splitJsonBlocks(text) {
  const regex = /```json\s*\n([\s\S]*?)```/gi;
  const segments = [];
  let lastIndex = 0;
  let match;

  while ((match = regex.exec(text)) !== null) {
    if (match.index > lastIndex) {
      segments.push({ type: "md", value: text.slice(lastIndex, match.index) });
    }
    segments.push({ type: "json", value: match[1].trim() });
    lastIndex = match.index + match[0].length;
  }

  if (lastIndex < text.length) {
    segments.push({ type: "md", value: text.slice(lastIndex) });
  }

  return segments;
}

/**
 * Extracts "**Verification:**" sections from markdown text.
 * Returns { clean, verification } where clean has the section removed
 * and verification contains the extracted content (or null).
 *
 * The agent is instructed to start verification with "**Verification:**" on its own line.
 * We capture everything from that marker until the next heading, bold heading, code block,
 * or end of text.
 */
function extractVerification(text) {
  // Match **Verification:** (with optional colon/bold variants) and capture until next section or end
  const regex = /\**Verification:?\**\s*\n?([\s\S]*?)(?=\n\s*(?:#{1,3}\s|\*\*[A-Z])|\n\s*```|\s*$)/gi;
  const parts = [];
  let clean = text;
  let match;
  const matches = [];
  while ((match = regex.exec(text)) !== null) {
    matches.push({ full: match[0], content: match[1].trim() });
  }
  for (const m of matches) {
    clean = clean.replace(m.full, "\n");
    if (m.content) parts.push(m.content);
  }
  return {
    clean: clean.trim(),
    verification: parts.length > 0 ? parts.join("\n\n") : null,
  };
}