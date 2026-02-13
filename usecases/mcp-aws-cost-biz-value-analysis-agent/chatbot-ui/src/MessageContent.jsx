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
      return (
        <>
          <MarkdownBlock text={mdText} />
          <ExpandableSection headerText="View JSON data" variant="footer">
            <pre className="md-code-block">
              <code>{prettyJson(trimmed)}</code>
            </pre>
          </ExpandableSection>
        </>
      );
    }
    return <JsonAutoTable data={parsed} raw={trimmed} />;
  }

  // Split content into segments: text vs json code blocks
  const segments = splitJsonBlocks(content);
  const mdSegments = segments.filter((s) => s.type === "md");
  const jsonSegments = segments.filter((s) => s.type === "json");

  return (
    <>
      {mdSegments.map((seg, i) => (
        <MarkdownBlock key={`md-${i}`} text={seg.value} />
      ))}
      {jsonSegments.map((seg, i) => (
        <JsonSection key={`json-${i}`} code={seg.value} />
      ))}
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
      {textFields.length > 0 && <JsonTextFields fields={textFields} />}
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
      {textFields.length > 0 && <JsonTextFields fields={textFields} />}
      <ExpandableSection headerText="View JSON data" variant="footer">
        <pre className="md-code-block">
          <code>{prettyJson(raw)}</code>
        </pre>
      </ExpandableSection>
    </>
  );
}

/**
 * Renders extracted text fields from JSON as styled paragraphs.
 */
function JsonTextFields({ fields }) {
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
