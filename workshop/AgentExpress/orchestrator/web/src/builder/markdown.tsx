/** The little Markdown the designer and the in-app assistant write — paragraphs, headings,
 *  bullet and numbered lists, tables, rules, fenced code, **bold**, *italic* and `code` —
 *  rendered as React elements. Never as HTML: the
 *  text is a model's, so nothing in it can become markup. */
import type { ReactNode } from "react";
import "./markdown.css";

function inline(text: string, key: string): ReactNode[] {
  const out: ReactNode[] = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`|\*[^*\s][^*]*\*)/g;
  let last = 0;
  let m: RegExpExecArray | null;
  let n = 0;
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(text.slice(last, m.index));
    const t = m[0];
    const k = `${key}-${n++}`;
    // Bold may hold `code` (the designer writes **`tools.crm.endpoint`**), so its inside
    // is rendered too.
    if (t.startsWith("**")) out.push(<strong key={k}>{inline(t.slice(2, -2), k)}</strong>);
    else if (t.startsWith("`")) out.push(<code key={k}>{t.slice(1, -1)}</code>);
    else out.push(<em key={k}>{t.slice(1, -1)}</em>);
    last = m.index + t.length;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

const RULE = /^\s*([-*_])(\s*\1){2,}\s*$/;
const ROW = /^\s*\|.*\|\s*$/;
const SEP = /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?\s*$/;
const cells = (row: string) => row.trim().replace(/^\|/, "").replace(/\|$/, "").split("|").map((c) => c.trim());

/** `streaming`: the reply is still being written, so a cursor follows its last word. */
export function Markdown({ text, streaming = false }: { text: string; streaming?: boolean }) {
  const blocks: ReactNode[] = [];
  const lines = text.replace(/\r/g, "").split("\n");
  let i = 0;
  let b = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (!line.trim()) { i++; continue; }
    const key = `b${b++}`;
    if (/^\s*```/.test(line)) {
      const code: string[] = [];
      i++;
      while (i < lines.length && !/^\s*```/.test(lines[i])) code.push(lines[i++]);
      i++;                                   // the closing fence (or the end, mid-stream)
      blocks.push(<pre key={key} className="axd-md-pre"><code>{code.join("\n")}</code></pre>);
      continue;
    }
    if (RULE.test(line)) {
      blocks.push(<hr key={key} className="axd-md-hr" />);
      i++;
      continue;
    }
    // A table: a header row, a |---|---| row, then body rows.
    if (ROW.test(line) && i + 1 < lines.length && SEP.test(lines[i + 1])) {
      const head = cells(line);
      i += 2;
      const body: string[][] = [];
      while (i < lines.length && ROW.test(lines[i])) body.push(cells(lines[i++]));
      blocks.push(
        <table key={key} className="axd-md-table">
          <thead><tr>{head.map((c, j) => <th key={j}>{inline(c, `${key}-h${j}`)}</th>)}</tr></thead>
          <tbody>{body.map((r, n) => <tr key={n}>{r.map((c, j) => <td key={j}>{inline(c, `${key}-${n}-${j}`)}</td>)}</tr>)}</tbody>
        </table>);
      continue;
    }
    const heading = /^(#{1,4})\s+(.*)$/.exec(line);
    if (heading) {
      blocks.push(<p key={key} className="axd-md-h"><strong>{inline(heading[2], key)}</strong></p>);
      i++;
      continue;
    }
    const bullet = /^\s*[-*•]\s+/;
    const numbered = /^\s*\d+[.)]\s+/;
    if (bullet.test(line) || numbered.test(line)) {
      const ordered = numbered.test(line);
      const items: ReactNode[] = [];
      while (i < lines.length && (ordered ? numbered : bullet).test(lines[i])) {
        items.push(<li key={`${key}-${items.length}`}>{inline(lines[i].replace(ordered ? numbered : bullet, ""), `${key}-${items.length}`)}</li>);
        i++;
      }
      blocks.push(ordered ? <ol key={key}>{items}</ol> : <ul key={key}>{items}</ul>);
      continue;
    }
    const para: string[] = [];
    while (i < lines.length && lines[i].trim() && !bullet.test(lines[i]) && !numbered.test(lines[i])
      && !/^#{1,4}\s/.test(lines[i]) && !/^\s*```/.test(lines[i]) && !RULE.test(lines[i]) && !ROW.test(lines[i])) {
      para.push(lines[i]);
      i++;
    }
    blocks.push(<p key={key}>{inline(para.join(" "), key)}</p>);
  }
  return <div className={streaming ? "axd-md axd-streaming" : "axd-md"}>{blocks}</div>;
}
