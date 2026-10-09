/* Observability tab — self-contained.
 *
 * Everything for the cost/latency/token/quality views lives here: styles, data
 * fetches (the /api/telemetry/*, /api/insights endpoints), charts (Chart.js), and
 * drilldown. Its whole contract with the React shell is `mount(el, token, workflow)`
 * (see the bottom of this file); the only ambient globals it touches are optional
 * conveniences (`window.can`/`window.gate` for RBAC hints) that fall back to "allowed".
 *
 * Three levels:
 *   Overview    — aggregate by date / model / user over a range (chart + table).
 *   Run detail  — one session: per-agent cost/latency/tokens, expandable to the
 *                 individual model/tool/memory/guardrail/policy calls, plus a
 *                 "Prompts & I/O" inspector per agent (per run version) with the
 *                 AgentCore Evaluations scores for each named prompt.
 *   Insights    — cross-run AgentCore batch analysis: failure patterns, user
 *                 intents and execution summaries over a lookback window.
 */
(function () {
  "use strict";

  // ---- small helpers (reuse index.html globals when present) ----------------
  /* The element the React shell mounts us into. Set by mount(); every lookup is
     scoped to it so this module cannot reach into Cloudscape's DOM. Falls back to the
     document for the modal, which is appended to <body> on purpose. */
  let host = null;
  /* NOT CSS.escape(): this module already declares `const CSS` for its stylesheet, so
     that name resolves to a string here and `.escape` is undefined. Every id below is a
     plain identifier anyway, so no escaping is needed. */
  const $ = (id) =>
    (host ? host.querySelector("#" + id) : null) || document.getElementById(id);
  const esc = window.esc || ((s) => String(s == null ? "" : s).replace(/[&<>"']/g,
    (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[m])));
  let authToken = null;
  /* The projected workflow definition (/api/workflow), handed in by mount().
   *
   * IT USED TO BE READ AS A BARE GLOBAL named `workflow`, behind a typeof guard,
   * because the pre-React index.html fetched /api/workflow into a global of that
   * name. The Cloudscape shell holds it in React state instead and never assigns the
   * global, so every read here resolved to undefined: `evalAgents` was always empty,
   * `agentFlags().evalEnabled` was always false, and evalSection() returned "" for
   * EVERY agent. Evaluations were being recorded and billed, and the scores were
   * silently unrenderable — the Evaluation block and the Evaluate button were both
   * simply absent, which reads as "evaluations don't work".
   *
   * So it is part of the island's contract now, not an ambient global: a dependency
   * this module cannot get without being given it cannot go missing unnoticed. */
  let wfDef = {};
  /* Same shape as the old window.api: resolves the JSON body or throws with the BFF's
     own message, which is what names the group a 403 wanted. */
  /* The deployed Builder build this view is scoped to ("" = this deployment's own
     workflow). Only the two COLLECTION reads take it: a per-run read finds the run's
     build by the run id on the server. */
  let buildId = "";
  const scoped = (p) => (!buildId || !(p === "/api/sessions" || p.startsWith("/api/telemetry/aggregate")
      || p.startsWith("/api/insights")))
    ? p : p + (p.includes("?") ? "&" : "?") + "build=" + encodeURIComponent(buildId);
  const api = async (p0, o) => {
    const p = scoped(p0);
    const opts = o || {};
    const headers = Object.assign({}, opts.headers || {});
    if (authToken) headers["Authorization"] = "Bearer " + authToken;
    if (opts.body && !headers["Content-Type"]) headers["Content-Type"] = "application/json";
    const res = await fetch(p, Object.assign({}, opts, { headers }));
    if (!res.ok) {
      let detail = res.statusText;
      try { const b = await res.json(); detail = b.error || b.message || detail; } catch (e) { /* non-JSON */ }
      throw new Error(detail);
    }
    if (res.status === 204) return undefined;
    return res.json();
  };
  // RBAC (workflow.json -> authorization), enforced server-side in bff/authz.py.
  // These only stop the panel offering a control that would 403. Both fall back to
  // "allowed" when index.html hasn't defined them, so this file still works alone.
  const authzCan = (a) => (window.can ? window.can(a) : true);
  const authzGate = (a) => (window.gate ? window.gate(a) : "");
  /* A JS STRING LITERAL inside an inline handler: onclick="f(${jsq(x)})". esc() is
     the wrong tool there and undoes itself — the HTML parser decodes attribute
     entities BEFORE the JS is compiled, so esc()'s &#39; becomes a live quote that
     closes the argument. Returns the quoted literal; add no quotes of your own. */
  const jsq = window.jsq || ((v) => esc(JSON.stringify(String(v == null ? "" : v))));

  const fmtUsd = (x) => "$" + Number(x || 0).toLocaleString(undefined,
    { minimumFractionDigits: 2, maximumFractionDigits: 6 });
  const fmtInt = (x) => Number(x || 0).toLocaleString();
  const fmtMs = (x) => { x = Number(x || 0); return x >= 1000 ? (x / 1000).toFixed(1) + "s" : Math.round(x) + "ms"; };
  // Eastern-Time helpers (match the rest of the project).
  const etDateStr = () => new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" });
  const etStamp = () => new Date().toLocaleString("sv-SE", { timeZone: "America/New_York" })
    .replace(" ", "_").replace(/:/g, "-");
  // Shift a YYYY-MM-DD string by N days (noon-UTC anchor avoids tz/DST edges).
  const shiftDays = (ymd, days) => { const d = new Date(ymd + "T12:00:00Z"); d.setUTCDate(d.getUTCDate() + days); return d.toISOString().slice(0, 10); };

  // The estimate disclaimer, in two halves so the banner (which sets the lead-in in
  // bold) and the per-table note can share one wording rather than keeping two copies.
  const LEGEND_LEAD = "Costs and token counts are estimates for planning.";
  const LEGEND_DETAIL = "They will not match your actual AWS bill (private pricing, "
    + "region, and caching discounts differ).";
  const LEGEND = LEGEND_LEAD + " " + LEGEND_DETAIL;

  // Price-book provenance, displayed with every cost figure. These describe the
  // PRICE BOOK, not the deploy region: the rates in
  // app/features/observability/pricing.py are us-east-1 list prices, so a
  // deployment elsewhere still prices against them. Update both together.
  const PRICES_AS_OF = "2026-08-20";
  const PRICES_REGION = "us-east-1";

  // ---- styles (kept here so the tab is fully self-contained) -----------------
  // Reuses the app's global CSS variables (--blue, --surface, --muted, …) so the
  // tab is visually consistent with the rest of the product.
  const CSS = `
  /* The top navigation is styled by index.html (Cloudscape TopNavigation, dark
     surface). The light-header rules that used to live here fought it. */
  #obsView { flex:1; overflow-y:auto; background:linear-gradient(160deg,var(--bg1,#eef2f9),var(--bg2,#e6ebf5)); }
  .obs-inner { padding:26px 30px 60px; max-width:1240px; margin:0 auto; }

  /* Page heading */
  .obs-hero { display:flex; align-items:flex-start; gap:16px; margin-bottom:20px; }
  .obs-hero .obs-hero-ic { width:44px; height:44px; border-radius:13px; flex:0 0 auto;
      background:linear-gradient(135deg,#1e3a8a,#2563eb); color:#fff; font-size:22px;
      display:flex; align-items:center; justify-content:center; box-shadow:0 6px 16px #1e3a8a44; }
  .obs-hero h1 { margin:0 0 3px; font-size:20px; font-weight:800; letter-spacing:-.02em; color:#0f172a; }
  .obs-hero p { margin:0; font-size:13px; color:var(--muted,#64748b); max-width:640px; line-height:1.5; }

  /* Trust banner */
  .obs-banner { display:flex; align-items:center; gap:13px; background:linear-gradient(180deg,#f8fafc,#eef2f7);
      border:1px solid var(--border,#e2e8f0); border-radius:14px; padding:13px 18px; margin-bottom:16px; }
  .obs-banner .ck { width:24px; height:24px; border-radius:50%; background:#64748b; color:#fff; flex:0 0 auto;
      display:flex; align-items:center; justify-content:center; font-size:13px; font-weight:800; font-style:italic;
      font-family:Georgia,serif; box-shadow:0 2px 6px #64748b44; }
  .obs-banner .bt { font-size:12.5px; color:#334155; line-height:1.5; }
  .obs-banner .bt b { font-weight:700; color:#0f172a; }
  .obs-banner .bt .meta { color:var(--muted,#64748b); }

  /* Tabs */
  .obs-tabs { display:inline-flex; gap:4px; margin-bottom:18px; background:#fff; border:1px solid var(--border,#e2e8f0);
      border-radius:12px; padding:4px; box-shadow:0 1px 3px #0f172a0f; }
  .obs-tab { padding:8px 18px; border-radius:9px; border:0; background:transparent;
             font-weight:700; font-size:13px; cursor:pointer; color:var(--muted,#64748b); transition:all .15s; }
  .obs-tab:hover { color:#0f172a; }
  .obs-tab.active { background:linear-gradient(135deg,#1e3a8a,#2563eb); color:#fff; box-shadow:0 2px 8px #2563eb44; }

  /* Cards */
  .obs-card { background:var(--surface,#fff); border:1px solid var(--border,#e2e8f0); border-radius:16px;
      padding:20px 22px; margin-bottom:18px; box-shadow:0 1px 3px #0f172a0f; }
  .obs-card > h3 { margin:0 0 15px; font-size:12px; font-weight:700; color:var(--muted2,#94a3b8);
      text-transform:uppercase; letter-spacing:.05em; display:flex; align-items:center; gap:8px; }
  .obs-card > h3 .obs-note { text-transform:none; letter-spacing:0; font-weight:500; }

  /* Controls */
  .obs-controls { display:flex; flex-wrap:wrap; gap:14px; align-items:end; }
  .obs-controls label { display:flex; flex-direction:column; gap:5px; font-size:10.5px; font-weight:700;
                        text-transform:uppercase; letter-spacing:.04em; color:var(--muted,#64748b); }
  .obs-controls input, .obs-controls select { border:1px solid var(--border2,#cbd5e1); border-radius:9px; padding:8px 11px;
                        font-size:13px; min-width:150px; background:#fff; color:#0f172a; transition:border-color .15s, box-shadow .15s; }
  .obs-controls input:focus, .obs-controls select:focus { outline:none; border-color:var(--blue,#2563eb); box-shadow:0 0 0 3px #2563eb22; }
  .obs-controls .grow { flex:1; }
  .obs-btn { background:linear-gradient(135deg,#1e3a8a,#2563eb); color:#fff; border:none; border-radius:9px; padding:9px 18px;
             font-weight:700; font-size:13px; cursor:pointer; box-shadow:0 2px 6px #2563eb33; transition:filter .15s; }
  .obs-btn:hover { filter:brightness(1.06); }
  .obs-btn.ghost { background:#fff; color:var(--blue,#2563eb); border:1px solid #bfdbfe; box-shadow:none; }
  .obs-btn.ghost:hover { background:#eff6ff; filter:none; }
  .obs-exports { display:flex; gap:8px; }

  /* KPI strip */
  .obs-strip { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:14px; margin-bottom:18px; }
  .obs-kpi { position:relative; background:var(--surface,#fff); border:1px solid var(--border,#e2e8f0); border-radius:14px;
      padding:16px 18px 15px; box-shadow:0 1px 3px #0f172a0f; overflow:hidden; }
  .obs-kpi::before { content:""; position:absolute; top:0; left:0; bottom:0; width:4px; background:var(--kpi-accent,#2563eb); }
  .obs-kpi .v { font-size:24px; font-weight:800; color:#0f172a; letter-spacing:-.02em; line-height:1.1; font-variant-numeric:tabular-nums; }
  .obs-kpi .l { font-size:10.5px; font-weight:700; text-transform:uppercase; letter-spacing:.04em; color:var(--muted,#64748b); margin-top:5px; }
  .obs-kpi .s { font-size:11px; color:var(--muted2,#94a3b8); margin-top:3px; }

  /* Charts */
  .obs-chart-wrap { position:relative; height:300px; }

  /* Tables */
  table.obs { width:100%; border-collapse:collapse; font-size:12.5px; }
  table.obs th, table.obs td { text-align:left; padding:9px 12px; border-bottom:1px solid #eef2f7; }
  table.obs thead th { font-size:10px; text-transform:uppercase; letter-spacing:.04em; color:var(--muted,#64748b);
      font-weight:700; border-bottom:1.5px solid #e5e9f0; background:#fafbfd; position:sticky; top:0; }
  table.obs tbody tr:hover { background:#f8fafc; }
  table.obs td.num, table.obs th.num { text-align:right; font-variant-numeric:tabular-nums; }
  table.obs td.strong { font-weight:700; color:#0f172a; }
  .obs-tablewrap { max-height:520px; overflow:auto; border-radius:10px; border:1px solid #eef2f7; }
  tr.obs-agent { cursor:pointer; } tr.obs-agent:hover { background:#f4f7ff; }
  tr.obs-agent td.strong { color:#1e3a8a; }
  tr.obs-agent .caret { color:var(--blue,#2563eb); display:inline-block; width:12px; font-size:11px; }
  tr.obs-calls td { background:#fbfdff; padding:0; }
  .obs-calls-inner { padding:8px 12px 14px 30px; }

  /* Pills — one per telemetry kind (llm | tool | agentcore | memory | guardrail | policy | eval) */
  .obs-pill { display:inline-block; font-size:10px; font-weight:700; padding:2px 8px; border-radius:999px; text-transform:capitalize; }
  .obs-pill.llm { background:#eef2ff; color:#4338ca; }
  .obs-pill.tool { background:#ecfdf5; color:#047857; }
  .obs-pill.agentcore { background:#fff7ed; color:#c2410c; }
  .obs-pill.memory { background:#fdf4ff; color:#a21caf; }
  .obs-pill.guardrail { background:#fff1f2; color:#be123c; }
  .obs-pill.policy { background:#fefce8; color:#a16207; }
  .obs-pill.eval { background:#f0fdfa; color:#0f766e; }

  /* Per-agent "Prompts & I/O" trigger button (in the agent row) */
  .obs-io { margin-left:10px; font-size:10.5px; font-weight:700; color:var(--blue,#2563eb);
      background:#eff6ff; border:1px solid #bfdbfe; border-radius:7px; padding:2px 9px; cursor:pointer;
      vertical-align:middle; transition:background .15s; }
  .obs-io:hover { background:#dbeafe; }

  /* Modal (Prompts & I/O inspector) */
  .obs-modal-back { position:fixed; inset:0; background:rgba(15,23,42,.55); z-index:1000;
      display:flex; align-items:flex-start; justify-content:center; padding:40px 18px; overflow-y:auto; }
  .obs-modal { background:#fff; border-radius:16px; width:min(920px,100%); box-shadow:0 24px 64px #0f172a55;
      display:flex; flex-direction:column; max-height:calc(100vh - 80px); overflow:hidden; }
  .obs-modal-head { display:flex; align-items:center; gap:12px; padding:18px 22px; border-bottom:1px solid var(--border,#e2e8f0);
      background:linear-gradient(180deg,#f8fafc,#eef2f7); }
  .obs-modal-head .mt { font-size:16px; font-weight:800; color:#0f172a; letter-spacing:-.01em; }
  .obs-modal-head .ms { font-size:12px; color:var(--muted,#64748b); }
  .obs-modal-x { margin-left:auto; border:0; background:#e2e8f0; color:#334155; width:30px; height:30px; border-radius:8px;
      font-size:16px; cursor:pointer; line-height:1; } .obs-modal-x:hover { background:#cbd5e1; }
  .obs-modal-body { padding:16px 22px 24px; overflow-y:auto; }

  /* KPI chips inside the modal */
  .obs-mchips { display:flex; flex-wrap:wrap; gap:8px; margin-bottom:14px; align-items:center; }
  .obs-mchip { background:#f1f5f9; border:1px solid #e2e8f0; border-radius:999px; padding:4px 12px; font-size:11.5px; color:#334155; }
  .obs-mchip b { color:#0f172a; }
  .obs-mchip code { font-family:ui-monospace,Menlo,monospace; font-size:11px; }

  /* Per-call title row + labeled meta grid */
  .obs-callttl { display:flex; align-items:center; flex-wrap:wrap; gap:10px; padding:11px 14px; background:#f8fafc; border-bottom:1px solid #eef2f7; }
  .obs-callttl .cn { font-weight:800; color:#0f172a; font-size:13px; }
  .obs-callttl .cmodel { font-size:12px; color:var(--muted,#64748b); font-family:ui-monospace,Menlo,monospace; }
  .obs-badge { display:inline-block; font-size:10px; font-weight:800; padding:2px 9px; border-radius:999px; text-transform:capitalize; }
  .obs-badge.ok { background:#ecfdf5; color:#047857; }
  .obs-badge.error { background:#fef2f2; color:#b91c1c; }
  .obs-badge.agentcore { background:#fdf4ff; color:#a21caf; }
  .obs-badge.passed { background:#ecfdf5; color:#047857; }
  .obs-badge.blocked { background:#fef2f2; color:#b91c1c; }
  .obs-badge.allowed { background:#ecfdf5; color:#047857; }
  .obs-badge.denied { background:#fef2f2; color:#b91c1c; }
  .obs-badge.logonly { background:#fefce8; color:#a16207; }
  .obs-metagrid { display:grid; grid-template-columns:repeat(auto-fit,minmax(104px,1fr)); gap:10px 16px; padding:12px 16px; border-bottom:1px solid #f1f5f9; }
  .obs-kv { display:flex; flex-direction:column; gap:2px; min-width:0; }
  .obs-kv .k { font-size:9.5px; font-weight:700; text-transform:uppercase; letter-spacing:.04em; color:var(--muted2,#94a3b8); }
  .obs-kv .val { font-size:13.5px; font-weight:700; color:#0f172a; font-variant-numeric:tabular-nums; }
  .obs-kv .val small { font-size:11px; font-weight:600; color:var(--muted,#64748b); }

  /* One call block */
  .obs-callblk { border:1px solid var(--border,#e2e8f0); border-radius:12px; margin-bottom:14px; overflow:hidden; }
  .obs-seg { border-top:1px solid #f1f5f9; }
  .obs-seg > summary { cursor:pointer; padding:9px 14px; font-size:11px; font-weight:700; text-transform:uppercase;
      letter-spacing:.04em; color:var(--muted,#64748b); list-style:none; display:flex; align-items:center; gap:8px; user-select:none; }
  .obs-seg > summary::-webkit-details-marker { display:none; }
  .obs-seg > summary:hover { background:#f8fafc; color:#0f172a; }
  .obs-seg > summary .tw { color:var(--blue,#2563eb); font-size:10px; }
  .obs-seg > summary .cc { margin-left:auto; text-transform:none; letter-spacing:0; font-weight:600; color:var(--muted2,#94a3b8); }
  .obs-seg pre { margin:0; padding:12px 16px 16px; background:#0f172a; color:#e2e8f0; font-size:12px; line-height:1.55;
      white-space:pre-wrap; word-break:break-word; max-height:360px; overflow:auto;
      font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }
  .obs-seg.sys pre { background:#1e293b; }
  .obs-copy { margin-left:8px; font-size:10px; font-weight:700; color:var(--blue,#2563eb); background:#eff6ff;
      border:1px solid #bfdbfe; border-radius:6px; padding:1px 8px; cursor:pointer; text-transform:none; letter-spacing:0; }
  .obs-copy:hover { background:#dbeafe; }
  .obs-seg-empty { padding:10px 16px 14px; font-size:12px; color:var(--muted2,#94a3b8); font-style:italic; }

  /* Per-version grouping in the Prompts & I/O modal */
  .obs-verhead { display:flex; align-items:center; flex-wrap:wrap; gap:10px; padding:12px 14px;
      background:linear-gradient(180deg,#eef2ff,#e0e7ff); border:1px solid #c7d2fe; border-radius:10px; margin-bottom:14px; }
  .obs-vertag { font-size:12px; font-weight:800; color:#3730a3; background:#fff; border:1px solid #c7d2fe;
      border-radius:999px; padding:3px 10px; }
  .obs-vernote { font-size:12px; color:var(--muted,#64748b); }
  .obs-verfb { flex:1 1 100%; font-size:12.5px; color:#334155; background:#fffbeb; border:1px solid #fde68a;
      border-radius:8px; padding:8px 11px; }
  .obs-verfb b { color:#92400e; }
  /* Version picker (dropdown) at the top of the Prompts & I/O modal */
  .obs-vercontrols { display:flex; align-items:center; gap:10px; margin:0 0 14px; padding:10px 12px;
      background:#eef2ff; border:1px solid #c7d2fe; border-radius:10px; }
  .obs-vercontrols label { font-size:12px; font-weight:700; color:#3730a3; }
  .obs-verselect { font-size:12.5px; color:#0f172a; background:#fff; border:1px solid #c7d2fe;
      border-radius:8px; padding:6px 9px; cursor:pointer; }
  .obs-vercount { font-size:11.5px; color:var(--muted,#64748b); margin-left:auto; }

  /* Evaluation (AgentCore, LLM-as-judge) section */
  .obs-eval-run { margin-left:auto; font-size:11px; font-weight:700; color:#0f766e; background:#f0fdfa;
      border:1px solid #99f6e4; border-radius:7px; padding:3px 12px; cursor:pointer; }
  .obs-eval-run:hover:not(:disabled) { background:#ccfbf1; }
  .obs-eval-run:disabled { opacity:.6; cursor:default; }
  .obs-evalgrid { display:flex; flex-direction:column; gap:10px; }
  .obs-evalcard { border:1px solid #e2e8f0; border-radius:10px; padding:12px 14px; background:#fff; }
  .obs-evalhd { display:flex; align-items:center; gap:10px; margin-bottom:8px; }
  .obs-evalname { font-weight:800; font-size:13px; color:#0f172a; }
  .obs-evalscore { font-weight:800; font-size:13px; font-variant-numeric:tabular-nums; margin-left:auto; }
  .obs-evalscore.good { color:#059669; } .obs-evalscore.mid { color:#b45309; } .obs-evalscore.low { color:#be123c; }
  .obs-evallabel { font-size:11px; font-weight:700; text-transform:capitalize; color:var(--muted,#64748b);
      background:#f1f5f9; border-radius:999px; padding:2px 9px; }
  .obs-evalbar { height:8px; border-radius:999px; background:#eef2f7; overflow:hidden; }
  .obs-evalfill { display:block; height:100%; border-radius:999px; }
  .obs-evalfill.good { background:#10b981; } .obs-evalfill.mid { background:#f59e0b; } .obs-evalfill.low { background:#f43f5e; }
  .obs-evalexpl { font-size:12px; color:#334155; margin-top:8px; line-height:1.5; }

  /* Insights panel (cluster details) */
  .obs-optgrid { display:flex; flex-direction:column; gap:8px; }
  .obs-optcard { border:1px solid #e2e8f0; border-radius:10px; padding:8px 12px; background:#fff; }
  .obs-optcard > summary { cursor:pointer; font-weight:700; font-size:12px; color:#0f172a; }
  /* Clickable session ids inside an insights cluster */
  .obs-sesslink { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:11px; font-weight:700;
      color:#0f766e; text-decoration:none; background:#f0fdfa; border:1px solid #99f6e4;
      border-radius:6px; padding:1px 7px; }
  .obs-sesslink:hover { background:#ccfbf1; border-color:#5eead4; }
  .obs-sesslink-dead { color:#94a3b8; background:#f1f5f9; border:1px solid #e2e8f0; cursor:not-allowed; }
  /* Insights failure detail: subcategory -> root cause -> session -> span */
  .obs-ins-sub { margin:8px 0 0; padding:8px 0 0; border-top:1px solid #eef0f3; }
  .obs-ins-sub-h { font-size:12px; font-weight:700; color:#0f172a; }
  .obs-ins-rc { margin:8px 0 0 8px; padding:8px 10px; border-left:2px solid #e2e8f0; }
  .obs-ins-rc-h { font-size:12px; color:#0f172a; }
  .obs-ins-sess { margin:8px 0 0 8px; padding:6px 10px; background:#f8fafc; border:1px solid #eef0f3; border-radius:8px; }
  .obs-ins-span { margin:4px 0 0 8px; font-size:11px; color:#475569; }
  .obs-ins-span > code { font-size:10.5px; color:#0f766e; }
  .obs-note.warn { color:#b45309; }

  /* Notes + empty states */
  .obs-note { font-size:11.5px; color:var(--muted2,#94a3b8); margin-top:10px; line-height:1.55; }
  .obs-empty { color:var(--muted2,#94a3b8); padding:40px; text-align:center; font-size:13px; }

  /* Session combobox (dropdown + typeahead) */
  .obs-combo { position:relative; width:380px; max-width:100%; }
  .obs-combo > input { width:100%; padding-right:30px; }
  .obs-combo-caret { position:absolute; right:4px; top:50%; transform:translateY(-50%); background:transparent;
      border:0; color:var(--muted,#64748b); cursor:pointer; font-size:12px; line-height:1; padding:6px 8px; }
  .obs-combo-caret:hover { color:#0f172a; }
  .obs-combo.open .obs-combo-caret { transform:translateY(-50%) rotate(180deg); }
  .obs-combo-list { position:absolute; z-index:30; top:calc(100% + 4px); left:0; right:0; max-height:280px;
      overflow-y:auto; background:#fff; border:1px solid var(--border2,#cbd5e1); border-radius:11px;
      box-shadow:0 10px 28px #0f172a24; padding:4px; }
  .obs-combo-opt { padding:8px 11px; border-radius:8px; cursor:pointer; }
  .obs-combo-opt:hover, .obs-combo-opt.active { background:#eff6ff; }
  .obs-combo-opt .ot { font-size:13px; color:#0f172a; font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .obs-combo-opt .oid { font-family:ui-monospace,Menlo,monospace; font-size:11px; color:var(--muted2,#94a3b8); margin-top:1px; }
  .obs-combo-empty { padding:12px; font-size:12.5px; color:var(--muted2,#94a3b8); text-align:center; }

  /* Projection */
  .obs-proj { display:flex; flex-wrap:wrap; gap:22px; align-items:center; }
  .obs-proj .pcol { display:flex; flex-direction:column; gap:3px; }
  .obs-proj .pcol .pv { font-size:22px; font-weight:800; color:#0f172a; letter-spacing:-.02em; font-variant-numeric:tabular-nums; }
  .obs-proj .pcol .pl { font-size:10.5px; font-weight:700; text-transform:uppercase; letter-spacing:.04em; color:var(--muted,#64748b); }
  .obs-proj .psep { width:1px; align-self:stretch; background:var(--border,#e2e8f0); }
  .obs-proj .prunbox { display:flex; align-items:center; gap:8px; }
  .obs-proj input { width:80px; border:1px solid var(--border2,#cbd5e1); border-radius:8px; padding:7px 9px; font-size:14px;
      font-weight:700; text-align:center; }
  .obs-proj input:focus { outline:none; border-color:var(--blue,#2563eb); box-shadow:0 0 0 3px #2563eb22; }
  .obs-proj .pbig { font-size:26px; font-weight:800; color:#1e3a8a; letter-spacing:-.02em; }

  /* ======================================================================
     Cloudscape surface, applied last so it wins over the rules above.
     Kept as an override block rather than a rewrite of the 240 lines above:
     those rules carry the tab's LAYOUT, which is already right. What was wrong
     was the visual language — gradients, 8px radii, indigo accents, five
     different greys. This restates only the surfaces, the type and the palette,
     so the tab reads as an AWS console page.
     ====================================================================== */

  /* THE TYPEFACE, stated once for the whole tab.
     Cloudscape ships Open Sans as an @font-face and each component sets font-family on
     itself — it does NOT set one on <body>. This tab is plain markup, not components, so
     it inherited the browser's default serif and read as a different application sitting
     inside the same shell. Declaring it here is the fix; the value is Cloudscape's own
     fallback stack (@cloudscape-design/components, --font-family-base), copied rather
     than referenced because the custom property's name carries a build hash that changes
     between releases.
     Form controls are listed explicitly: a browser does not inherit font-family into
     input, select, button or textarea. */
  /* TWO ROOTS, not one. The Prompts & I/O inspector is a modal, and openAgentIO()
     appends it to document.body under #obsModalHost so it can sit above everything —
     which also puts it OUTSIDE #obsView. A rule scoped to #obsView alone therefore fixed
     the tab and left the modal on the browser's default serif, which is the one place a
     reader spends the most time. Both roots are named here and everywhere below. */
  #obsView, #obsModalHost,
  #obsView input, #obsView select, #obsView button, #obsView textarea,
  #obsModalHost input, #obsModalHost select, #obsModalHost button, #obsModalHost textarea {
    font-family: "Open Sans", "Helvetica Neue", Roboto, Arial, sans-serif;
  }
  /* And one monospace stack, Cloudscape's --font-family-monospace, for every place this
     tab shows code, ids or a raw prompt. There were three different ones. */
  #obsView code, #obsView pre, #obsView .cmodel, #obsView .oid,
  #obsView .obs-mchip code, #obsView .obs-sesslink,
  #obsModalHost code, #obsModalHost pre, #obsModalHost .cmodel, #obsModalHost .oid,
  #obsModalHost .obs-mchip code {
    font-family: Monaco, Menlo, Consolas, "Courier Prime", Courier, "Courier New", monospace;
  }
  /* The banner's check glyph was Georgia, then "Amazon Ember" — neither is the app's
     font. It is a letter in a circle; it should be the same letter as everywhere else. */
  #obsView .obs-banner .ck { font-family: inherit; }

  #obsView { background: #f2f3f3; }
  .obs-inner { padding: 24px 24px 60px; max-width: 1360px; }

  /* ContentLayout header, not a hero. A console page states what it is and
     moves on; the gradient tile and drop shadow were decoration. */
  .obs-hero { align-items: center; gap: 12px; margin-bottom: 20px; }
  .obs-hero .obs-hero-ic {
    width: 32px; height: 32px; border-radius: 8px; font-size: 16px;
    background: linear-gradient(135deg, #4a7fd4 0%, #0972d3 55%, #065299 100%);
    box-shadow: inset 0 0 0 1px rgba(255,255,255,.25);
  }
  .obs-hero h1 {
    font-size: 24px; line-height: 30px; font-weight: 700;
    letter-spacing: normal; color: #0f141a;
  }
  .obs-hero p { font-size: 14px; line-height: 20px; color: #424650; }

  /* Alert (info variant). */
  .obs-banner {
    background: #f2f8fd;
    border: 1px solid #b8d9f5;
    border-radius: 12px;
    padding: 12px 16px;
    gap: 12px;
  }
  .obs-banner .ck {
    width: 20px; height: 20px;
    background: #0972d3;
    font-style: normal; font-size: 12px;
    box-shadow: none;
  }
  .obs-banner .bt { font-size: 14px; line-height: 20px; color: #0f141a; }
  .obs-banner .bt .meta { color: #424650; }

  /* Container */
  .obs-card, .obs-tablewrap, .obs-kpi, .obs-optcard, .obs-evalcard,
  .obs-chart-wrap, .obs-ins-rc, .obs-ins-sess {
    background: #fff;
    border: 0;
    border-radius: 16px;
    box-shadow: 0 0 1px 1px #e9ebed, 0 1px 8px 2px rgba(0,7,22,.12);
  }
  .obs-card { padding: 20px 24px; }

  /* KPI: Cloudscape puts the label above the value, label small and grey. */
  .obs-kpi { padding: 16px 20px; }
  .obs-kpi .l { font-size: 12px; line-height: 16px; color: #656871; font-weight: 400; text-transform: none; letter-spacing: normal; }
  .obs-kpi .v, .obs-kpi .num {
    font-size: 28px; line-height: 34px; font-weight: 700;
    color: #0f141a; letter-spacing: normal;
  }
  .obs-kpi .s, .obs-kpi .meta { font-size: 12px; line-height: 16px; color: #656871; }

  /* Tabs: a bottom-border indicator, not a pill. */
  .obs-tabs { border-bottom: 1px solid #c6c6cd; gap: 0; background: transparent; padding: 0; }
  .obs-tab {
    background: transparent; border: 0; border-bottom: 2px solid transparent;
    border-radius: 0; box-shadow: none;
    padding: 10px 20px; margin-bottom: -1px;
    font-size: 14px; font-weight: 700; color: #424650; cursor: pointer;
  }
  .obs-tab:hover { color: #0972d3; background: transparent; }
  .obs-tab.active {
    background: transparent; color: #0972d3;
    border-bottom-color: #0972d3; box-shadow: none;
  }

  /* Buttons */
  .obs-btn, .obs-copy, .obs-eval-run {
    font-family: inherit; font-size: 14px; font-weight: 700;
    height: 32px; padding: 0 20px;
    border-radius: 8px;
    border: 1px solid #0972d3;
    background: transparent; color: #0972d3;
    box-shadow: none; cursor: pointer;
  }
  .obs-btn:hover:not(:disabled), .obs-copy:hover:not(:disabled), .obs-eval-run:hover:not(:disabled) { background: #f2f8fd; }
  .obs-btn:disabled, .obs-copy:disabled, .obs-eval-run:disabled {
    border-color: #c6c6cd; color: #656871; background: transparent; cursor: not-allowed; opacity: 1;
  }
  .obs-btn.primary {
    background: #0972d3; color: #fff; border-color: #0972d3;
  }
  .obs-btn.primary:hover:not(:disabled) { background: #065299; border-color: #065299; }
  .obs-btn.ghost { border-color: #c6c6cd; color: #424650; }

  /* Inputs and the session combo */
  .obs-controls select, .obs-controls input, .obs-combo, .obs-proj input, .obs-verselect {
    font-family: inherit; font-size: 14px;
    border: 1px solid #c6c6cd; border-radius: 8px;
    background: #fff; color: #0f141a;
    box-shadow: none;
  }
  .obs-controls select:focus, .obs-controls input:focus, .obs-proj input:focus, .obs-verselect:focus {
    outline: 2px solid #0972d3; outline-offset: -1px; border-color: #0972d3; box-shadow: none;
  }
  .obs-combo.open, .obs-combo-list {
    border-color: #c6c6cd; border-radius: 8px;
    box-shadow: 0 4px 20px 1px rgba(0,7,22,.1);
  }
  .obs-combo-opt { font-size: 14px; }
  .obs-combo-opt:hover, .obs-combo-opt.active { background: #f4f4f4; }

  /* Table: Cloudscape uses a light header row, row dividers, no zebra. */
  .obs-tablewrap table { font-size: 14px; }
  .obs-tablewrap th {
    background: #fbfbfb; color: #424650;
    font-size: 12px; line-height: 16px; font-weight: 700;
    text-transform: none; letter-spacing: normal;
    border-bottom: 1px solid #c6c6cd;
    padding: 10px 16px;
  }
  .obs-tablewrap td {
    border-bottom: 1px solid #e9ebed;
    padding: 10px 16px;
    color: #0f141a;
  }
  .obs-tablewrap tr:hover td { background: #f4f4f4; }
  .obs-tablewrap .num { font-variant-numeric: tabular-nums; }

  /* Badges / pills / chips */
  .obs-badge, .obs-pill, .obs-mchip, .obs-vertag, .obs-ins-span {
    border-radius: 16px;
    font-size: 12px; line-height: 16px; font-weight: 400;
    padding: 2px 10px;
    box-shadow: none;
    background: #f1f3f5; color: #424650; border: 1px solid #e1e4e8;
  }
  .obs-badge.good, .obs-badge.ok, .obs-badge.allowed, .obs-badge.passed {
    background: #eef9f1; color: #0a6b30; border-color: #c4e5d0;
  }
  .obs-badge.warn, .obs-badge.low, .obs-badge.logonly {
    background: #fbf4e6; color: #8d6605; border-color: #f0d99a;
  }
  .obs-badge.error, .obs-badge.blocked, .obs-badge.denied {
    background: #fff0f0; color: #db0000; border-color: #f5c6c6;
  }
  .obs-badge.llm, .obs-badge.tool, .obs-badge.memory, .obs-badge.guardrail,
  .obs-badge.policy, .obs-badge.eval, .obs-badge.agentcore, .obs-badge.sys {
    background: #f2f8fd; color: #0959a8; border-color: #b8d9f5;
  }

  /* Notes and helper text */
  .obs-note, .obs-empty, .obs-seg-empty, .obs-combo-empty, .obs-vernote {
    font-size: 12px; line-height: 16px; color: #656871;
  }
  .obs-note.warn { color: #8d6605; }

  /* Prompts & I/O inspector: this is where a reader spends the most time, so it
     gets the console's code surface rather than a tinted panel. */
  .obs-io, .obs-callblk, .obs-calls-inner {
    background: #fff; border-color: #e9ebed;
  }
  .obs-callttl { font-size: 13px; line-height: 18px; font-weight: 700; color: #0f141a; }
  /* EVERY code surface, including the .obs-seg pre one — the system prompt, the input
     and the output inside the inspector modal.
     It was missed, and the result was the worst of both rules: the original
     .obs-seg pre rule painted it near-black (#0f172a) as a terminal-style block, and this
     restyle set a DARK text colour for a LIGHT surface it never actually applied. The
     segments are not wrapped in .obs-io or .obs-callblk, so they kept the dark background
     and got grey text on it. A console renders code on a light surface; there is one
     surface here now. */
  .obs-io pre, .obs-callblk pre, .obs-seg pre, .obs-seg.sys pre {
    background: #f4f4f4;
    border: 1px solid #e9ebed;
    border-radius: 4px;
    font-family: Monaco, Menlo, monospace;
    font-size: 12px; line-height: 18px;
    color: #0f141a;
  }
  /* The system prompt reads differently from a response, so it keeps a tint — a lighter
     one, not an inverted one. */
  .obs-seg.sys pre { background: #fbfbfb; }
  .obs-seg pre { margin: 0 14px 14px; }
  .obs-metagrid .k, .obs-kv .k { font-size: 12px; color: #656871; }
  .obs-metagrid .v, .obs-kv .v { font-size: 14px; color: #0f141a; }

  /* Evaluation scores */
  .obs-evalbar { background: #e9ebed; border-radius: 2px; }
  .obs-evalfill { background: #0972d3; border-radius: 2px; }
  .obs-evalscore { font-weight: 700; color: #0f141a; font-variant-numeric: tabular-nums; }
  .obs-evalname { font-weight: 700; color: #0f141a; font-size: 14px; }
  .obs-evallabel, .obs-evalexpl { font-size: 12px; line-height: 16px; color: #656871; }

  /* Modal */
  .obs-modal-back { background: rgba(35,47,62,.7); }
  .obs-modal {
    border-radius: 16px;
    box-shadow: 0 4px 20px 1px rgba(0,7,22,.1);
    border: 0;
  }
  .obs-modal-head {
    background: #fbfbfb;
    border-bottom: 1px solid #e9ebed;
    font-size: 18px; line-height: 22px; font-weight: 700; color: #0f141a;
  }

  /* Links */
  .obs-sesslink { color: #0972d3; }
  .obs-sesslink:hover { text-decoration: underline; }
  .obs-sesslink-dead { color: #656871; }

  /* The projection figure */
  .obs-proj .pbig { font-size: 28px; line-height: 34px; font-weight: 700; color: #0f141a; letter-spacing: normal; }
  /* Section labels: Cloudscape Header uses sentence case at heading-s, not a small
     uppercase tracking-out label. */
  .obs-card > .ot, .obs-card > .obs-callttl, .obs-strip .ot {
    font-size: 16px; line-height: 20px; font-weight: 700;
    text-transform: none; letter-spacing: normal; color: #0f141a;
  }
  .obs-card > .ot .obs-note, .obs-strip .ot .obs-note { font-weight: 400; }

  `;
  const style = document.createElement("style"); style.textContent = CSS; document.head.appendChild(style);

  // ---- chart lifecycle -------------------------------------------------------
  const charts = {};
  /* Chart.js used to come from a <script> tag in the old index.html. The island loads it
     on demand, once, and degrades to the tables if the CDN is unreachable — they carry
     the same numbers, so a missing chart must never block the view. */
  const CHART_CDN = "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js";
  let chartLoading = null;
  function ensureChart() {
    if (window.Chart) return Promise.resolve();
    if (!chartLoading) {
      chartLoading = new Promise((resolve) => {
        const el = document.createElement("script");
        el.src = CHART_CDN;
        el.onload = () => resolve();
        el.onerror = () => resolve();
        document.head.appendChild(el);
      });
    }
    return chartLoading;
  }
  function draw(id, config) {
    const canvas = $(id);
    if (!canvas) return;
    if (!window.Chart) { void ensureChart().then(() => draw(id, config)); return; }
    if (charts[id]) charts[id].destroy();
    charts[id] = new window.Chart(canvas, config);
  }
  const PALETTE = ["#2563eb", "#7c3aed", "#059669", "#d97706", "#dc2626", "#0891b2",
                   "#db2777", "#65a30d", "#4f46e5", "#ea580c", "#0d9488", "#9333ea", "#e11d48"];

  // ---- one-time layout -------------------------------------------------------
  let built = false, tab = "overview", flowCalls = [], flowHistory = {},
      lastBuckets = [], lastBy = "date";
  function build() {
    if (built) return; built = true;
    const todayStr = etDateStr();                       // ET "today"
    const fromStr = shiftDays(todayStr, -29);           // 30-day default window
    host.innerHTML = `
     <div class="obs-view" id="obsView">
     <div class="obs-inner">
      <div class="obs-hero">
        <div class="obs-hero-ic">📊</div>
        <div>
          <h1>Observability, Cost &amp; Quality</h1>
          <p>Every model call, tool call, memory op, guardrail check, policy decision and
             compute burst captured per run — with token counts, latency, cost, the exact
             prompts and I/O, and LLM-as-judge evaluation scores you can drill into,
             aggregate, and plan against.</p>
        </div>
      </div>

      <div class="obs-banner">
        <div class="ck">i</div>
        <div class="bt"><b>${esc(LEGEND_LEAD)}</b> ${esc(LEGEND_DETAIL)}
          <span class="meta">Price book: AWS list prices, ${esc(PRICES_REGION)}, as of ${esc(PRICES_AS_OF)}.</span></div>
      </div>

      <div class="obs-tabs">
        <button class="obs-tab active" id="obsTabOverview" onclick="obsGoto('overview')">Overview</button>
        <button class="obs-tab" id="obsTabFlow" onclick="obsGoto('flow')">Run detail</button>
        <button class="obs-tab" id="obsTabInsights" onclick="obsGoto('insights')">Insights</button>
      </div>

      <section id="obsOverview">
        <div class="obs-card">
          <div class="obs-controls">
            <label>From<input type="date" id="obsFrom" value="${fromStr}"></label>
            <label>To<input type="date" id="obsTo" value="${todayStr}"></label>
            <label>Group by
              <select id="obsBy">
                <option value="date">Date</option>
                <option value="model">Model</option>
                <option value="user">User</option>
              </select>
            </label>
            <button class="obs-btn" onclick="obsLoadOverview()">Apply</button>
            <span class="grow"></span>
            <div class="obs-exports">
              <button class="obs-btn ghost" onclick="obsExport('csv')" title="Download the breakdown as CSV">⬇ CSV</button>
              <button class="obs-btn ghost" onclick="obsExport('json')" title="Download the breakdown as JSON">⬇ JSON</button>
            </div>
          </div>
        </div>
        <div class="obs-strip" id="obsKpis"></div>
        <div class="obs-card"><h3>Cost projection <span class="obs-note">planning estimate from your average run</span></h3>
          <div id="obsProj"></div></div>
        <div class="obs-card"><h3 id="obsChartTitle">Cost</h3><div class="obs-chart-wrap"><canvas id="obsChart"></canvas></div></div>
        <div class="obs-card"><h3>Breakdown</h3>
          <div class="obs-tablewrap" id="obsTable"></div>
          <div class="obs-note" id="obsLegend"></div></div>
      </section>

      <section id="obsFlow" style="display:none">
        <div class="obs-card">
          <div class="obs-controls">
            <label class="grow">Session <span class="obs-note" style="text-transform:none;font-weight:500;margin:0">select or type to filter</span>
              <div class="obs-combo" id="obsCombo">
                <input id="obsSession" autocomplete="off" placeholder="Select or type a session…">
                <button type="button" class="obs-combo-caret" id="obsComboCaret" aria-label="Toggle list">▾</button>
                <div class="obs-combo-list" id="obsComboList" style="display:none"></div>
              </div>
            </label>
            <button class="obs-btn" onclick="obsLoadFlow()">Load</button>
            <div class="obs-exports">
              <button class="obs-btn ghost" onclick="obsExportFlow('csv')" title="Download this run's calls as CSV">⬇ CSV</button>
              <button class="obs-btn ghost" onclick="obsExportFlow('json')" title="Download this run's calls as JSON">⬇ JSON</button>
            </div>
          </div>
        </div>
        <div class="obs-strip" id="obsFlowKpis"></div>
        <div class="obs-card"><h3>Model rates <span class="obs-note">USD per 1M tokens, from the price book</span></h3><div id="obsRates"></div></div>
        <div class="obs-card"><h3>Cost by agent</h3><div class="obs-chart-wrap"><canvas id="obsFlowChart"></canvas></div></div>
        <div class="obs-card"><h3>Per-agent breakdown <span class="obs-note">click a row to see individual calls, or “Prompts &amp; I/O” for the full inspector</span></h3>
          <div class="obs-tablewrap" id="obsFlowTable"></div>
          <div class="obs-note" id="obsFlowLegend"></div></div>
      </section>

      <section id="obsInsights" style="display:none">
        <div class="obs-card">
          <div class="obs-controls">
            <div class="grow"><h3 style="margin:0">Cross-run insights <span class="obs-note">AgentCore batch analysis over recent runs</span></h3>
              <div class="obs-note" style="margin:4px 0 0">Failure-pattern, user-intent and behavior analysis across ALL runs in the window.
                Runs a batch evaluation over the runtime's CloudWatch traces — takes a few minutes.
                Requires Transaction Search (provisioned by <code>terraform/transaction_search.tf</code>).</div></div>
            <label>Window
              <select id="obsInsLookback">
                <option value="1">Last 1 hour</option>
                <option value="3">Last 3 hours</option>
                <option value="6">Last 6 hours</option>
                <option value="12">Last 12 hours</option>
                <option value="24">Last 24 hours</option>
                <option value="72">Last 3 days</option>
                <option value="168" selected>Last 7 days</option>
                <option value="720">Last 30 days</option>
              </select>
            </label>
            <button class="obs-btn"${authzGate("insights")} onclick="obsRunInsights()">Run insights</button>
            <button class="obs-btn ghost" onclick="obsLoadInsights()">Refresh</button>
          </div>
        </div>
        <div class="obs-card"><div id="obsInsightsBody"><div class="obs-note" style="margin:0">Loading…</div></div></div>
      </section>
     </div>
     </div>`;
    wireCombo();
  }

  // ---- view toggle -----------------------------------------------------------
  /* Kept because the module's own inline handlers reference them, but they no longer
     show or hide anything: which VIEW is on screen is the React shell's decision, and a
     module reaching for document.querySelector("main") to hide it is exactly what an
     island must not do. showObs now only (re)builds and selects the inner tab. */
  function showObs() {
    build();
    goto(tab);
  }
  function showCampaigns() { /* the shell navigates; nothing to do here */ }

  /* The island's entry point. Called by src/views/Observability.tsx with the element to
     render into, the IdP token to call the API with, and the projected workflow
     definition (agent names + evalAgents). Idempotent: mounting twice re-renders into
     the new host rather than duplicating anything. */
  function mount(el, token, wf, opts) {
    host = el;
    authToken = token || null;
    wfDef = wf || {};
    buildId = (opts && opts.build) || "";
    built = false;
    build();
    goto(tab);
  }
  function goto(t) {
    tab = t;
    $("obsTabOverview").classList.toggle("active", t === "overview");
    $("obsTabFlow").classList.toggle("active", t === "flow");
    if ($("obsTabInsights")) $("obsTabInsights").classList.toggle("active", t === "insights");
    $("obsOverview").style.display = t === "overview" ? "" : "none";
    $("obsFlow").style.display = t === "flow" ? "" : "none";
    if ($("obsInsights")) $("obsInsights").style.display = t === "insights" ? "" : "none";
    if (t === "overview") loadOverview();
    else if (t === "insights") loadInsights();
    else loadSessions();
  }

  // ---- overview --------------------------------------------------------------
  async function loadOverview() {
    const by = $("obsBy").value, from = $("obsFrom").value, to = $("obsTo").value;
    let data;
    try { data = await api(`/api/telemetry/aggregate?by=${by}&from=${from}&to=${to}`); }
    catch (e) { $("obsTable").innerHTML = `<div class="obs-empty">Couldn't load telemetry: ${esc(e.message)}</div>`; return; }
    const buckets = data.buckets || [];
    lastBuckets = buckets; lastBy = by;
    const tot = buckets.reduce((a, b) => ({
      cost: a.cost + Number(b.costUsd || 0), inp: a.inp + Number(b.inputTokens || 0),
      out: a.out + Number(b.outputTokens || 0), calls: a.calls + Number(b.calls || 0),
    }), { cost: 0, inp: 0, out: 0, calls: 0 });
    const runs = Number(data.totalSessions || 0);

    const bucketLabel = { date: "Active days", model: "Models used", user: "Users" }[by];
    $("obsKpis").innerHTML = kpi(fmtUsd(tot.cost), "Total cost", "#2563eb")
      + kpi(fmtInt(runs), "Runs", "#7c3aed")
      + kpi(fmtInt(tot.inp + tot.out), "Total tokens", "#0891b2",
            fmtInt(tot.inp) + " in · " + fmtInt(tot.out) + " out")
      + kpi(fmtInt(tot.calls), "Calls", "#059669")
      + kpi(buckets.length, bucketLabel, "#d97706");

    renderProjection(tot.cost, runs);

    $("obsChartTitle").textContent = "Cost by " + by;
    if (!buckets.length) { $("obsTable").innerHTML = `<div class="obs-empty">No telemetry in this range yet. Run a session, then come back.</div>`; if (charts.obsChart) charts.obsChart.destroy(); return; }

    const labels = buckets.map((b) => String(b.key));
    const costs = buckets.map((b) => Number(b.costUsd || 0));
    const asBar = by === "date";
    draw("obsChart", {
      type: asBar ? "bar" : "doughnut",
      data: {
        labels,
        datasets: [{
          label: "Cost (USD)", data: costs,
          backgroundColor: asBar ? "#2563eb" : labels.map((_, i) => PALETTE[i % PALETTE.length]),
          borderRadius: asBar ? 6 : 0,
        }],
      },
      options: {
        maintainAspectRatio: false,
        plugins: { legend: { display: !asBar, position: "right" },
          tooltip: { callbacks: { label: (c) => " " + fmtUsd(c.parsed.y ?? c.parsed) } } },
        scales: asBar ? { y: { ticks: { callback: (v) => "$" + v } } } : {},
      },
    });

    // In the by-model view, surface the per-1M rates that priced each model so
    // the customer can see the input/output unit price alongside the totals.
    const rateCols = by === "model";
    const rateCell = (v) => Number(v || 0) ? "$" + Number(v).toFixed(2) : "—";
    $("obsTable").innerHTML = `<table class="obs"><thead><tr>
      <th>${esc(by[0].toUpperCase() + by.slice(1))}</th><th class="num">Cost</th>
      <th class="num">Input tok</th><th class="num">Output tok</th>${
      rateCols ? '<th class="num">In $/M</th><th class="num">Out $/M</th>' : ""}
      <th class="num">Calls</th><th class="num">Latency</th></tr></thead><tbody>${
      buckets.map((b) => `<tr><td>${esc(String(b.key))}</td>
        <td class="num">${fmtUsd(b.costUsd)}</td>
        <td class="num">${fmtInt(b.inputTokens)}</td>
        <td class="num">${fmtInt(b.outputTokens)}</td>${
        rateCols ? `<td class="num">${rateCell(b.inRate)}</td><td class="num">${rateCell(b.outRate)}</td>` : ""}
        <td class="num">${fmtInt(b.calls)}</td>
        <td class="num">${fmtMs(b.latencyMs)}</td></tr>`).join("")}</tbody></table>`;
    $("obsLegend").innerHTML = LEGEND;
  }

  // ---- run detail ------------------------------------------------------------
  let sessionMap = {};        // "topic · id" -> id  (for resolveSessionId)
  let sessionsCache = [];     // [{session_id, topic, disp}]
  async function loadSessions() {
    let sessions = [];
    try { sessions = await api("/api/sessions"); } catch (e) { /* ignore */ }
    sessionMap = {};
    sessionsCache = (sessions || []).map((s) => {
      const disp = `${s.topic || s.session_id} · ${s.session_id}`;
      sessionMap[disp] = s.session_id;
      return { session_id: s.session_id, topic: s.topic || s.session_id, disp };
    });
    renderCombo("");
    const inp = $("obsSession");
    const cur = window.current || "";
    if (cur && !inp.value) {
      const match = sessionsCache.find((s) => s.session_id === cur);
      if (match) { inp.value = match.disp; loadFlow(); }
    } else if (inp.value) {
      loadFlow();
    }
  }

  // ---- combobox (dropdown + typeahead) --------------------------------------
  function renderCombo(filter) {
    const q = (filter || "").toLowerCase().trim();
    const list = q
      ? sessionsCache.filter((s) => `${s.topic} ${s.session_id}`.toLowerCase().includes(q))
      : sessionsCache;
    const box = $("obsComboList");
    if (!box) return;
    if (!sessionsCache.length) { box.innerHTML = `<div class="obs-combo-empty">No sessions yet — run one first.</div>`; return; }
    if (!list.length) { box.innerHTML = `<div class="obs-combo-empty">No match for "${esc(filter)}"</div>`; return; }
    box.innerHTML = list.map((s) =>
      `<div class="obs-combo-opt" data-id="${esc(s.session_id)}" data-disp="${esc(s.disp)}">
         <div class="ot">${esc(s.topic)}</div><div class="oid">${esc(s.session_id)}</div></div>`).join("");
    // mousedown (not click) so the pick fires before the input's blur closes the list
    box.querySelectorAll(".obs-combo-opt").forEach((el) => {
      el.addEventListener("mousedown", (e) => {
        e.preventDefault();
        pickSession(el.dataset.disp, el.dataset.id);
      });
    });
  }
  function openCombo() { renderCombo($("obsSession").value); $("obsComboList").style.display = ""; $("obsCombo").classList.add("open"); }
  function closeCombo() { const b = $("obsComboList"); if (b) b.style.display = "none"; const c = $("obsCombo"); if (c) c.classList.remove("open"); }
  function pickSession(disp, id) {
    const inp = $("obsSession");
    inp.value = disp; sessionMap[disp] = id;
    closeCombo(); loadFlow();
  }
  function wireCombo() {
    const inp = $("obsSession"), caret = $("obsComboCaret");
    if (!inp || !caret) return;
    inp.addEventListener("focus", openCombo);
    inp.addEventListener("input", () => { renderCombo(inp.value); openCombo(); });
    inp.addEventListener("blur", () => setTimeout(closeCombo, 150));
    inp.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { closeCombo(); loadFlow(); }
      else if (e.key === "Escape") { closeCombo(); }
    });
    caret.addEventListener("mousedown", (e) => {
      e.preventDefault();
      if ($("obsCombo").classList.contains("open")) closeCombo();
      else { inp.focus(); openCombo(); }
    });
  }

  function resolveSessionId() {
    const raw = ($("obsSession").value || "").trim();
    if (sessionMap[raw]) return sessionMap[raw];          // picked from list
    const m = raw.match(/([0-9a-f]{6,})\s*$/i);           // "…· id" or a raw id
    return m ? m[1] : raw;
  }

  async function loadFlow() {
    const sid = resolveSessionId();
    if (!sid) return;
    // The shell's Run Assistant answers "this run" with the run shown here.
    window.dispatchEvent(new CustomEvent("obs:session", { detail: sid }));
    let data;
    try { data = await api(`/api/sessions/${sid}/telemetry`); }
    catch (e) { $("obsFlowTable").innerHTML = `<div class="obs-empty">Couldn't load: ${esc(e.message)}</div>`; return; }
    flowCalls = data.calls || [];
    // Also pull the session snapshot so the per-agent inspector can show the human
    // feedback that triggered each re-run (that lives in snapshot.history[agent],
    // not in telemetry). Best-effort: the flow still renders if this fails.
    try { const snap = await api(`/api/sessions/${sid}`); flowHistory = (snap && snap.history) || {}; }
    catch (e) { flowHistory = {}; }
    renderRates(flowCalls);
    const t = data.totals || {};
    // When this run happened: derive the window from the telemetry timestamps.
    const times = flowCalls.map((c) => String(c.ts || "")).filter(Boolean).sort();
    let ranTile = "";
    if (times.length) {
      const start = shortTime(times[0]);
      const endT = shortTime(times[times.length - 1]).split(" ")[1] || "";
      const day = start.split(" ")[0], startT = start.split(" ")[1] || "";
      ranTile = kpi(day, "Run date", "#475569",
                    startT + (endT && endT !== startT ? " → " + endT : "") + " ET");
    }
    $("obsFlowKpis").innerHTML = ranTile
      + kpi(fmtUsd(t.costUsd), "Run cost", "#2563eb")
      + kpi(fmtInt((Number(t.inputTokens || 0) + Number(t.outputTokens || 0))), "Tokens", "#0891b2",
            fmtInt(t.inputTokens) + " in · " + fmtInt(t.outputTokens) + " out")
      + kpi(fmtInt(t.calls), "Calls", "#059669")
      + kpi(fmtMs(t.latencyMs), "Model+tool latency", "#d97706");

    const agents = (data.byAgent || []).slice().sort((a, b) => Number(b.costUsd || 0) - Number(a.costUsd || 0));
    if (!agents.length) { $("obsFlowTable").innerHTML = `<div class="obs-empty">No telemetry for this session yet.</div>`; if (charts.obsFlowChart) charts.obsFlowChart.destroy(); return; }

    draw("obsFlowChart", {
      type: "bar",
      data: {
        labels: agents.map((a) => a.agentId),
        datasets: [{ label: "Cost (USD)", data: agents.map((a) => Number(a.costUsd || 0)),
          backgroundColor: "#7c3aed", borderRadius: 6 }],
      },
      options: {
        indexAxis: "y", maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => " " + fmtUsd(c.parsed.x) } } },
        scales: { x: { ticks: { callback: (v) => "$" + v } } },
      },
    });

    $("obsFlowTable").innerHTML = `<table class="obs"><thead><tr>
      <th>Agent</th><th>Model(s)</th><th class="num">Input</th><th class="num">Output</th>
      <th class="num">Tools</th><th class="num">Latency</th><th class="num">Cost</th></tr></thead><tbody>${
      agents.map((a) => `
        <tr class="obs-agent" onclick="obsToggle(${jsq(a.agentId)})">
          <td><span class="caret" id="caret-${esc(a.agentId)}">▸</span> ${esc(friendlyAgent(a.agentId))}${
            a.agentId === "__session__" ? "" :
            `<button class="obs-io" onclick="event.stopPropagation();obsAgentIO(${jsq(a.agentId)})" title="View this agent's system prompt, input, output and evaluation scores">⤢ Prompts &amp; I/O</button>`}</td>
          <td>${(a.models || []).map((m) => esc(shortModel(m))).join(", ") || "—"}</td>
          <td class="num">${fmtInt(a.inputTokens)}</td>
          <td class="num">${fmtInt(a.outputTokens)}</td>
          <td class="num">${fmtInt(a.tools)}</td>
          <td class="num">${fmtMs(a.latencyMs)}</td>
          <td class="num">${fmtUsd(a.costUsd)}</td>
        </tr>
        <tr class="obs-calls" id="calls-${esc(a.agentId)}" style="display:none"><td colspan="7">
          <div class="obs-calls-inner" id="calls-inner-${esc(a.agentId)}"></div></td></tr>`).join("")}</tbody></table>`;
    $("obsFlowLegend").innerHTML = LEGEND;
  }

  function renderRates(calls) {
    const seen = {};
    (calls || []).forEach((c) => {
      if (c.kind === "llm" && c.label && !(c.label in seen)) {
        seen[c.label] = { in: Number(c.in_rate || 0), out: Number(c.out_rate || 0) };
      }
    });
    const models = Object.keys(seen);
    $("obsRates").innerHTML = models.length ? `<table class="obs"><thead><tr>
      <th>Model</th><th class="num">Input $/1M tokens</th><th class="num">Output $/1M tokens</th></tr></thead><tbody>${
      models.map((m) => `<tr><td>${esc(shortModel(m))}</td>
        <td class="num">$${seen[m].in.toFixed(2)}</td>
        <td class="num">$${seen[m].out.toFixed(2)}</td></tr>`).join("")}</tbody></table>`
      : `<div class="obs-note">No model calls in this session.</div>`;
  }

  function toggleAgent(agentId) {
    const row = $("calls-" + agentId), caret = $("caret-" + agentId);
    if (!row) return;
    const open = row.style.display !== "none";
    row.style.display = open ? "none" : "";
    if (caret) caret.textContent = open ? "▸" : "▾";
    if (!open) {
      // Chronological within the agent. `sk` (and `ts`) are ET strings now
      // ("YYYY-MM-DD HH:MM:SS…"), which sort lexicographically = chronologically;
      // legacy rows have a numeric sk that also sorts correctly among themselves.
      const calls = flowCalls.filter((c) => c.agent_id === agentId)
        .sort((a, b) => String(a.sk || a.ts || "").localeCompare(String(b.sk || b.ts || "")));
      // A rate is "$x.xx" when it is a real price and "~$x.xx" when pricing.py did not
      // recognise the model and fell back to a guess (rates_known === false). The
      // fallback used to be invisible, so a customer on a model the price table has
      // never heard of read fabricated cost figures as measured ones.
      // The tooltip names what priced the row (rates_source): orchestrator.modelRates,
      // the AWS Price List for this region, the built-in snapshot, or the fallback.
      const RATE_SOURCE = {
        config: "from orchestrator.modelRates in workflow.json",
        pricelist: "from the AWS Price List for this region (public on-demand price)",
        table: "from the built-in price snapshot (the AWS Price List was unavailable or has no row for this model)",
      };
      const rate = (c, k) => {
        if (c.kind !== "llm" || !Number(c[k] || 0)) return "—";
        const guessed = c.rates_known === false;
        const text = (guessed ? "~$" : "$") + Number(c[k]).toFixed(2);
        const why = guessed
          ? "estimated: no rate found for this model — set orchestrator.modelRates in workflow.json"
          : RATE_SOURCE[c.rates_source] || "";
        return why ? `<span title="${esc(why)}">${text}</span>` : text;
      };
      // System-prompt tokens: exact via Bedrock CountTokens; a "~" marks the rare
      // estimate fallback.
      const sysCell = (c) => {
        if (c.kind !== "llm") return "—";
        return (c.system_tokens_exact === false ? "~" : "") + fmtInt(c.system_tokens);
      };
      // Input column: LLM rows show exact model input tokens; tool rows have no
      // model input, but a KB tool row embeds its query (Titan) — an estimate we
      // show with a ~ marker so it is never confused with the exact model count.
      const inputCell = (c) => {
        if (c.kind === "llm") return fmtInt(c.input_tokens);
        if (Number(c.embed_tokens_est || 0)) return `<span title="estimated embedding tokens">~${fmtInt(c.embed_tokens_est)} embed</span>`;
        return "—";
      };
      $("calls-inner-" + agentId).innerHTML = calls.length ? `<table class="obs"><thead><tr>
        <th>Kind</th><th>Model / provider</th><th class="num">Input</th><th class="num">Output</th>
        <th class="num" title="The agent's system-prompt share of the input tokens — exact, via the Bedrock CountTokens API. A subset of Input. A leading ~ marks a rare estimate fallback.">Sys prompt</th>
        <th class="num">In $/M</th><th class="num">Out $/M</th>
        <th class="num">Latency</th><th class="num">Cost</th><th>Result</th></tr></thead><tbody>${
        calls.map((c) => `<tr>
          <td><span class="obs-pill ${esc(c.kind)}">${esc(c.kind)}</span></td>
          <td>${esc(providerLabel(c))}</td>
          <td class="num">${inputCell(c)}</td>
          <td class="num">${c.kind === "llm" ? fmtInt(c.output_tokens) : "—"}</td>
          <td class="num">${sysCell(c)}</td>
          <td class="num">${rate(c, "in_rate")}</td>
          <td class="num">${rate(c, "out_rate")}</td>
          <td class="num">${fmtMs(c.latency_ms)}</td>
          <td class="num">${fmtUsd(c.cost_usd)}</td>
          <td>${resultCell(c)}</td>
        </tr>`).join("")}</tbody></table>
        <div class="obs-note">${LEGEND}</div>` : `<div class="obs-note">No calls recorded.</div>`;
    }
  }

  // Rightmost column: how the call resolved. LLM/tool rows show the access mode
  // (bedrock / gateway / denied); the feature rows show their own outcome
  // (passed|blocked, allowed|denied|log-only, an eval score). There is no
  // "simulated" mode — an unreachable tool raises ToolUnavailable and the run fails.
  function resultCell(c) {
    if (c.kind === "eval") {
      const v = Number(c.value || 0);
      return String(c.status || "") === "error"
        ? '<span class="obs-badge error">error</span>'
        : `<span class="obs-evalscore ${scoreClass(v)}">${v.toFixed(2)}</span>`;
    }
    if (c.kind === "guardrail" || c.kind === "policy") {
      const st = String(c.status || "").toLowerCase();
      const cls = { blocked: "blocked", denied: "denied", passed: "passed",
                    allowed: "allowed", "log-only": "logonly" }[st] || "ok";
      return `<span class="obs-badge ${cls}">${esc(st || "ok")}</span>`;
    }
    if (c.mode === "denied") return '<span class="obs-badge denied">denied</span>';
    return esc(c.mode);
  }

  function shortModel(m) {
    m = String(m || "");
    return m.replace(/^us\.anthropic\./, "").replace(/-\d{8}-v\d.*$/, "").replace(/:0$/, "");
  }
  // "__session__" is the synthetic bucket for AgentCore Runtime compute (not a
  // real agent, and not a model call — so it has no tokens, only a compute cost).
  function friendlyAgent(id) {
    if (id === "__session__") return "AgentCore Runtime · compute";
    const meta = (wfDef.agents || {})[id];
    return meta && meta.name ? `${meta.name} (${id})` : id;
  }
  // What the "Model / provider" column shows, per telemetry kind.
  function providerLabel(c) {
    switch (c.kind) {
      case "agentcore": return "AgentCore Runtime — compute (vCPU + GB-hours)";
      case "memory":    return `Long-term memory · ${c.label || "?"}${c.namespace ? ` (${c.namespace})` : ""}`;
      case "guardrail": return `Bedrock Guardrails · ${String(c.label || "").toUpperCase()}`;
      case "policy":    return `Cedar policy · ${c.label || "?"}`;
      case "eval":      return `Evaluator · ${String(c.label || "").replace(/^Builtin\./, "")}`;
      default:          return shortModel(c.label);
    }
  }
  function kpi(v, l, accent, sub) {
    const a = accent ? ` style="--kpi-accent:${accent}"` : "";
    const s = sub ? `<div class="s">${esc(String(sub))}</div>` : "";
    return `<div class="obs-kpi"${a}><div class="v">${esc(String(v))}</div><div class="l">${esc(l)}</div>${s}</div>`;
  }

  // ---- cost projection -------------------------------------------------------
  const DEFAULT_RUNS_PER_DAY = 10;
  const PROJECTION_DAYS = 30;
  let projAvg = 0;
  const monthly = (perDay) => projAvg * perDay * PROJECTION_DAYS;
  function renderProjection(totalCost, runs) {
    projAvg = runs > 0 ? totalCost / runs : 0;
    const el = $("obsProj");
    if (!el) return;
    if (!runs) { el.innerHTML = `<div class="obs-note" style="margin:0">Run a session to see per-run cost and a monthly projection.</div>`; return; }
    const perDay = Number(($("obsRunsPerDay") && $("obsRunsPerDay").value) || DEFAULT_RUNS_PER_DAY);
    el.innerHTML = `
      <div class="obs-proj">
        <div class="pcol"><span class="pv">${fmtUsd(projAvg)}</span><span class="pl">Avg cost / run</span></div>
        <div class="psep"></div>
        <div class="pcol"><span class="pv">${fmtInt(runs)}</span><span class="pl">Runs measured</span></div>
        <div class="psep"></div>
        <div class="pcol">
          <div class="prunbox"><input id="obsRunsPerDay" type="number" min="0" step="1" value="${perDay}" oninput="obsReproject()"> <span class="pl" style="text-transform:none">runs / day</span></div>
          <span class="pl">Planned volume</span>
        </div>
        <div class="psep"></div>
        <div class="pcol"><span class="pbig" id="obsProjMonthly">${fmtUsd(monthly(perDay))}</span><span class="pl">Projected / month (30 days)</span></div>
      </div>`;
  }
  function reproject() {
    // Same default as renderProjection: these disagreed (10 vs 0), so clearing the
    // input dropped the projection to $0.00 instead of back to the rendered figure.
    const perDay = Number(($("obsRunsPerDay") && $("obsRunsPerDay").value) || DEFAULT_RUNS_PER_DAY);
    const m = $("obsProjMonthly");
    if (m) m.textContent = fmtUsd(monthly(perDay));
  }

  // ---- export (client-side download) -----------------------------------------
  function download(name, text, type) {
    const blob = new Blob([text], { type });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob); a.download = name;
    document.body.appendChild(a); a.click();
    setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 0);
  }
  function toCsv(rows) {
    const cell = (v) => {
      const s = String(v == null ? "" : v);
      return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    return rows.map((r) => r.map(cell).join(",")).join("\r\n");
  }

  function exportOverview(fmt) {
    if (!lastBuckets.length) return;
    if (fmt === "json") {
      download(`observability-${lastBy}-${etStamp()}.json`,
        JSON.stringify({ groupedBy: lastBy, buckets: lastBuckets }, null, 2), "application/json");
      return;
    }
    const head = [lastBy, "costUsd", "inputTokens", "outputTokens", "calls", "runs", "latencyMs"];
    if (lastBy === "model") head.push("inRatePerM", "outRatePerM");
    const rows = [head].concat(lastBuckets.map((b) => {
      const r = [b.key, b.costUsd, b.inputTokens, b.outputTokens, b.calls, b.sessions, b.latencyMs];
      if (lastBy === "model") r.push(b.inRate, b.outRate);
      return r;
    }));
    download(`observability-${lastBy}-${etStamp()}.csv`, toCsv(rows), "text/csv");
  }

  function exportFlow(fmt) {
    const sid = resolveSessionId();
    if (!flowCalls.length) return;
    if (fmt === "json") {
      download(`run-${sid}-${etStamp()}.json`, JSON.stringify(flowCalls, null, 2), "application/json");
      return;
    }
    const head = ["agent_id", "kind", "label", "mode", "status", "version", "prompt",
      "input_tokens", "output_tokens", "system_tokens", "system_tokens_exact",
      "embed_tokens_est", "in_rate", "out_rate", "rates_known", "rates_source", "latency_ms", "cost_usd", "value",
      "eval_label", "namespace", "finish_reason", "ts"];
    // embed_tokens_est = estimated KB-query embedding tokens (tool rows);
    // value = the evaluator score (eval rows); namespace = memory rows.
    const rows = [head].concat(flowCalls.map((c) => head.map((k) => c[k])));
    download(`run-${sid}-${etStamp()}.csv`, toCsv(rows), "text/csv");
  }

  // ---- per-agent "Prompts & I/O" inspector -----------------------------------
  // Shows, for one agent and one run version, every model call's system prompt,
  // input and output (captured into the telemetry rows), plus tool queries, memory
  // reads/writes, guardrail checks, policy decisions, and the AgentCore
  // Evaluations scores per named prompt — so you can see exactly what each agent
  // was told, what it produced, and how it scored. Data comes from the
  // already-loaded `flowCalls`; no extra fetch.

  const seg = (cls, title, text, count) => {
    const body = (text && String(text).trim())
      ? `<pre>${esc(text)}</pre>`
      : `<div class="obs-seg-empty">Not captured for this call (an older run, a tool/compute row, or content dropped on a cold-start container).</div>`;
    const cc = count != null ? `<span class="cc">${esc(count)}</span>` : "";
    const copy = (text && String(text).trim())
      ? `<button class="obs-copy" onclick="event.preventDefault();obsCopy(this)">copy</button>` : "";
    return `<details class="obs-seg ${cls}" open><summary><span class="tw">▸</span>${esc(title)}${copy}${cc}</summary>${body}</details>`;
  };

  const badge = (status) => {
    const s = status || "ok";
    return `<span class="obs-badge ${esc(s)}">${esc(s)}</span>`;
  };
  const kv = (k, val) => `<div class="obs-kv"><div class="k">${esc(k)}</div><div class="val">${val}</div></div>`;
  // Short local time from an ET timestamp string ("YYYY-MM-DD HH:MM:SS…").
  // Full "YYYY-MM-DD HH:MM:SS" when the row carries a date; falls back to
  // time-only, then a dash.
  const shortTime = (ts) => {
    const s = String(ts || "");
    const dt = s.match(/(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/);
    if (dt) return dt[1] + " " + dt[2];
    const t = s.match(/\d{2}:\d{2}:\d{2}/);
    return t ? t[0] : "—";
  };

  // Build the per-kind call blocks (model / tool / memory / guardrail / policy)
  // for a subset of calls. Reused per version so a re-run's calls render under
  // their own "Version N" header.
  function callBlocks(calls) {
    const llm = calls.filter((c) => c.kind === "llm");
    const tools = calls.filter((c) => c.kind === "tool");
    const mems = calls.filter((c) => c.kind === "memory");
    const guards = calls.filter((c) => c.kind === "guardrail");
    const pols = calls.filter((c) => c.kind === "policy");

    // ---- one model call ----
    let blocks = llm.map((c, i) => {
      const temp = c.temperature != null ? Number(c.temperature) : null;
      const meta = `<div class="obs-metagrid">
        ${kv("Input tokens", fmtInt(c.input_tokens) + ` <small>${fmtInt(c.system_tokens)} sys</small>`)}
        ${kv("Output tokens", fmtInt(c.output_tokens))}
        ${kv("Temperature", temp == null ? "—" : esc(String(temp)))}
        ${kv("Max tokens", c.max_tokens ? fmtInt(c.max_tokens) : "—")}
        ${kv("Finish", c.finish_reason ? esc(c.finish_reason) : "—")}
        ${kv("Latency", fmtMs(c.latency_ms))}
        ${kv("Cost", fmtUsd(c.cost_usd))}
        ${kv("Time", esc(shortTime(c.ts)))}
      </div>`;
      const pname = String(c.prompt || "").trim();
      return `<div class="obs-callblk">
        <div class="obs-callttl"><span class="cn">Model call ${i + 1}</span>
          ${pname ? `<span class="obs-mchip"><code>${esc(pname)}</code></span>` : ""}
          <span class="cmodel">${esc(shortModel(c.label))}</span>${badge(c.status)}</div>
        ${meta}
        ${seg("sys", "System prompt", c.system_prompt)}
        ${seg("in", "Input", c.user_input)}
        ${seg("out", "Output", c.output_text)}</div>`;
    }).join("");

    // ---- tool / KB calls ----
    if (tools.length) {
      blocks += `<div class="obs-mchips" style="margin:18px 0 8px"><span class="obs-mchip"><b>Tool &amp; knowledge-base calls</b></span></div>`;
      blocks += tools.map((c, i) => {
        const meta = `<div class="obs-metagrid">
          ${kv("Provider", esc(c.label || "—"))}
          ${kv("Embed tokens", Number(c.embed_tokens_est || 0) ? "~" + fmtInt(c.embed_tokens_est) : "—")}
          ${kv("Latency", fmtMs(c.latency_ms))}
          ${kv("Cost", fmtUsd(c.cost_usd))}
          ${kv("Time", esc(shortTime(c.ts)))}
        </div>`;
        return `<div class="obs-callblk">
          <div class="obs-callttl"><span class="cn">Tool call ${i + 1}</span>
            <span class="cmodel">${esc(providerLabel(c))}</span>${badge(c.status)}</div>
          ${meta}
          ${seg("in", "Query", c.user_input)}
          ${seg("out", "Retrieved context", c.output_text)}</div>`;
      }).join("");
    }

    // ---- long-term memory reads / writes ----
    if (mems.length) {
      blocks += `<div class="obs-mchips" style="margin:18px 0 8px"><span class="obs-mchip"><b>Long-term memory</b> (recall / store)</span></div>`;
      blocks += mems.map((c) => {
        const isRecall = String(c.label || "").toLowerCase() === "recall";
        const meta = `<div class="obs-metagrid">
          ${kv("Operation", isRecall ? "Recall (read)" : "Store (write)")}
          ${kv("Namespace", esc(c.namespace || "—"))}
          ${kv("Latency", fmtMs(c.latency_ms))}
          ${kv("Time", esc(shortTime(c.ts)))}
        </div>`;
        const body = isRecall
          ? seg("in", "Recall query", c.user_input) + seg("out", "Recalled insights", c.output_text)
          : seg("out", "Stored insight", c.output_text);
        return `<div class="obs-callblk">
          <div class="obs-callttl"><span class="cn">${isRecall ? "Recall" : "Store"}</span>
            <span class="cmodel">${esc(c.namespace || "")}</span>
            <span class="obs-badge agentcore">memory</span></div>
          ${meta}${body}</div>`;
      }).join("");
    }

    // ---- guardrail checks (input / output) ----
    if (guards.length) {
      blocks += `<div class="obs-mchips" style="margin:18px 0 8px"><span class="obs-mchip"><b>Guardrails</b> (content safety)</span></div>`;
      blocks += guards.map((c) => {
        const st = String(c.status || "").toLowerCase();
        const src = String(c.label || "").toUpperCase();
        const meta = `<div class="obs-metagrid">
          ${kv("Check", src === "INPUT" ? "Input (before LLM)" : "Output (after LLM)")}
          ${kv("Result", st === "blocked" ? "Blocked" : "Passed")}
          ${kv("Latency", fmtMs(c.latency_ms))}
          ${kv("Time", esc(shortTime(c.ts)))}
        </div>`;
        const detail = st === "blocked" ? seg("out", "Guardrail message", c.output_text) : "";
        return `<div class="obs-callblk">
          <div class="obs-callttl"><span class="cn">Guardrail — ${esc(src)}</span>
            <span class="obs-badge ${st === "blocked" ? "blocked" : "passed"}">${esc(st || "passed")}</span></div>
          ${meta}${detail}</div>`;
      }).join("");
    }

    // ---- policy (Cedar authorization at the Gateway) ----
    if (pols.length) {
      blocks += `<div class="obs-mchips" style="margin:18px 0 8px"><span class="obs-mchip"><b>Policy</b> (Cedar authorization at the Gateway)</span></div>`;
      blocks += pols.map((c) => {
        const st = String(c.status || "").toLowerCase(); // allowed | denied | log-only
        const badgeCls = st === "denied" ? "denied" : (st === "allowed" ? "allowed" : "logonly");
        const meta = `<div class="obs-metagrid">
          ${kv("Tool", esc(c.label || "—"))}
          ${kv("Parameter", esc(c.user_input || "—"))}
          ${kv("Mode", esc(String(c.mode || "").toUpperCase()))}
          ${kv("Latency", fmtMs(c.latency_ms))}
          ${kv("Time", esc(shortTime(c.ts)))}
        </div>`;
        return `<div class="obs-callblk">
          <div class="obs-callttl"><span class="cn">Policy check</span>
            <span class="cmodel">${esc(c.label || "")}</span>
            <span class="obs-badge ${badgeCls}">${esc(st || "n/a")}</span></div>
          ${meta}
          ${seg("out", "Decision detail", c.output_text)}</div>`;
      }).join("");
    }

    return blocks;
  }

  // Normalise a call's version: untagged rows (from a run before version tagging)
  // are treated as Version 1, so the first run shows as "Version 1" rather than a
  // confusing "Version 0".
  const callVersion = (c) => { const v = Number(c.version || 0); return v > 0 ? v : 1; };

  // Versions we can actually SHOW = the distinct versions present in telemetry.
  // We deliberately do NOT add versions that only exist in history: a run from
  // before version tagging has all its calls in one bucket, so offering a
  // "Version N" the telemetry can't back would just show an empty pane.
  function agentVersions(agentId) {
    const set = new Set();
    flowCalls.forEach((c) => { if (c.agent_id === agentId) set.add(callVersion(c)); });
    // No filter needed: callVersion() already maps 0/absent to 1.
    const arr = [...set].sort((a, b) => a - b);
    return arr.length ? arr : [1];
  }

  const versionHeader = (agentId, v, versions) => {
    const h = (flowHistory[agentId] || []).find((x) => Number(x.version) === Number(v));
    const fb = h && h.comment ? h.comment : "";
    const isLatest = v === versions[versions.length - 1];
    const tag = `Version ${esc(String(v))}${Number(v) === 1 ? " · initial" : ""}${isLatest && versions.length > 1 ? " · latest" : ""}`;
    return `<div class="obs-verhead">
      <span class="obs-vertag">${tag}</span>
      ${fb ? `<div class="obs-verfb"><b>Human feedback (triggered this version):</b> ${esc(fb)}</div>`
           : `<span class="obs-vernote">${Number(v) === 1 ? "Initial run — no reviewer feedback" : "Re-run — no feedback recorded"}</span>`}
    </div>`;
  };

  // Chips + version header + call blocks + evaluation, for ONE selected version.
  function agentIOBody(agentId, sel) {
    const versions = agentVersions(agentId);
    if (!versions.includes(sel)) sel = versions[versions.length - 1];
    const calls = flowCalls
      .filter((c) => c.agent_id === agentId && callVersion(c) === sel)
      .sort((a, b) => String(a.sk || a.ts || "").localeCompare(String(b.sk || b.ts || "")));

    const llm = calls.filter((c) => c.kind === "llm");
    const tools = calls.filter((c) => c.kind === "tool");
    const mems = calls.filter((c) => c.kind === "memory");
    const guards = calls.filter((c) => c.kind === "guardrail");
    const pols = calls.filter((c) => c.kind === "policy");
    const totIn = llm.reduce((s, c) => s + Number(c.input_tokens || 0), 0);
    const totOut = llm.reduce((s, c) => s + Number(c.output_tokens || 0), 0);
    const totSys = llm.reduce((s, c) => s + Number(c.system_tokens || 0), 0);
    const totCost = calls.reduce((s, c) => s + Number(c.cost_usd || 0), 0);
    const totLat = calls.reduce((s, c) => s + Number(c.latency_ms || 0), 0);

    const chips = `<div class="obs-mchips">
      <span class="obs-mchip"><b>${fmtInt(llm.length)}</b> model call${llm.length === 1 ? "" : "s"}</span>
      ${tools.length ? `<span class="obs-mchip"><b>${fmtInt(tools.length)}</b> tool call${tools.length === 1 ? "" : "s"}</span>` : ""}
      ${mems.length ? `<span class="obs-mchip"><b>${fmtInt(mems.length)}</b> memory op${mems.length === 1 ? "" : "s"}</span>` : ""}
      ${guards.length ? `<span class="obs-mchip"><b>${fmtInt(guards.length)}</b> guardrail check${guards.length === 1 ? "" : "s"}</span>` : ""}
      ${pols.length ? `<span class="obs-mchip"><b>${fmtInt(pols.length)}</b> policy check${pols.length === 1 ? "" : "s"}</span>` : ""}
      <span class="obs-mchip"><b>${fmtInt(totIn)}</b> in · <b>${fmtInt(totOut)}</b> out · <b>${fmtInt(totSys)}</b> sys tokens</span>
      <span class="obs-mchip">total latency <b>${fmtMs(totLat)}</b></span>
      <span class="obs-mchip">est. cost <b>${fmtUsd(totCost)}</b></span></div>`;

    // If the snapshot history knows about more re-runs than the telemetry can
    // separate, this run predates per-version telemetry — say so plainly.
    const knownVersions = (flowHistory[agentId] || []).length;
    const legacyNote = (knownVersions > versions.length)
      ? `<div class="obs-note" style="margin:0 0 12px">This run predates per-version telemetry, so its ${fmtInt(knownVersions)} versions can't be separated here — all recorded calls are shown together. New runs are split by version.</div>`
      : "";

    const body = calls.length ? callBlocks(calls)
      : `<div class="obs-empty">No telemetry was captured for Version ${esc(String(sel))} (a compute-only step, or a run recorded before per-version telemetry).</div>`;
    // Evaluation is scoped to the SELECTED version, so each version shows the
    // eval that scored that version's run.
    return chips + legacyNote + versionHeader(agentId, sel, versions) + body + evalSection(agentId, sel);
  }

  // When to re-read telemetry after starting an evaluation. Scoring is asynchronous
  // (LLM judge, then a CloudWatch read), so there is nothing to show immediately.
  // One place to tune, rather than a magic number per call site.
  const EVAL_POLL_MS = [25000, 50000];

  // ---- AgentCore Evaluations (LLM-as-judge) ----------------------------------
  // Per-evaluator score bar + label + explanation, plus an on-demand "Evaluate"
  // button. Eval rows are kind="eval" telemetry written by the runtime
  // (bedrock-agentcore:Evaluate over the agent's spans / persisted prompt I/O).
  function scoreClass(v) { return v >= 0.7 ? "good" : (v >= 0.4 ? "mid" : "low"); }

  // The judge's own band when AgentCore returned one (eval_label, e.g. "Very
  // Helpful") — it is authoritative. Only when it is absent do we derive a band
  // from the score, which means inventing thresholds the evaluator never set.
  function evalBand(c) {
    if (c.eval_label) return c.eval_label;
    const v = Number(c.value || 0);
    return v >= 0.7 ? "strong" : (v >= 0.4 ? "moderate" : "weak");
  }

  // Per-agent feature flags from the workflow definition handed to mount().
  // The BFF ships `evalAgents` = the ids with evaluations.enabled in workflow.json.
  function agentFlags(agentId) {
    const ev = wfDef.evalAgents || [];
    return { evalEnabled: ev.indexOf(agentId) !== -1 };
  }

  // Distinct named prompts an agent issued in the selected version (first-seen
  // order). An agent can make several model calls with different prompts (e.g.
  // "analysis" and "analysis-scoring"). Runs with no prompt tag yield [""] so
  // they still render as a single block.
  function agentPrompts(agentId, sel) {
    const seen = [];
    flowCalls
      .filter((c) => c.agent_id === agentId && c.kind === "llm" && callVersion(c) === sel)
      .sort((a, b) => String(a.sk || a.ts || "").localeCompare(String(b.sk || b.ts || "")))
      .forEach((c) => { const p = String(c.prompt || "").trim(); if (!seen.includes(p)) seen.push(p); });
    return seen.length ? seen : [""];
  }

  // Eval rows for ONE prompt of ONE version, scoped only by the dimensions that
  // are actually tagged (older rows lack version/prompt → shown unscoped).
  function evalRowsFor(agentId, prompt, sel) {
    const all = flowCalls.filter((c) => c.agent_id === agentId && c.kind === "eval");
    const versioned = all.some((c) => Number(c.version) > 0);
    const prompted = all.some((c) => String(c.prompt || "").trim() !== "");
    return all.filter((c) =>
        (!versioned || Number(c.version) === Number(sel)) &&
        (!prompted || String(c.prompt || "").trim() === prompt))
      .sort((a, b) => String(a.sk || a.ts || "").localeCompare(String(b.sk || b.ts || "")));
  }

  // The most-recent result per evaluator, rendered as score cards.
  function evalCards(rows) {
    const latest = {};
    rows.forEach((r) => { latest[r.label] = r; });
    return Object.values(latest).map((c) => {
      const err = String(c.status || "").toLowerCase() === "error";
      const v = Number(c.value || 0);
      const pct = Math.round(v * 100);
      const name = String(c.label || "").replace(/^Builtin\./, "");
      if (err) {
        return `<div class="obs-evalcard">
          <div class="obs-evalhd"><span class="obs-evalname">${esc(name)}</span>
            <span class="obs-badge error">error</span></div>
          <div class="obs-evalexpl">${esc(c.output_text || "Evaluation error.")}</div></div>`;
      }
      return `<div class="obs-evalcard">
        <div class="obs-evalhd"><span class="obs-evalname">${esc(name)}</span>
          <span class="obs-evallabel">${esc(evalBand(c) || "")}</span>
          <span class="obs-evalscore ${scoreClass(v)}">${v.toFixed(2)}</span></div>
        <div class="obs-evalbar"><span class="obs-evalfill ${scoreClass(v)}" style="width:${pct}%"></span></div>
        ${c.output_text ? `<div class="obs-evalexpl">${esc(c.output_text)}</div>` : ""}</div>`;
    }).join("");
  }

  // ONE block per prompt for the selected version, each with its own score cards
  // + Evaluate button. Shown only for agents with evaluations.enabled.
  function evalSection(agentId, sel) {
    if (!agentFlags(agentId).evalEnabled) return "";
    const prompts = agentPrompts(agentId, sel);
    const multi = prompts.length > 1 || !!(prompts[0] || "");
    const blocks = prompts.map((p) => promptEvalBlock(agentId, p, sel)).join("");
    return `<div class="obs-mchips" style="margin:22px 0 8px">
        <span class="obs-mchip"><b>Evaluation</b> (AgentCore, LLM-as-judge)${multi ? " · per prompt" : ""}</span></div>
      ${blocks}`;
  }

  function promptEvalBlock(agentId, prompt, sel) {
    const rows = evalRowsFor(agentId, prompt, sel);
    const cards = evalCards(rows);
    const runBtn = `<button class="obs-eval-run"${authzGate("evaluate")} onclick="obsRunEval(${jsq(agentId)},${jsq(prompt)})">${rows.length ? "Re-evaluate" : "Evaluate"}</button>`;
    const title = prompt ? `Prompt <code>${esc(prompt)}</code>` : "Prompt";
    const emptyMsg = `Not evaluated yet${prompt ? ` for <code>${esc(prompt)}</code>` : ""} at <b>Version ${esc(String(sel))}</b>. Click <b>Evaluate</b> to score it.`;
    return `<div style="border-top:1px solid #eef0f3;padding-top:10px;margin-top:10px">
        <div class="obs-mchips" style="margin:0 0 8px">
          <span class="obs-mchip">${title}</span>${runBtn}</div>
        <div class="obs-evalgrid">${cards || `<div class="obs-note" style="margin:0">${emptyMsg}</div>`}</div>
      </div>`;
  }

  async function runEval(agentId, prompt = "") {
    const sid = resolveSessionId();
    if (!sid) return;
    document.querySelectorAll(".obs-eval-run").forEach((b) => { b.disabled = true; });
    try {
      const r = await api(`/api/sessions/${sid}/evaluate`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ agentId, prompt }),
      });
      if (r && r.ok === false) { alert(r.error || "Evaluation is not available here."); return; }
      // Scoring is asynchronous (LLM judge + CloudWatch reads); poll twice.
      const shown = selectedVersion[agentId] || agentVersions(agentId).slice(-1)[0];
      EVAL_POLL_MS.forEach((ms) => setTimeout(() => refreshEval(agentId, sid, shown), ms));
    } catch (e) {
      alert("Couldn't start evaluation: " + e.message);
    } finally {
      // Only re-enable if the user is actually allowed to evaluate — otherwise this
      // blanket re-enable would undo the gate applied when the block was rendered.
      if (authzCan("evaluate")) {
        setTimeout(() => document.querySelectorAll(".obs-eval-run").forEach((b) => { b.disabled = false; }), 1000);
      }
    }
  }

  // `sid` is passed in from the click that started the evaluation rather than
  // re-resolved: the user may have typed a different run into the session box while
  // the judge was still scoring, and re-resolving would write another run's calls
  // into flowCalls. `version` is likewise the version the modal is SHOWING — this
  // used to jump to the latest, silently moving a reviewer off Version 1.
  async function refreshEval(agentId, sid, version) {
    try { const d = await api(`/api/sessions/${sid}/telemetry`); flowCalls = d.calls || []; }
    catch (e) { return; }
    if ($("obsIOBody")) $("obsIOBody").innerHTML = agentIOBody(agentId, version);
  }

  // ---- the modal itself ------------------------------------------------------
  // A <select> to switch versions inside the modal (only when >1 version).
  function versionPicker(agentId, sel) {
    const versions = agentVersions(agentId);
    if (versions.length <= 1) return "";
    const opts = versions.slice().reverse().map((v) =>
      `<option value="${v}"${v === sel ? " selected" : ""}>Version ${esc(String(v))}` +
      `${v === versions[versions.length - 1] ? " (latest)" : ""}${v === 1 ? " · initial" : ""}</option>`).join("");
    return `<div class="obs-vercontrols"><label for="obsVerSel">Show version</label>
      <select id="obsVerSel" class="obs-verselect" onchange="obsPickVersion(${jsq(agentId)}, Number(this.value))">${opts}</select>
      <span class="obs-vercount">${versions.length} versions</span></div>`;
  }

  function openAgentIO(agentId) {
    const versions = agentVersions(agentId);
    const sel = versions[versions.length - 1];
    selectedVersion[agentId] = sel;
    const html = `<div class="obs-modal-back" id="obsModalBack">
      <div class="obs-modal" role="dialog" aria-modal="true">
        <div class="obs-modal-head">
          <div><div class="mt">${esc(friendlyAgent(agentId))}</div>
            <div class="ms">System prompt · input · output · features · evaluation${versions.length > 1 ? " — pick a version" : ""}</div></div>
          <button class="obs-modal-x" onclick="obsCloseModal()" aria-label="Close">✕</button>
        </div>
        <div class="obs-modal-body">${versionPicker(agentId, sel)}<div id="obsIOBody">${agentIOBody(agentId, sel)}</div></div>
      </div></div>`;
    let host = $("obsModalHost");
    if (!host) { host = document.createElement("div"); host.id = "obsModalHost"; document.body.appendChild(host); }
    host.innerHTML = html;
    $("obsModalBack").addEventListener("mousedown", (e) => { if (e.target.id === "obsModalBack") closeModal(); });
    document.addEventListener("keydown", escClose);
  }

  // Which version each agent's modal is currently showing, so an async eval refresh
  // re-renders the version the reviewer is looking at rather than the latest.
  const selectedVersion = {};

  function pickVersion(agentId, v) {
    selectedVersion[agentId] = v;
    const el = $("obsIOBody");
    if (el) el.innerHTML = agentIOBody(agentId, v);
  }
  function escClose(e) { if (e.key === "Escape") closeModal(); }
  function closeModal() {
    const host = $("obsModalHost");
    if (host) host.innerHTML = "";
    document.removeEventListener("keydown", escClose);
  }
  function copyPre(btn) {
    const pre = btn.closest("summary") && btn.closest("summary").parentElement.querySelector("pre");
    if (!pre) return;
    navigator.clipboard.writeText(pre.textContent || "").then(() => {
      const t = btn.textContent; btn.textContent = "copied"; setTimeout(() => { btn.textContent = t; }, 1200);
    }).catch(() => {});
  }

  // ---- Insights (cross-run batch analysis) ----------------------------------
  // A clickable session id -> opens the Run detail tab for that session.
  function sessionChip(sid, available) {
    if (!sid) return "";
    const s = esc(String(sid));
    // available === false: the run's data has aged out (Insights analyzes a wider
    // CloudWatch window than the app retains) — show it, but don't link.
    if (available === false) {
      return `<span class="obs-sesslink obs-sesslink-dead" title="This run is no longer available (aged out of retention or deleted) — nothing to open">${s} · n/a</span>`;
    }
    return `<a href="#" class="obs-sesslink" onclick="obsOpenSession(${jsq(sid)});return false;" title="Open run detail for ${s}">${s}</a>`;
  }

  function sectionHead(title, subtitle, n) {
    return `<div class="obs-mchips" style="margin:16px 0 4px"><span class="obs-mchip"><b>${esc(title)}</b> (${n})</span></div>`
      + (subtitle ? `<div class="obs-note" style="margin:0 0 8px">${esc(subtitle)}</div>` : "");
  }

  // The span-level evidence under a failed session (spanId + strongest signal).
  function failureSpans(spans) {
    if (!spans || !spans.length) return "";
    const rows = spans.map((sp) => `
      <div class="obs-ins-span">
        <code>${esc(String(sp.spanId || ""))}</code>${sp.category ? ` <span class="obs-note" style="margin:0">(${esc(String(sp.category))})</span>` : ""}
        ${sp.evidence ? `<div class="obs-evalexpl" style="margin-top:3px">${esc(String(sp.evidence))}</div>` : ""}
      </div>`).join("");
    return `<div class="obs-note" style="margin:6px 0 2px">Failure spans</div>${rows}`;
  }

  function failureSessionRows(sessions) {
    if (!sessions || !sessions.length) return "";
    return sessions.map((se) => `
      <div class="obs-ins-sess">
        <div class="obs-note" style="margin:0">Session ${sessionChip(se.sid, se.available)}${se.fixType ? ` · <b>${esc(String(se.fixType))}</b>` : ""}</div>
        ${se.explanation ? `<div class="obs-evalexpl">${esc(String(se.explanation))}</div>` : ""}
        ${se.recommendation ? `<div class="obs-note" style="margin:4px 0 0"><b>Fix:</b> ${esc(String(se.recommendation))}</div>` : ""}
        ${failureSpans(se.spans)}
      </div>`).join("");
  }

  function rootCauseRows(rcs) {
    if (!rcs || !rcs.length) return "";
    return rcs.map((rc, i) => `
      <div class="obs-ins-rc">
        <div class="obs-ins-rc-h"><b>Root cause ${i + 1}:</b> ${esc(String(rc.name || ""))} <span class="obs-note" style="margin:0">· ${esc(String(rc.affectedSessionCount || 0))} session(s)</span></div>
        ${rc.rootCause ? `<div class="obs-evalexpl">${esc(String(rc.rootCause))}</div>` : ""}
        ${rc.recommendation ? `<div class="obs-note" style="margin:4px 0 0"><b>Recommendation:</b> ${esc(String(rc.recommendation))}</div>` : ""}
        ${failureSessionRows(rc.sessions)}
      </div>`).join("");
  }

  // Failure patterns: cluster -> subcategory -> root causes -> sessions -> spans.
  function failureClusters(items) {
    if (!items || !items.length) return "";
    const rows = items.map((c) => `
      <details class="obs-optcard">
        <summary>${esc(String(c.name || "cluster"))} <span class="obs-note" style="margin:0">· ${esc(String(c.affectedSessionCount || 0))} session(s)</span></summary>
        ${c.description ? `<div class="obs-evalexpl">${esc(String(c.description))}</div>` : ""}
        ${(c.subCategories || []).map((sc) => `
          <div class="obs-ins-sub">
            <div class="obs-ins-sub-h">${esc(String(sc.name || ""))} <span class="obs-note" style="margin:0">· ${esc(String(sc.affectedSessionCount || 0))} session(s)</span></div>
            ${sc.description ? `<div class="obs-evalexpl">${esc(String(sc.description))}</div>` : ""}
            ${rootCauseRows(sc.rootCauses)}
          </div>`).join("")}
      </details>`).join("");
    return sectionHead("Failure patterns",
      "Recurring ways runs broke — clustered by root cause, with the affected sessions and span-level evidence.", items.length)
      + `<div class="obs-optgrid">${rows}</div>`;
  }

  // User intents: goal clusters with a sample user message per session.
  function intentClusters(items) {
    if (!items || !items.length) return "";
    const rows = items.map((c) => `
      <details class="obs-optcard">
        <summary>${esc(String(c.name || "intent"))} <span class="obs-note" style="margin:0">· ${esc(String(c.affectedSessionCount || 0))} session(s)</span></summary>
        ${c.description ? `<div class="obs-evalexpl">${esc(String(c.description))}</div>` : ""}
        ${(c.sessions || []).map((se) => `
          <div class="obs-ins-sess">
            <div class="obs-note" style="margin:0">Session ${sessionChip(se.sid, se.available)}</div>
            ${(se.messages || []).map((m) => `<div class="obs-evalexpl">“${esc(String(m))}”</div>`).join("")}
          </div>`).join("")}
      </details>`).join("");
    return sectionHead("User intents",
      "What users were trying to get done, grouped by goal.", items.length)
      + `<div class="obs-optgrid">${rows}</div>`;
  }

  // Execution summaries: how the agents approached the work + the outcome.
  function summaryClusters(items) {
    if (!items || !items.length) return "";
    const rows = items.map((c) => `
      <details class="obs-optcard">
        <summary>${esc(String(c.name || "summary"))} <span class="obs-note" style="margin:0">· ${esc(String(c.affectedSessionCount || 0))} session(s)</span></summary>
        ${c.description ? `<div class="obs-evalexpl">${esc(String(c.description))}</div>` : ""}
        ${(c.sessions || []).map((se) => `
          <div class="obs-ins-sess">
            <div class="obs-note" style="margin:0">Session ${sessionChip(se.sid, se.available)}</div>
            ${se.approach ? `<div class="obs-evalexpl"><b>Approach:</b> ${esc(String(se.approach))}</div>` : ""}
            ${se.outcome ? `<div class="obs-evalexpl"><b>Outcome:</b> ${esc(String(se.outcome))}</div>` : ""}
          </div>`).join("")}
      </details>`).join("");
    return sectionHead("Execution summaries",
      "How the agents approached the work and the outcomes reached.", items.length)
      + `<div class="obs-optgrid">${rows}</div>`;
  }

  // Jump from an insights cluster to the Run detail tab for a given session.
  function openSession(sid) {
    if (!sid) return;
    // Set the target BEFORE switching tabs. goto("flow") auto-loads whatever the
    // session box currently holds, so setting it afterwards fired a fetch for the
    // PREVIOUS run and raced the correct one — last response won, which could leave
    // the table showing the wrong session.
    const inp = $("obsSession");
    if (inp) { sessionMap[sid] = sid; inp.value = sid; }
    goto("flow");
  }

  function insightsHtml(data) {
    const st = data.status || "none";
    if (st === "none") {
      const note = data.note ? ` ${esc(String(data.note))}` : "";
      return `<div class="obs-note" style="margin:0">No insights run yet. Pick a window and click <b>Run insights</b> — it analyzes recent runs for failure patterns, user intents, and behavior summaries.${note}</div>`;
    }
    // The statuses insights.py can actually store: none | IN_PROGRESS | COMPLETED |
    // COMPLETED_WITH_ERRORS | FAILED | STOPPED. STOPPED used to fall through to the
    // findings renderer and report "No clusters returned for this window", which
    // reads like an empty result rather than a cancelled job.
    if (st === "IN_PROGRESS") return `<div class="obs-note" style="margin:0"><span class="obs-mchip">Running…</span> AgentCore is analyzing recent runtime traces (a few minutes). Click <b>Refresh</b> to check.</div>`;
    if (st === "FAILED") return `<div class="obs-note warn" style="margin:0">Insights run failed. Ensure at least one run has completed and Transaction Search is enabled, then try again.</div>`;
    if (st === "STOPPED") return `<div class="obs-note warn" style="margin:0">The insights run was stopped before it finished, so there are no findings. Start a new one when you're ready.</div>`;
    const f = data.findings || {};
    const s = f.sessions || {};
    const total = Number(s.total || 0), analyzed = Number(s.completed || 0), skipped = Number(s.failed || 0);
    const head = `<div class="obs-mchips" style="margin:0 0 8px">
      <span class="obs-mchip"><b>${esc(st)}</b></span>
      <span class="obs-mchip">${analyzed} of ${total} sessions analyzed${skipped ? ` · ${skipped} skipped` : ""}</span>
      ${data.updated_at ? `<span class="obs-note" style="margin:0">updated ${esc(String(data.updated_at))}</span>` : ""}</div>`;
    const errNote = (st === "COMPLETED_WITH_ERRORS" && skipped)
      ? `<div class="obs-note" style="margin:0 0 10px">Completed with partial errors — ${skipped} session(s) couldn't be analyzed (typically control-plane/one-off sessions, or runs with no scorable agent content). The findings below come from the ${analyzed} analyzed session(s).</div>`
      : "";
    const body = failureClusters(f.failures) + intentClusters(f.userIntents) + summaryClusters(f.executionSummaries);
    return head + errNote + (body || `<div class="obs-note" style="margin:0">No clusters returned for this window.</div>`);
  }

  async function loadInsights() {
    const el = $("obsInsightsBody");
    if (!el) return;
    try {
      const data = await api("/api/insights");
      el.innerHTML = insightsHtml(data || {});
    } catch (e) {
      el.innerHTML = `<div class="obs-note warn" style="margin:0">Couldn't load insights: ${esc(e.message)}</div>`;
    }
  }

  async function runInsights() {
    const lookback = Number(($("obsInsLookback") && $("obsInsLookback").value) || 168);
    try {
      const r = await api("/api/insights/run", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ lookbackHours: lookback }),
      });
      if (r && r.ok === false) {
        if ($("obsInsightsBody")) $("obsInsightsBody").innerHTML = `<div class="obs-note warn" style="margin:0">${esc(r.error || "Insights is not available here.")}</div>`;
        return;
      }
      if ($("obsInsightsBody")) $("obsInsightsBody").innerHTML = `<div class="obs-note" style="margin:0"><span class="obs-mchip">Running…</span> analyzing the last ${lookback}h of runs. This takes a few minutes — click <b>Refresh</b> to check.</div>`;
      [60, 120, 180, 240].forEach((s) => setTimeout(loadInsights, s * 1000));
    } catch (e) {
      alert("Couldn't start insights: " + e.message);
    }
  }

  // ---- the island's contract with the React shell ---------------------------
  window.ObservabilityIsland = { mount: mount };

  // ---- the handlers this module's own inline HTML calls ---------------------
  window.showObsView = showObs;
  window.showCampaignsView = showCampaigns;
  window.obsGoto = goto;
  window.obsLoadOverview = loadOverview;
  window.obsLoadFlow = loadFlow;
  window.obsToggle = toggleAgent;
  window.obsReproject = reproject;
  window.obsExport = exportOverview;
  window.obsExportFlow = exportFlow;
  window.obsAgentIO = openAgentIO;
  window.obsPickVersion = pickVersion;
  window.obsRunEval = runEval;
  window.obsCloseModal = closeModal;
  window.obsCopy = copyPre;
  window.obsRunInsights = runInsights;
  window.obsLoadInsights = loadInsights;
  window.obsOpenSession = openSession;
})();
