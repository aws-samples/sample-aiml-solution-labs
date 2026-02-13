import React from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  LineChart,
  Line,
  AreaChart,
  Area,
  RadarChart,
  Radar,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
} from "recharts";

const COLORS = [
  "#0972d3", "#037f0c", "#d91515", "#8b5cf6",
  "#f59e0b", "#ec4899", "#06b6d4", "#84cc16",
  "#f97316", "#6366f1",
];

/**
 * Attempts to detect chart data in a parsed JSON object and render it.
 * Only renders if the JSON explicitly contains chart_type or chart_data fields.
 */
export function tryRenderChart(parsed) {
  if (!parsed || typeof parsed !== "object") return null;

  const hasChartType = findField(parsed, "chart_type") !== null;
  const hasChartData = parsed.chart_data || (typeof parsed === "object" &&
    Object.values(parsed).some(v => v && typeof v === "object" && v.chart_data));

  if (!hasChartType && !hasChartData) return null;

  const chartData = findChartData(parsed);
  if (!chartData || chartData.length === 0) return null;

  const rawType = (findField(parsed, "chart_type") || "").toLowerCase();
  const chartType = normalizeChartType(rawType, chartData);
  const chartTitle = findField(parsed, "chart_title") || "Data";

  const numericKeys = getNumericKeys(chartData);
  if (numericKeys.length === 0) return null;

  const data = chartData.map((item) => {
    const row = { ...item };
    numericKeys.forEach((key) => {
      const val = row[key];
      if (typeof val === "string") {
        row[key] = parseFloat(val.replace(/[$,]/g, "")) || 0;
      }
    });
    return row;
  });

  const chartHeight = ["pie", "donut"].includes(chartType) ? 350
    : chartType === "radar" ? 380
    : Math.max(280, 50 + data.length * 35);

  return (
    <div style={{ margin: "12px 0" }}>
      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>{chartTitle}</div>
      {renderChart(chartType, data, numericKeys, chartHeight)}
    </div>
  );
}

function renderChart(type, data, numericKeys, height) {
  switch (type) {
    case "pie":
      return (
        <ResponsiveContainer width="100%" height={height}>
          <PieChart>
            <Pie data={data} dataKey={numericKeys[0]} nameKey="name" cx="50%" cy="50%" outerRadius={100}
              label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`} labelLine>
              {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Pie>
            <Tooltip /><Legend verticalAlign="bottom" />
          </PieChart>
        </ResponsiveContainer>
      );

    case "donut":
      return (
        <ResponsiveContainer width="100%" height={height}>
          <PieChart>
            <Pie data={data} dataKey={numericKeys[0]} nameKey="name" cx="50%" cy="50%"
              innerRadius={60} outerRadius={100}
              label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`} labelLine>
              {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Pie>
            <Tooltip /><Legend verticalAlign="bottom" />
          </PieChart>
        </ResponsiveContainer>
      );

    case "line":
      return (
        <ResponsiveContainer width="100%" height={height}>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} angle={-30} textAnchor="end" height={60} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip /><Legend />
            {numericKeys.map((key, i) => (
              <Line key={key} type="monotone" dataKey={key} stroke={COLORS[i % COLORS.length]} strokeWidth={2} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      );

    case "area":
      return (
        <ResponsiveContainer width="100%" height={height}>
          <AreaChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} angle={-30} textAnchor="end" height={60} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip /><Legend />
            {numericKeys.map((key, i) => (
              <Area key={key} type="monotone" dataKey={key}
                stroke={COLORS[i % COLORS.length]} fill={COLORS[i % COLORS.length]} fillOpacity={0.3} />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      );

    case "stacked_bar":
      return (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={data} layout="vertical" margin={{ left: 20, right: 20, top: 5, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" tick={{ fontSize: 11 }} />
            <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={150} />
            <Tooltip /><Legend />
            {numericKeys.map((key, i) => (
              <Bar key={key} dataKey={key} stackId="stack" fill={COLORS[i % COLORS.length]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      );

    case "grouped_bar":
      return (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={data} margin={{ left: 10, right: 10, top: 5, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} angle={-30} textAnchor="end" height={80} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip /><Legend />
            {numericKeys.map((key, i) => (
              <Bar key={key} dataKey={key} fill={COLORS[i % COLORS.length]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      );

    case "radar":
      return (
        <ResponsiveContainer width="100%" height={height}>
          <RadarChart data={data} cx="50%" cy="50%" outerRadius={120}>
            <PolarGrid />
            <PolarAngleAxis dataKey="name" tick={{ fontSize: 11 }} />
            <PolarRadiusAxis tick={{ fontSize: 10 }} />
            {numericKeys.map((key, i) => (
              <Radar key={key} name={formatHeader(key)} dataKey={key}
                stroke={COLORS[i % COLORS.length]} fill={COLORS[i % COLORS.length]} fillOpacity={0.2} />
            ))}
            <Tooltip /><Legend />
          </RadarChart>
        </ResponsiveContainer>
      );

    case "heatmap":
      return <Heatmap data={data} numericKeys={numericKeys} />;

    default: // "bar"
      return (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={data} layout="vertical" margin={{ left: 20, right: 20, top: 5, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" tick={{ fontSize: 11 }} />
            <YAxis dataKey="name" type="category" tick={{ fontSize: 11 }} width={150} />
            <Tooltip /><Legend />
            {numericKeys.map((key, i) => (
              <Bar key={key} dataKey={key} fill={COLORS[i % COLORS.length]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      );
  }
}

function Heatmap({ data, numericKeys }) {
  let globalMin = Infinity, globalMax = -Infinity;
  data.forEach((row) => numericKeys.forEach((key) => {
    const v = row[key];
    if (typeof v === "number") { globalMin = Math.min(globalMin, v); globalMax = Math.max(globalMax, v); }
  }));
  const range = globalMax - globalMin || 1;

  function cellColor(value) {
    const ratio = (value - globalMin) / range;
    if (ratio < 0.5) { return `rgb(${Math.round(255 * ratio * 2)}, 200, 80)`; }
    return `rgb(255, ${Math.round(200 * (1 - (ratio - 0.5) * 2))}, 60)`;
  }

  const thStyle = { padding: "6px 10px", background: "#f2f3f3", border: "1px solid #e9ebed", fontWeight: 600 };

  return (
    <div style={{ overflowX: "auto", margin: "8px 0" }}>
      <table style={{ borderCollapse: "collapse", fontSize: 12, width: "100%" }}>
        <thead>
          <tr>
            <th style={{ ...thStyle, textAlign: "left" }}>Name</th>
            {numericKeys.map((k) => <th key={k} style={{ ...thStyle, textAlign: "center" }}>{formatHeader(k)}</th>)}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i}>
              <td style={{ padding: "6px 10px", border: "1px solid #e9ebed", fontWeight: 500, whiteSpace: "nowrap" }}>{row.name || "—"}</td>
              {numericKeys.map((k) => {
                const v = row[k];
                return (
                  <td key={k} style={{ padding: "6px 10px", border: "1px solid #e9ebed", textAlign: "center",
                    background: typeof v === "number" ? cellColor(v) : "#fff", fontWeight: 500 }}>
                    {typeof v === "number" ? v.toFixed(6) : "—"}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 6, fontSize: 11, color: "#5f6b7a" }}>
        <span>Low</span>
        <div style={{ width: 120, height: 12, borderRadius: 3,
          background: "linear-gradient(to right, rgb(0,200,80), rgb(255,200,60), rgb(255,60,60))" }} />
        <span>High</span>
      </div>
    </div>
  );
}

/**
 * Normalizes chart_type strings from the agent into supported types.
 */
function normalizeChartType(raw, data) {
  const map = {
    bar: "bar", "bar_chart": "bar", "bar chart": "bar", "bar graph": "bar", "horizontal_bar": "bar",
    pie: "pie", "pie_chart": "pie", "pie chart": "pie",
    donut: "donut", "donut_chart": "donut", "donut chart": "donut", doughnut: "donut",
    line: "line", "line_chart": "line", "line chart": "line",
    area: "area", "area_chart": "area", "area chart": "area",
    stacked_bar: "stacked_bar", "stacked bar": "stacked_bar", "stacked_bar_chart": "stacked_bar", "stacked bar chart": "stacked_bar",
    grouped_bar: "grouped_bar", "grouped bar": "grouped_bar", "grouped_bar_chart": "grouped_bar", "grouped bar chart": "grouped_bar",
    "vertical_bar": "grouped_bar", "vertical bar": "grouped_bar",
    radar: "radar", "radar_chart": "radar", "radar chart": "radar", spider: "radar", "spider_chart": "radar",
    heatmap: "heatmap", "heat_map": "heatmap", "heat map": "heatmap",
  };
  return map[raw] || guessChartType(data);
}

function findChartData(obj) {
  if (Array.isArray(obj) && obj.length > 0 && typeof obj[0] === "object") {
    if (getNumericKeys(obj).length > 0) return normalizeNames(obj);
  }
  if (obj && typeof obj === "object") {
    if (obj.chart_data && Array.isArray(obj.chart_data)) return normalizeNames(obj.chart_data);
    for (const val of Object.values(obj)) {
      const found = findChartData(val);
      if (found) return found;
    }
  }
  return null;
}

function normalizeNames(arr) {
  return arr.map((item) => {
    if (item.name) return item;
    const labelKey = Object.keys(item).find((k) => typeof item[k] === "string" && !k.includes("type"));
    return labelKey ? { ...item, name: item[labelKey] } : item;
  });
}

function getNumericKeys(arr) {
  if (!arr.length) return [];
  const first = arr[0];
  return Object.keys(first).filter((key) => {
    if (key === "name") return false;
    const val = first[key];
    if (typeof val === "number") return true;
    if (typeof val === "string") { const n = parseFloat(val.replace(/[$,]/g, "")); return !isNaN(n) && isFinite(n); }
    return false;
  });
}

function findField(obj, fieldName) {
  if (!obj || typeof obj !== "object") return null;
  if (obj[fieldName] && typeof obj[fieldName] === "string") return obj[fieldName];
  for (const val of Object.values(obj)) { const f = findField(val, fieldName); if (f) return f; }
  return null;
}

function guessChartType(data) { return data.length <= 5 ? "pie" : "bar"; }

function formatHeader(key) {
  return key.replace(/_/g, " ").replace(/([a-z])([A-Z])/g, "$1 $2").replace(/\b\w/g, (c) => c.toUpperCase());
}
