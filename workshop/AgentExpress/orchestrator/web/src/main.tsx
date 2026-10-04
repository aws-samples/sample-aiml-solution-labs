import "@cloudscape-design/global-styles/index.css";

import { applyMode, Mode } from "@cloudscape-design/global-styles";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import App from "./App";

// Light mode explicitly. Cloudscape supports dark, and the graph's CSS reads the same
// design tokens, so switching is a one-line change — but a reference sample should
// look the same in every screenshot.
applyMode(Mode.Light);

const el = document.getElementById("root");
if (!el) throw new Error("#root is missing from index.html");

createRoot(el).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
