import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { THEME } from "./constants/theme";

// base document styles
const root = document.documentElement;
root.style.fontFamily =
  "'DM Sans', Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif";
root.style.color = THEME.text;
root.style.backgroundColor = THEME.bg;
root.style.fontSynthesis = "none";
root.style.textRendering = "optimizeLegibility";
root.style.setProperty("-webkit-font-smoothing", "antialiased");

root.style.setProperty("--accent", THEME.accent);
root.style.setProperty("--accent-dark", THEME.accentDark);
root.style.setProperty("--accent-mid", THEME.accentMid);

root.style.setProperty("--text", THEME.text);
root.style.setProperty("--muted-text", THEME.mutedText);
root.style.setProperty("--input-bg", THEME.inputBg);

document.body.style.margin = "0";
document.body.style.minWidth = "320px";
document.body.style.background = THEME.bg;

const appRoot = document.getElementById("root");
if (appRoot) {
  appRoot.style.minHeight = "100vh";
  appRoot.style.boxSizing = "border-box";
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
