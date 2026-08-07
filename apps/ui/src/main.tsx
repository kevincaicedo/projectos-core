import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles/tokens.css";
import "./styles/shell.css";

const root = document.getElementById("root");
if (root === null) {
  throw new Error("index.html must provide #root");
}
createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
