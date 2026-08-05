import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// One React bundle for both shells (L12): rendered in the Tauri webview and
// served by pos-server. Transport selection is the only allowed platform branch.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 1420,
    strictPort: true,
  },
});
