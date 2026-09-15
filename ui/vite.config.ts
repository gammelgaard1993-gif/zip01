import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The built UI is served by the UI-app server (ui/server, FastAPI) at the root.
// In dev, proxy the control-plane API + SSE to the UI-app on :5174 so EventSource
// and fetch work same-origin without CORS. The UI-app, not this dev server, owns
// the runner and talks to the system-under-test backend.
export default defineConfig({
  plugins: [react()],
  base: "/",
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      "/api": { target: "http://127.0.0.1:5174", changeOrigin: true },
    },
  },
});
