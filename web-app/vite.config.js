import { defineConfig } from "vite";

// SharedArrayBuffer (what shmring_ipc.rs's main-thread/Worker channel is
// built on -- see that file's doc comment) is only exposed on a
// cross-origin-isolated page, which requires these two response headers.
const crossOriginIsolationHeaders = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
};

export default defineConfig({
  server: { headers: crossOriginIsolationHeaders },
  preview: { headers: crossOriginIsolationHeaders },
  worker: { format: "es" },
});
