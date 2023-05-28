import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/members": {
        target: "http://127.0.0.1:6050",
      },
    },
  },
  resolve: {
    alias: [{ find: "@", replacement: path.resolve(__dirname, "src") }],
  },
});
