/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  base: '/',
  plugins: [vue(), tailwindcss()],
  build: {
    // silod embeds this directory with go:embed, so the build lands inside the Go tree.
    outDir: '../internal/webui/dist',
    // The Makefile clears assets/ first; emptying the whole directory would delete the tracked .gitkeep.
    emptyOutDir: false,
  },
  server: {
    proxy: {
      '/api': 'http://localhost:7080',
    },
  },
  test: {
    environment: 'jsdom',
    include: ['src/**/*.spec.ts'],
    restoreMocks: true,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.{ts,vue}'],
      exclude: ['src/**/*.spec.ts', 'src/main.ts', 'src/env.d.ts'],
      reporter: ['text-summary', 'text', 'html'],
      thresholds: { lines: 100, functions: 100, branches: 100, statements: 100 },
    },
  },
})
