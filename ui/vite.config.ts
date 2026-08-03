import path from 'node:path'
import tailwindcss from '@tailwindcss/vite'
import { TanStackRouterVite } from '@tanstack/router-plugin/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [TanStackRouterVite({ quoteStyle: 'single' }), react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(import.meta.dirname, './src'),
    },
  },
  server: {
    proxy: {
      '/api': 'http://localhost:9090',
      '/healthz': 'http://localhost:9090',
      '/metrics': 'http://localhost:9090',
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    chunkSizeWarningLimit: 700,
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            { name: 'tanstack', test: /node_modules[\\/]@tanstack/ },
            { name: 'react-flow', test: /node_modules[\\/]@xyflow/ },
            { name: 'charts', test: /node_modules[\\/](?:recharts|victory-vendor)/ },
            { name: 'vendor', test: /node_modules/ },
          ],
        },
      },
    },
  },
})
