import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const apiTarget = env.VITE_API_URL || 'http://api:8000';

  return {
    plugins: [react()],
    server: {
      port: 3000,
      proxy: {
        '/upload': apiTarget,
        '/download': apiTarget,
        '/ws': {
          target: apiTarget,
          ws: true,
        },
        '/column-stats': apiTarget,
        '/lineage': apiTarget,
        '/health': apiTarget,
        '/api': apiTarget,
        '/validate-yaml': apiTarget,
        '/download-outlier-cleaned': apiTarget,
      }
    }
  }
})
