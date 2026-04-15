import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 3000,
    allowedHosts: [
      'nyc-taxi.app',
      'www.nyc-taxi.app',
      'api.nyc-taxi.app',
      'airflow.nyc-taxi.app',
      'mlflow.nyc-taxi.app'
    ],
    watch: {
      ignored: ['**/tsconfig*.json', '**/vite.config.ts']
    }
  }
})