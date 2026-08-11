import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // 开发模式下接口与代理播放地址都转发给 Rust 后端（cargo run 默认端口）
      '/api': 'http://127.0.0.1:9527',
      '/stream': 'http://127.0.0.1:9527',
    },
  },
  build: {
    outDir: 'dist',
    target: 'es2022',
  },
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts'],
  },
});
