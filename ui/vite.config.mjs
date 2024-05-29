import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tsconfigPaths from 'vite-tsconfig-paths';

export default defineConfig({
    plugins: [react(), tsconfigPaths()],
    test: {
        globals: true,
        environment: 'jsdom',
        setupFiles: './vitest.setup.mjs',
    },
    coverage: {
        provider: 'c8',
        reporter: ['text', 'html'],
        // TODO: Exclude test files from coverage is not working
        exclude: [
            '**/index.ts',
            '**/node_modules/**',
            '**/tests/**',
            'ui/test-utils/**',
            '**/__mocks__/**',
        ]
    },
    server: {
        proxy: {
            '/login/oauth/access_token': {
                target: 'https://github.com',
                changeOrigin: true,
                rewrite: path => path.replace(/^\/login\/oauth\/access_token/, '/login/oauth/access_token')

            }
        }
    }
});
