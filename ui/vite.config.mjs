import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tsconfigPaths from 'vite-tsconfig-paths';
import istanbul from 'vite-plugin-istanbul';

export default defineConfig({
    plugins: [react(), tsconfigPaths(), istanbul({
        include: [
            'src/**/*.ts',
            'src/**/*.tsx',
        ],
        exclude: [
            'node_modules/**',
            'tests/**',
            '**/__*',
            '**/*.mock.ts',
            'test-utils/__mocks__/**',
            'postcss.config.cjs',
        ],
        extension: [ '.ts', '.tsx' ],
        cypress: false,
        requireEnv: false
    })],
    test: {
        globals: true,
        environment: 'jsdom',
        setupFiles: './vitest.setup.mjs',
    },
    coverage: {
        provider: 'v8',
        reporter: ['text', 'html'],
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
