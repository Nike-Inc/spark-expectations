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
        exclude: [
            '**/index.ts',
            '**/node_modules/**',
            '**/tests/**'
        ]
    }
});
