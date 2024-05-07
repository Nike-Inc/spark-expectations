# Spark-Expectations UI - Temporary Readme.md

## Available Scripts

In the project directory, you can run:

### `npm run dev`

Starts the development server using Vite on port 3000. This allows for live reloading of changes during development.

### `npm run build`

Compiles the TypeScript code using the TypeScript compiler (`tsc`) and then builds the production version of the application using Vite.

### `npm run preview`

Provides a local preview of the production build. This is useful for testing the built version before deployment.

### `npm run typecheck`

Runs the TypeScript compiler to check for type errors in the codebase without emitting any output files.

### `npm run code:fix`

A composite command that first fixes linting issues, formats the code using Prettier, and then performs a type check.

### `npm run lint`

Runs both ESLint and Stylelint to check for any linting errors in TypeScript/JavaScript files and CSS files, respectively.

### `npm run lint:fix`

Fixes linting issues automatically in both TypeScript/JavaScript and CSS files.

### `npm run lint:eslint`

Runs ESLint on all TypeScript and JSX files in the project, caching the results for faster subsequent linting.

### `npm run lint:eslint:fix`

Automatically fixes fixable linting issues in TypeScript and JSX files.

### `npm run lint:stylelint`

Runs Stylelint on all CSS files in the project, caching the results to speed up future linting.

### `npm run lint:stylelint:fix`

Automatically fixes fixable linting issues in CSS files.

### `npm run prettier`

Checks if TypeScript and JSX files are formatted according to Prettier's rules.

### `npm run prettier:write`

Formats TypeScript and JSX files according to Prettier's rules.

### `npm run vitest`

Runs the Vitest test runner to execute tests.

### `npm run vitest:watch`

Starts the Vitest test runner in watch mode, re-running tests as files change.

### `npm run test`

A comprehensive script that runs type checks, checks formatting, lints files, runs tests, and builds the project. This is typically used to ensure that all checks pass before committing or deploying code.

These scripts are essential for maintaining code quality and ensuring that the application functions as expected before deployment.
