# AGENTS.md

**Agent Instructions – Baseline for This Repository**
This file provides specific guidance for AI coding agents working in this project.  

**Core Rule**: Stay strictly focused on the exact task requested. Do not expand scope, add extra features, or over-deliver unless explicitly asked.

## How to Approach Any Task
- Read the user's request carefully and implement **only** what is asked.
- Do not add "nice-to-have" improvements, refactoring, new tests, or extra documentation unless the request specifically mentions them.
- If the request is ambiguous, ask for clarification instead of making assumptions.
- Keep changes minimal, surgical, and targeted.
- After completing the requested change, stop. Do not suggest or implement additional work.

## Repository Guidelines (Summary for Agents)

### Project Structure & Module Organization
- All source code belongs in the `/src` directory.
- All variables and credentials are defined in `.env.example`. Copy `.env.example` to `.env` for local development and testing. Never commit secrets.

### Build, Test, and Development Commands
- Use the project's configured build and package manager for dependency installation and running commands.
- Run tests using the project's test runner.
- Format code using the project's configured formatter.
- Build container: `docker build .`

**Docker Maintenance**:
- Maintain the `Dockerfile` and `docker-compose.yml` files so they remain up to date and functional.
- Ensure the build section in `Dockerfile` accurately reflects current dependencies, runtime version, and entrypoint.
- Update `docker-compose.yml` whenever environment variables, ports, volumes, or services change.
- Any code or dependency changes that affect the container must include corresponding updates to `Dockerfile` and/or `docker-compose.yml`.

**Dependency Management**:
- Maintain the primary dependency file (`pyproject.toml`, `package.json`, `go.mod`, `Cargo.toml`, `composer.json`, etc.) as the single source of truth.
- Keep any generated lockfiles or exported files synchronized when dependencies change.
- When adding dependencies, use the project's official package manager commands.
- Never edit generated dependency files manually.

### Documentation Guidelines
- Keep `README.md` up to date at all times.
- Maintain a high-level `ARCHITECTURE.md` file that describes the overall system design, major components, data flows, and key decisions.
- When making changes that affect how the project works, update the relevant documentation in the same commit/PR.

### UI Guidelines
- Adhere to WCAG and Section 508 Standards for ADA Compliance
- The official Clemson University color palette is anchored by Clemson Orange (RGB: 245, 102, 0) and Regalia (purple, RGB: 82, 45, 128).
These primary colors are supported by neutrals including Goal Line (white, RGB: 255, 255, 255) and College Avenue (dark gray, RGB: 51, 51, 51)
- More standards available at https://www.clemson.edu/brand/web/
  
### Stock Header Template
Use the following standard stock header at the top of **every new or significantly modified file**. Replace placeholders as needed.

**Python example:**
```python
"""
File: src/example_module.py
Purpose: Brief one-line description of the file's responsibility.
Created: YYYY-MM-DD
Author: [Your Name / Team]
Last Changed: [Your Name] Issue: #123
"""
```
**Go example:**
```go
// File: src/example.go
// Purpose: Brief one-line description of the file's responsibility.
// Created: YYYY-MM-DD
// Last Changed: [Your Name] Issue: #123
```
**Rust example:**
```rust
//! File: src/example.rs
//! Purpose: Brief one-line description of the file's responsibility.
//! Created: YYYY-MM-DD
//! Last Changed: [Your Name] Issue: #123
```
**TypeScript / JavaScript example:**
```javascript
/**
 * File: src/example.ts
 * Purpose: Brief one-line description of the file's responsibility.
 * Created: YYYY-MM-DD
 * Last Changed: [Your Name] Issue: #123
 */
```
**PHP example:**
```php
<?php
/**
 * File: src/example.php
 * Purpose: Brief one-line description of the file's responsibility.
 * Created: YYYY-MM-DD
 * Last Changed: [Your Name] Issue: #123
 */
```

### Commenting Guidelines
- Keep comments concise and minimal.
- Add only a standard stock header at the top of every new or significantly modified file.
- Comment **only** when the logic is not obvious from reading the code.
- Comment important decisions, trade-offs, or compromises that need to be addressed later.
- Every comment must include the date (YYYY-MM-DD) and reference any relevant issue number (e.g., `#123`).
- Avoid redundant comments that simply restate what the code does.

### Logging Guidelines
- Use the project's standard logging approach.
- **INFO level**: Use sparingly — only for high-level important events.
- **WARNING level**: Use for recoverable issues and situations that require attention.
- **ERROR level**: Use for errors and failed operations.
- **DEBUG level**: Use heavily for detailed tracing, decision paths, and internal state — especially in complex logic.
- Configure log levels appropriately per environment (more verbose in development).
- Do not log sensitive information.

### Commit & Pull Request Guidelines
- Use short, imperative commit messages.
- Keep commits narrowly scoped.
- In PRs: explain the change, note any runtime or configuration impact, link issues, and include test evidence.

### Security & Configuration Tips
- Store all credentials in `.env` only.
- Never commit secrets or generated state files.
- Follow least-privilege principles for any external integrations.

## Language-Specific Guidelines

### Python
- Use **Poetry** as the primary dependency manager.
- Install dependencies: `poetry install --with dev`
- Activate environment: `eval $(poetry env activate)` or prefix commands with `poetry run`
- Run tests: `poetry run pytest`
- Format code: `poetry run black src tests main.py` (90-character line length)
- Target Python 3.12+
- Always include type hints and use explicit imports.
- Naming: `snake_case` for modules/functions/variables, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.

### Go
- Use `go mod` for dependency management (`go get`, `go mod tidy`).
- Run tests with `go test ./...`
- Format code with `gofmt` or `go fmt`
- Target idiomatic Go style: short, clear variable names, explicit error handling.
- Use structured logging (e.g., `log/slog` or `zerolog`) with appropriate levels.
- Write godoc-style comments above exported items when needed.

### Rust
- Use `Cargo` for dependency management (`cargo add`, `cargo build`).
- Run tests with `cargo test`
- Format code with `cargo fmt` and lint with `cargo clippy`
- Follow Rust idioms: ownership, borrowing, and explicit error handling (`Result`/`Option`).
- Use the `log` crate or `tracing` for logging with heavy debug output.
- Prefer self-documenting code; add comments sparingly for complex logic.

### Node.js / TypeScript
- Use `npm` or `yarn` / `pnpm` for dependency management (`npm install`, `npm ci`).
- Run tests with the project's test command (e.g., `npm test`).
- Format and lint with the configured tools (e.g., Prettier + ESLint).
- Prefer TypeScript with strict settings when applicable.
- Use a mature logger (e.g., Pino or Winston) instead of `console.log`.
- Follow async/await patterns and proper error handling.

### PHP
- Use Composer for dependency management (`composer install`, `composer require`).
- Run tests with the project's test runner (e.g., PHPUnit).
- Follow PSR-12 coding style where possible.
- Use a logging library such as Monolog with appropriate levels.
- Prefer clear, self-documenting code; add comments only for non-obvious logic or decisions.
- Use type hints and strict types when supported by the PHP version.

## Strict Agent Rules – No Scope Creep
- **Focus only** on the exact request. If the task is "add a function to validate X", do only that.
- Do not refactor existing code unless explicitly asked.
- Do not add new dependencies without explicit approval.
- Do not improve or optimize code outside the requested change.
- When in doubt, do less rather than more.
- After finishing the requested task, clearly indicate completion and wait for further instructions.

**Remember**: The goal is reliable, incremental progress. Over-delivering creates unnecessary review burden.

Refer to the full project README and other documentation for additional context when needed, but always prioritize the exact task given.

Last updated: March 2026
