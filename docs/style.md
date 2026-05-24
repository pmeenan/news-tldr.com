# Code Style Guide

This document defines the coding standards, formatting rules, and toolchains for the news-tldr.com project.

## Python (Pipeline)

- **Linter & Formatter**: Use [Ruff](https://github.com/astral-sh/ruff) for linting and formatting. It consolidates Black, Flake8, isort, and pyupgrade rules to enforce **PEP 8** style guidelines and standard conventions.
- **Style Rules**:
  - Max line length: 120 characters.
  - Quote style: Double quotes `"` for strings unless single quotes prevent escaping.
  - Imports: Grouped by standard library, third-party, and local imports, sorted alphabetically within each group.
- **Type Annotations**:
  - All functions and public methods must have complete type annotations.
  - Prefer modern Python type syntax (e.g., `list[str]` instead of `List[str]`, `str | None` instead of `Optional[str]`).
  - Import `from __future__ import annotations` at the top of every file to enable postponed evaluation of type annotations.
- **Best Practices**:
  - Use parameterized queries for all SQLite interactions (no string formatting/concatenation for SQL).
  - Explicitly wrap I/O operations (like `socket.getaddrinfo` or heavy file reads) in `asyncio.to_thread` when executing in an async event loop.
  - Follow the fail-safe fallback pattern: log component-level failures and proceed with baseline content instead of crashing the run.

## Frontend (Astro, JS, CSS)

- **Formatter**: Use [Prettier](https://prettier.io/) for formatting HTML, CSS, JavaScript, and Astro components.
- **Linter**: Use [ESLint](https://eslint.org/) for JavaScript/Astro logic. Setup will be completed during Milestone 4 once the frontend codebase in `site/` is initialized.
- **JavaScript / TypeScript**:
  - Use ES6+ features (arrow functions, destructuring, template literals, async/await).
  - Treat all user-generated/external JSON fields as untrusted. Ensure auto-escaping is active and apply HTML sanitization where raw content is rendered.
- **Vanilla CSS**:
  - Use modern CSS variables (custom properties) defined in a central theme.
  - Follow a mobile-first responsive layout design.
  - Avoid utility-class frameworks (like TailwindCSS) to maintain direct control over clean, custom style sheets.

## Tooling Commands

To check Python style and format:
```bash
# Run Ruff lint check and formatting checks
./.venv/bin/python -m ruff check .
./.venv/bin/python -m ruff format --check .

# To apply fixes automatically
./.venv/bin/python -m ruff check --fix .
./.venv/bin/python -m ruff format .
```
