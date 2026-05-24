# Agent Instructions and Project State

This file serves as the coordinator and handoff state for AI agents working on this project. Read this before starting any task.

## Rules of Engagement

1. **No Fluff.** Do not write long explanations or pleasantries. Be concise and direct.
2. **Read First.** Before doing anything, read [README.md](file:///home/pmeenan/src/news-tldr.com/README.md), [docs/design.md](file:///home/pmeenan/src/news-tldr.com/docs/design.md), and [docs/plan.md](file:///home/pmeenan/src/news-tldr.com/docs/plan.md).
3. **Verify.** Run all tests and build steps before claiming a task is done.
4. **Keep Git Clean.** Update [.gitignore](file:///home/pmeenan/src/news-tldr.com/.gitignore) whenever new tooling, dependencies, build outputs, caches, or generated runtime artifacts are introduced. For example, if Astro or another Node-based toolchain is installed, ignore `node_modules/` and any framework-specific generated directories that should not be committed.
5. **Handoff Protocol.** When the user requests a handoff (via `/handoff` or similar wrapping-up language):
   - Update [docs/design.md](file:///home/pmeenan/src/news-tldr.com/docs/design.md) with any architectural modifications.
   - Update [docs/plan.md](file:///home/pmeenan/src/news-tldr.com/docs/plan.md) to mark completed items and adjust future tasks.
   - Update the **Current State & Handoff** section in this file (`AGENTS.md`) to clearly state what was done and what the incoming agent must do.

---

## Directory Structure

- [README.md](file:///home/pmeenan/src/news-tldr.com/README.md) - Public project description and user guides.
- [AGENTS.md](file:///home/pmeenan/src/news-tldr.com/AGENTS.md) - This file. Contains rules, structure, and current agent state.
- [docs/design.md](file:///home/pmeenan/src/news-tldr.com/docs/design.md) - System architecture and data flow.
- [docs/plan.md](file:///home/pmeenan/src/news-tldr.com/docs/plan.md) - Living build plan and checklists.
- [config/feeds.json](file:///home/pmeenan/src/news-tldr.com/config/feeds.json) - Seed RSS/Atom feed list.
- [config/categories.json](file:///home/pmeenan/src/news-tldr.com/config/categories.json) - Category definitions and sort order.

---

## Current State & Handoff

### State on May 24, 2026

- **Completed:** Milestone 0 (Architecture & Data Contracts) is done. All design decisions have been made and documented.
- **Key Decisions Made:**
  - **Stack**: Python pipeline + Astro presentation + SQLite state database.
  - **Event model**: Flat events (no topic registry). Articles can only belong to one event (one-to-many relationship). Optional `thread` tags for linking related events.
  - **Incremental processing**: SQLite tracks article→event assignments. Aggregation processes only unassigned articles. Editorial regenerates only updated events.
  - **Concurrency**: Lock file with PID check and watchdog timeout (default 30 min).
  - **HTTP policy**: Desktop Chrome UA, per-domain rate limiting, robots.txt respect, exponential backoff.
  - **No headless browser** in initial implementation.
  - **Political framing**: Neutral TL;DR + transparent left/right perspectives for clearly political stories.
  - **LLM calls**: Batched for aggregation (headline + brief paragraph summary), per-event for editorial (full article text).
  - **Categories**: Externalized to `config/categories.json` (11 categories).
  - **Article directories**: Keyed on publish date, with fetch-date fallback.
  - **Presentation**: Rolling time window, not fixed daily editions.
- **In Progress:** Nothing. Ready to start Milestone 1.
- **Next Steps:**
  1. Start Milestone 1: Data Collection (see [docs/plan.md](file:///home/pmeenan/src/news-tldr.com/docs/plan.md)).
  2. First tasks: set up Python project, initialize SQLite schema, implement pipeline locking, then feed fetching.
  3. Populate `config/feeds.json` with real RSS/Atom sources.
