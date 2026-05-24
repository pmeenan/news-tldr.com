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
6. **Dependencies.** Before adding any new dependency: scan it using security tools, add it to `requirements.txt`, and update [README.md](file:///home/pmeenan/src/news-tldr.com/README.md) with setup instructions.


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

- **Completed:** Milestone 0 (Architecture & Data Contracts) and Milestone 1 (Data Collection), followed by a Stage 1 review-and-hardening pass.
- **Hardening pass changes:**
  - `pipeline/lock.py`: foreign-host locks now always raise (per design they are unverifiable, so the operator must intervene). Added `__aenter__`/`__aexit__` that offload acquire/release via `asyncio.to_thread`, so process-kill waits during stale-lock recovery do not block the event loop.
  - `pipeline/http_client.py`: added a streaming response cap (default 25 MiB) with a `Content-Length` precheck and a new `ResponseTooLarge` exception. Defends against OOM on huge feeds and gzip-bomb decompression.
  - `pipeline/state.py`: replaced the single-string schema with a versioned, append-only `MIGRATIONS` journal; bumped to schema v2. `events` now has `keywords_json`, `entities_json`, `article_count`, `last_editorial_at`, and `confidence`; `item_errors` has `retry_count`. Added indexes `idx_articles_canonical_url`, partial `idx_articles_unassigned`, `idx_events_status_updated`, and `idx_item_errors_item`. Article paths in the DB are now stored relative to the project root for portability.
  - `pipeline/collect.py`: broadened the XML DTD-rejection regex to also cover `ELEMENT`, `ATTLIST`, and `NOTATION` declarations; `_canonicalize_url` now sorts query parameters so reordered URLs canonicalize identically; `collect_once` uses `async with PipelineLock(...)`.
  - `docs/design.md`: documented the response size cap and the append-only migration journal.
- **Tests added during hardening:** foreign-host lock refusal, boot-id-change recovery, async lock context manager, response-size cap (body and `Content-Length`), element-declaration DTD rejection, canonical-URL sort, migration upgrade path (v1 → v2), and migration idempotency.
- **Current implementation notes:**
  - Main commands:
    - `./.venv/bin/python -m pipeline.cli init-db`
    - `./.venv/bin/python -m pipeline.cli collect`
    - `./.venv/bin/python -m pytest -q`
    - `./.venv/bin/pip-audit -r requirements.txt`
  - Runtime outputs under `data/` are ignored by git. The local `data/state/pipeline.db` now records `schema_version` rows `[1, 2]`.
  - Full live collection against every feed was not run during this handoff to avoid a large network/data run.
- **Verified before handoff:**
  - `./.venv/bin/pip-audit -r requirements.txt` passed with no known vulnerabilities.
  - `./.venv/bin/python -m pytest -q` passed (`22 passed`).
  - `./.venv/bin/python -m compileall -q pipeline tests` passed.
  - `./.venv/bin/python -m pipeline.cli init-db` passed and upgraded the local DB from v1 to v2.
- **Next Steps:**
  1. Start Milestone 2: Story Aggregation. The v2 schema already provides the event/error columns and indexes aggregation will need; any further schema additions should be a new entry in `pipeline/state.py::MIGRATIONS`.
  2. Recommended first tasks: add state query helpers for unassigned articles (use `idx_articles_unassigned`), define event JSON write/update helpers, implement deterministic near-duplicate filtering, then design the batched LLM abstraction/prompt.
  3. Before aggregation work, consider running a limited live collection pass (or temporarily disabling most feeds) to produce a small real article set for aggregation fixtures.
