# Agent Instructions and Project State

This file serves as the coordinator and handoff state for AI agents working on this project. Read this before starting any task.

## Rules of Engagement

1. **No Fluff.** Do not write long explanations or pleasantries. Be concise and direct.
2. **Read First.** Before doing anything, read [README.md](file:///home/pmeenan/src/news-tldr.com/README.md), [docs/design.md](file:///home/pmeenan/src/news-tldr.com/docs/design.md), and [docs/plan.md](file:///home/pmeenan/src/news-tldr.com/docs/plan.md).
3. **Verify Appropriately.** Run the tests, linters, build steps, or smoke checks that are appropriate for the risk and scope of the session's changes before claiming a task is done. Code, config, dependency, schema, pipeline behavior, generated output, or presentation changes require targeted verification and any relevant full build/test commands. Documentation-only or planning-only changes do not require the full test suite unless they affect executable examples or setup instructions; use review or lightweight validation as appropriate. Always state what verification was run, or explicitly state why none was needed.
4. **Keep Git Clean.** Update [.gitignore](file:///home/pmeenan/src/news-tldr.com/.gitignore) whenever new tooling, dependencies, build outputs, caches, or generated runtime artifacts are introduced. For example, if Astro or another Node-based toolchain is installed, ignore `node_modules/` and any framework-specific generated directories that should not be committed.
5. **Handoff Protocol.** When the user requests a handoff (via `/handoff` or similar wrapping-up language):
   - Update [docs/design.md](file:///home/pmeenan/src/news-tldr.com/docs/design.md) with any architectural modifications.
   - Update [docs/plan.md](file:///home/pmeenan/src/news-tldr.com/docs/plan.md) to mark completed items and adjust future tasks.
   - Update the **Current State & Handoff** section in this file (`AGENTS.md`) to clearly state what was done and what the incoming agent must do.
6. **Dependencies.** Before adding any new dependency: scan it using security tools, add it to `requirements.txt`, and update [README.md](file:///home/pmeenan/src/news-tldr.com/README.md) with setup instructions.
7. **Pipeline CLI Feedback.** All long-running pipeline operations must support a `--verbose` flag that streams incremental progress/status to stderr while preserving machine-readable final output on stdout.


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

- **Completed:** Milestone 0 (Architecture & Data Contracts) and Milestone 1 (Data Collection), including the Stage 1 review, hardening, feed expansion, CLI quality-of-life commands, lead-image sidecar support, and a code-review pass on the collector and scrapers.
- **Stage 1 fixes & enhancements:**
  - **Decompression Bug Fixed:** `pipeline/http_client.py` removes stale compression headers (`Content-Encoding`, original `Content-Length`, `Transfer-Encoding`) from decompressed response objects to prevent downstream decompression errors.
  - **Robots.txt Feed Exemption:** Feeds explicitly configured by the operator are fetched with `check_robots=False`; robots.txt checks remain enforced for individual article page and image fetches.
  - **Date Filters & Automated Cleanup:** `pipeline/collect.py` ignores feed entries older than the staging retention window, and `cleanup_old_staging_data` removes old archived-event staging JSON and image sidecars while preserving article rows and fingerprints in SQLite.
  - **Article Extraction Recall:** Full-page extraction tries recall/default/precision `trafilatura` modes and keeps extracted text only when it improves on feed content.
  - **Concurrency Optimizations:** Feed/article concurrency defaults are high (100 feeds, 1000 articles), with per-domain async locks preserving rate limits.
  - **Custom Scraper Integration:** `config/feeds.json` supports `feed_type: "scraper"` with `fetch.scraper_module`. AP News and MotorTrend scrapers generate feed-like entries and optional `image_url`; the collector, not the scrapers, downloads selected images.
  - **Feed Catalog Expansion:** `config/feeds.json` now has 83 enabled sources, including 2 scraper sources. `config/source-policy.json` has 83 matching entries. The May 24 expansion added NBC News, Politico, The Hill, CNBC, MarketWatch, Scientific American, MIT Technology Review, Phys.org, NASA, Variety, The Hollywood Reporter, Billboard, DW, France 24, Electrek, Jalopnik, CBS Sports, CNET, 9to5Mac, and 9to5Google. Reuters, USA Today, and Axios were evaluated and skipped (Reuters and Axios are anti-bot/auth-gated; USA Today's RSS endpoints now serve HTML).
  - **Collector / Scraper Review (May 24):**
    - `_article_from_entry` now returns `(article, candidates)` so `_image_candidates` never enters the article dict written to disk or SQLite.
    - The article JSON write is wrapped to unlink the image sidecar if the JSON write fails, avoiding orphaned sidecars.
    - Removed the no-op `_fetch_article_page` helper; article-page GETs go through `self.client.get` directly.
    - Renamed `timeout_seconds_to_timedelta` → `_minutes_to_timedelta` to match its actual unit.
    - `collect_once` initializes `stats: dict[str, int] = {}` before the inner `try` and drops the `locals()`-based fallback in `finally`.
    - `migrate()` now runs inside the `PipelineLock` (was previously outside).
    - Summary fallback (`_summary_fallback`) truncates at the last whitespace within 300 chars and appends `…` instead of cutting mid-word.
    - `pipeline/cli.py` replaces the `progress = lambda` shim with a nested `def progress(...)` to satisfy ruff E731.
    - **Scraper hardening:** `pipeline/scrapers/__init__.py` now restricts `scraper_module` config values to the `pipeline.scrapers.*` namespace. `pipeline/scrapers/ap.py` and `pipeline/scrapers/motortrend.py` require resolved URLs to stay on the configured `site_url` host and to match an anchored path regex (`^/article/` for AP, `^/(?:news|features|reviews)/` for MotorTrend); previous unbounded substring matches allowed cross-domain link bleed and over-broad path matches.
  - **Lead Image Capture:** Collection extracts image candidates from RSS media/enclosure tags, feed HTML, scraper metadata, and article Open Graph/Twitter metadata, then stores one supported image sidecar (`.jpg`, `.png`, `.webp`, `.gif`) next to the article JSON.
  - **CLI Operations:** `collect --verbose` streams progress/status to stderr while final stats JSON stays on stdout. `clean-data --yes` removes local generated collection state and refuses to run under an active pipeline lock unless `--ignore-lock` is provided.
  - **User-Agent Update:** `PoliteHTTPClient` uses a modern desktop Chrome User-Agent string.
  - **Primary Lead Image Fallback Restriction:** If explicit primary lead image metadata is present and the fetch fails or is blocked (e.g. by robots.txt), the collector does not fall back to other candidate images for that article.
  - **Origin Circuit Breaker**: If image fetches from a specific scheme + host origin experience 3 consecutive failures (due to HTTP status >= 400, connection errors, timeouts, or robots.txt blocks), the origin is disabled for the remainder of the collection pass, and subsequent image requests to it are skipped immediately.
  - **Handoff cleanup:** `beautifulsoup4` and `h2` are declared in both `requirements.txt` and `pyproject.toml`; README setup notes mention scraper and HTTP/2 dependencies. HTTP retry messages now use the collector progress callback instead of unconditional stderr output. `config/pipeline.json` has explicit collection timeouts: 10s connect, 10s read, 15s total decoded download.
  - **Post-iteration review pass (May 24, late):**
    - `collect_once` wraps `state.finish_run` in its own try/except so a finish_run failure cannot mask the original collection exception.
    - The two duplicate inner `collect_with_sem` defs in `_collect_feed` are replaced by a single `Collector._gather_entries` helper.
    - `_entry_image_candidates` guards each per-item loop (media_content, media_thumbnail, links/enclosures, summary/description HTML, content) so a single malformed feed item doesn't crash candidate extraction.
    - `import time` hoisted to module scope in `pipeline/collect.py` (was duplicated in 3 methods).
    - `pipeline/http_client.py` `_respect_rate_limit` carries an explanatory comment documenting the intentional per-domain lock-held-across-sleep serialization tradeoff.
    - `pipeline/http_client.py` `_robots_policy` now passes `domain.lower()` through `sanitize_id()` for the robots cache filename.
    - `pipeline/scrapers/{ap,motortrend}.py` added `_looks_like_headline_link` that requires the anchor to live inside an `<article>`/`h1`–`h6` ancestor or contain a descendant heading element; the prior ">10 chars" heuristic was letting site-chrome anchors (e.g. "Sign in", subscribe CTAs) through. Existing AP scraper test fixtures were updated to realistic headline-wrapped markup.
    - Removed extra blank line in `pipeline/state.py` between `find_article_by_url_or_guid` and `insert_article`.
    - `docs/design.md` UA example bumped to `Chrome/148.0.0.0` and marked illustrative.
    - **New tests** (`tests/test_collect.py`): 6 cases for `_best_srcset_url` (w-descriptor, x-descriptor, missing, malformed, empty, mixed w/x); 6 cases for `find_article_by_url_or_guid` covering both intentional canonical/url cross-match directions, plus guid match and no-match.
    - Confirmed `Collector._fetch_article_image` circuit-breaker behavior is already covered by `test_fetch_article_image_disables_origin_after_three_failures` at `tests/test_collect.py:1094`.
- **Current local dataset:**
  - Run logs are recorded in `data/staging/fetch-log/2026-05-24.jsonl`.
  - Staging data contains **2,230 article JSON files** in `data/staging/articles/`.
  - `data/state/pipeline.db` has **2,230 article rows**, all currently unprocessed (`event_id IS NULL`). The `feeds` table may retain historical source rows from earlier syncs; use `config/feeds.json` as the current enabled source list.
  - There are currently **2,057 image sidecars** in `data/staging/articles/`.
- **Verified in this handoff:**
  - `./.venv/bin/python -m pytest -q` passed (`72 passed` after the late-May-24 review pass added 12 tests).
  - `./.venv/bin/python -m compileall -q pipeline tests` passed.
  - `./.venv/bin/ruff check .` passed.
  - `./.venv/bin/python -m pipeline.cli init-db` passed.
  - `./.venv/bin/python -m pipeline.cli list-feeds | wc -l` returns 83 (`config/feeds.json` and `config/source-policy.json` confirmed 1:1 on `source_id`).
  - Full live `collect` was not rerun during this final handoff pass.
  - `./.venv/bin/pip-audit -r requirements.txt` passed after adding `beautifulsoup4` and `h2`.

- **Next Steps:**
  1. Start Milestone 2: Story Aggregation. Read unprocessed articles (`event_id IS NULL`) from the state database.
  2. Implement near-duplicate reprint detection (headline similarity, exact text, or URL hashes).
  3. Design the batched LLM classification and event grouping prompt using headline, brief paragraph summary, source, publish date, and active event context.
  4. Run `collect --verbose` when refreshing the corpus. The 20 new sources added on May 24 (NBC, Politico, The Hill, CNBC, MarketWatch, Scientific American, MIT Technology Review, Phys.org, NASA, Variety, The Hollywood Reporter, Billboard, DW, France 24, Electrek, Jalopnik, CBS Sports, CNET, 9to5Mac, 9to5Google) have not been fetched yet. Use `clean-data --yes` first only when a fully fresh local collection is desired.
