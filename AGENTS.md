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
8. **Respect `is_filtered` column.** All database queries retrieving articles for downstream pipeline stages (e.g. digestion, aggregation, editorial, presentation) must strictly filter by `is_filtered = 0` so that excluded spam, non-news, or media-only content remains globally ignored.


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

### State on September 1, 2026 (Contained Anonymous Reader Sync)

- **Browser flow**: Added an optional masthead sync control that creates a
  256-bit private fragment link, imports it into local storage on another
  browser, removes it from the address bar, unions the last three days of read
  state, and pushes after a four-second idle period. A new page waits for one
  bounded pull before revealing and rendering stories. Later writes are silent
  and do not apply the returned union, while focus, tab, online, and cross-tab
  storage events do not pull or rerender. Opening a new capability link remains
  an explicit pull/apply action. The dialog explains in plain language that
  synchronization is automatic and has no redundant manual-sync action. Readers
  can disconnect one browser without losing local history or explicitly delete
  the shared group.
- **Origin API**: Added dependency-free PHP create/merge/delete endpoints backed
  by a separate SQLite database. Tokens are stored only as SHA-256 hashes;
  requests require an exact allowed Origin and JSON; responses are no-store;
  validation, transactions, retention, and capacity limits live in
  `server/sync/lib.php`.
- **Containment**: The checked-in Nginx route exposes only three exact API
  paths, applies layered request/body/connection limits and short FastCGI
  timeouts, and sends them to a dedicated ondemand PHP-FPM pool capped at three
  32 MiB workers. Application defaults cap the service at 2,000 groups, 100 new
  groups/day, 2,000 reads/group, 256 KiB per request/state, and a 256 MiB SQLite
  page limit. Reads expire after three days, inactive groups after 180 days,
  and a daily cron command prunes retained state.
- **Origin installation**: `scripts/install-sync-origin.sh` puts root-owned PHP
  under `/opt/news-tldr-sync`, state under `/var/lib/news-tldr-sync`, and detects
  the configured Nginx worker identity for the dedicated socket. On this host
  the socket is `pmeenan:pmeenan` mode `0660`; PHP-FPM and Nginx are active and
  the origin smoke check returns the expected HTTP 415 validation response.
- **Production state**: `presentation.reader_sync_enabled` is now `true`.
  Presentation v20 was republished with 3,872 stories and 7,754 managed static
  files. The public homepage contains the sync control and private-link warning;
  a public create/merge/delete lifecycle returned HTTP 201/200/204, and the API
  validation response carries no-store, nosniff, no-referrer, and same-origin
  resource-policy headers.
- **Verification**: `PYTHONPATH=. ./.venv/bin/pytest -q` passed **278 tests**;
  `ruff check .`, `compileall`, `git diff --check`, PHP lint for all sync files,
  JavaScript syntax checking with Node, installer `bash -n`, and
  `validate-data --verbose` passed. `health --verbose` confirms SQLite,
  artifacts, freshness, and public endpoints are good but remains unhealthy
  because the concurrent hourly run had Gemini capacity failures in digestion
  and left 43 editorial events pending. No dependency was added.
- **Files touched**: `pipeline/present.py`, `server/sync/*.php`,
  `deploy/{nginx,php-fpm,cron}`, `scripts/install-sync-origin.sh`,
  `config/pipeline.json`, `tests/{test_present,test_sync_origin}.py`,
  `README.md`, `docs/{design,plan}.md`, and this handoff.
- **Not committed.** The origin service and sync UI are installed and live;
  source changes remain in the working tree.

### State on August 24, 2026 (Global New View + Sharing/Indexing + Color Refinement)

- **Reader preference**: New is now the default New/All mode. The global choice uses the existing `newsTldrViewModeV1` local-storage key, is honored across category sections and later visits, and is reflected in the URL only for the non-default All mode. Three-day/10-second viewed-story behavior is unchanged.
- **Color system**: Removed source-hash colors and lead/secondary shade overrides that made one category appear inconsistent. The combined All view now groups muted tints by category family (World/U.S. browns, Politics/Business olives, Technology/Science/Environment grays, Automotive blue, Health purple, Entertainment orange). Shade depth depends only on distinct source count; focused categories return to the neutral white/tan scale while retaining kicker accents.
- **Indexing and sharing**:
  - Every generated HTML page includes `noindex,follow,noarchive` metadata. `robots.txt` disallows Google, Bing, DuckDuckGo, Apple, Yandex, Baidu, and Petal indexing crawlers while leaving `User-agent: *` allowed for social preview and ordinary access; the sitemap is no longer advertised in robots.
  - Added checked-in `site/assets/social-card.png`, a generated 1200×630 branded news-tldr.com poster. Home/archive/404/story pages emit complete Open Graph and X large-card fields; story pages use their own headline/dek and `og:type=article`.
  - Static validation now requires `robots.txt` and the social poster. Presentation version is `presentation-v3`.
- **Freshness label**: The homepage edition line now says `Updated <build timestamp>` instead of `Built`.
- **Production**: Presentation-only publish completed successfully with **596 stories** and **1,201 managed static files**. Live robots, homepage metadata/toggle state, a representative story card, and the poster returned expected content/HTTP 200. Health is `healthy` with 0 artifact errors.
- **Verification**: `PYTHONPATH=. ./.venv/bin/pytest -q` → **276 passed**; `ruff check .`, `compileall`, `git diff --check`, `validate-data --verbose`, and `health --verbose` passed.
- **Files touched for this work**: `pipeline/{present,paths,operations}.py`, `tests/{test_present,test_operations}.py`, `site/assets/social-card.png`, `README.md`, `docs/{design,plan}.md`, and this handoff. No dependencies were added.
- **Not committed.** The working tree retains earlier uncommitted pipeline and reader-polish work; runtime/build/production output remains outside source control.

### State on August 24, 2026 (Capacity Fallbacks + Concurrent Backlog-First Scheduling)

- **Scope**: Prevented hourly runs from growing an editorial backlog when Gemini 3.7 Flash is capacity-constrained, bounded repeated deduplication work, recovered the live queue, and parallelized collection with bounded backlog processing.
- **Model fallback policy**:
  - Editorial and selective full-quality review now try `gemini-3.7-flash` → `gemini-3.6-flash` → `gemini-3.5-flash`. Retryable transport/429/5xx failures open a five-minute per-model circuit so subsequent calls bypass a saturated tier.
  - Deduplication may additionally fall back to `gemini-3.5-flash-lite`, but Lite cannot authorize a merge and Lite decisions are not cached. Editorial never uses Lite.
  - Empty responses try the next full-Flash tier. Safety-sensitive editorial input that is empty across all full tiers gets one compact digest/key-fact retry, recorded under `editorial-v2-compact` or `editorial-framing-v1-compact`.
  - Live recovery provenance: editorial used 115 Gemini 3.6 calls and 60 Gemini 3.5 calls; deduplication review used 26 Gemini 3.6 calls and 7 Gemini 3.5 calls. No editorial Lite calls occurred.
- **Deduplication controls**:
  - Schema v9 adds `deduplication_reviews`, keyed by canonical event pair, both event `updated_at` values, and prompt version. Unchanged reviewed pairs are skipped; any event update invalidates its cached pair decisions.
  - Production dedup reviews are ordered newest-first and bounded to 40 new pairs and one pass per run. The live pass found 173 candidates, admitted 40 for review, deferred 133, and completed in about 90 seconds instead of overrunning the watchdog. It applied 13 merges and recorded 33 versioned review rows; pairs invalidated by an earlier merge in the same round were skipped.
- **Backlog-first combined run**:
  - `pipeline.cli run` marks interrupted `pipeline_runs` rows failed, performs maintenance, snapshots the maximum article `rowid`, and starts collection in a dedicated thread while editorial/digest/aggregation backlog processing continues.
  - Prior digest/aggregation queries are capped at the snapshot rowid, so concurrently inserted articles cannot move the finish line. If backlog remains, collection is still checkpointed but those new rows are deferred downstream; once the snapshot queues reach zero, the normal downstream pass admits them.
  - SQLite connections now use a 30-second connection/busy timeout so short collection and editorial/usage writes serialize safely under WAL mode.
- **Live recovery**:
  - Stopped the verified pre-fix 12:17 process while it was re-reviewing all 167 candidate pairs, then ran the patched pipeline.
  - Recovered 155 pending editorial events (154 on the first pass); the final safety-sensitive Wired story completed through the compact full-Flash retry. A subsequent full run collected 69/69 feeds, wrote 38 articles, digested 30/30 eligible articles, processed 4/4 aggregation windows, completed 20/20 resulting editorial stories, and published production.
  - Final state: **590 active stories**, **0 pending editorial**, **0 unfiltered/unassigned articles**, **0 running stage rows**, SQLite `quick_check=ok`, artifact validation clean, and homepage/API HTTP 200. Health is `healthy`.
- **Concurrent production verification**:
  - The 13:17 scheduled run exercised the new normal path: collection started immediately after the rowid snapshot, completed without SQLite lock errors, found 39 eligible articles, and downstream digestion/aggregation/editorial/presentation completed normally.
  - The run and health check both exited 0. Artifact validation found 0 errors across 3,579 articles, 2,068 digests, 1,547 events, 596 stories, and 1,200 static files; homepage/API returned HTTP 200. The live pre-run backlog was empty, while unit tests explicitly verify collection overlaps both editorial and snapshot-bounded upstream backlog work.
- **Verification**: `PYTHONPATH=. ./.venv/bin/pytest -q` → **275 passed**; `ruff check .`, `compileall`, and `git diff --check` passed. `validate-data --verbose` and `health --verbose` passed all checks.
- **Files touched for this work**: `.env.example`, `README.md`, `config/pipeline.json`, `pipeline/{llm,digest,aggregate,editorial,cli,state}.py`, `tests/{test_llm,test_state,test_aggregate,test_editorial,test_cli}.py`, `docs/{design,plan}.md`, and this handoff. No dependencies were added. The working tree also retains the earlier uncommitted reader-polish changes.
- **Not committed.** Runtime SQLite/JSON/static/health data and production files remain outside source control.

### State on August 24, 2026 (Reader Revisit + Ranking Polish)

- **Presentation** (`pipeline/present.py`, `config/categories.json`): compact
  navigation labels now fit the desktop header; full category names remain on
  cards and story pages. The homepage has a New/All control beside Latest
  Briefing. Stories that remain at least 55% visible for 10 seconds are recorded
  in local storage for three days and hidden on later visits in New mode; no
  reading state leaves the browser.
- **Ranking** (`pipeline/editorial.py`): active-story indexes now include
  freshness-aware `homepage_rank_score` and `category_rank_score`. All is sorted
  by global-impact rank; category filters re-sort by category-impact rank.
- **Visual hierarchy**: All uses deterministic muted source tints, category
  views use their category color, and tint strength incorporates rank and
  distinct source count. Lead/secondary layout follows the active view's rank.
- **Validation** (`pipeline/operations.py`): active-index ordering validation
  recognizes homepage rank while retaining backward compatibility with older
  indexes.
- **Production**: published presentation v2 with 471 completed stories and 950
  managed files. Homepage, active-story API, and a representative story returned
  HTTP 200 and the homepage contains the new controls/ranking metadata.
- **Verification**: `PYTHONPATH=. ./.venv/bin/pytest -q` → **264 passed**;
  `ruff check .`, `compileall`, and `git diff --check` passed.
- **Operational note**: the pre-existing live pipeline backlog currently has
  127 active/stale events needing editorial work (101 without story artifacts).
  The rebuilt index safely excludes missing artifacts; scheduled runs remain
  responsible for completing that content backlog.
- **Not committed.** No dependencies were added.

### State on August 24, 2026 (Milestone 5 Complete + Hourly Production Operations)

- **Scope**: Completed the final Operations & Quality milestone, installed hourly production scheduling, fixed incremental replay churn and a live watchdog edge case, refreshed the dataset, and published a fully healthy 441-story site.
- **Operational tooling** (`pipeline/operations.py`, `pipeline/cli.py`):
  - `validate-data --verbose` validates feed/source-policy symmetry, category IDs/order, SQLite integrity, prompt provenance, article/digest/event/story schemas, citations/source URLs, active-index parity/ranking, and generated static files. Invalid output is machine-readable and exits nonzero.
  - `health --verbose` checks freshness/status for all six recorded stages, stale running jobs, pending editorial events, latest collection failures, full artifact validation, and the public homepage/API over HTTPS. The atomic result is stored at `data/state/health.json`; failed checks exit nonzero.
  - `llm-usage --hours N` reports calls, input/output tokens, optional cost, and distinct runs grouped by stage/model/prompt. Existing `llm_usage` rows already provide per-run accounting; the live audit found zero rows without a prompt version.
  - `run --dry-run --verbose` is a non-mutating preflight: it acquires/releases the real lock, previews maintenance, counts work for all stages, and validates artifacts with zero network/LLM calls or database/artifact/publish mutations. Aggregation dry-run is now also plan-only and does not instantiate an LLM client.
- **Reliability fixes**:
  - Live expired-process lock recovery testing exposed zombie processes being treated as alive. `pipeline/lock.py` now recognizes `/proc` state `Z`, allowing the watchdog to recover a verified terminated process safely.
  - Sparse aggregation intentionally revisits the newest completed window, but unchanged groups previously rewrote event `updated_at`, causing 100 unnecessary editorial regenerations in the live run. Replays whose articles are already present in the same event are now true no-ops; article assignment SQL also writes only changed rows.
  - Post-aggregation deduplication now runs only when aggregation changed events. Merged loser story JSON is removed with the loser event so stale orphan artifacts do not accumulate.
  - Production concurrency was reduced to `editorial.concurrency=2` and `aggregation.deduplication_concurrency=4`; editorial event context is capped at 40,000 characters to reduce Gemini demand failures during unattended runs.
- **Scheduling and alerting**:
  - Installed user cron entry: `17 * * * * /home/pmeenan/src/news-tldr.com/scripts/run-scheduled.sh`. The system cron daemon is active; cron works without an active desktop/login session.
  - The checked-in schedule source is `deploy/cron/news-tldr.cron`. The wrapper runs the full pipeline/publish and health check, rotates `data/state/scheduled-pipeline.log` at 10 MiB, and exits nonzero for cron's normal error-mail path.
- **Live refresh and publish**:
  - Collection reached **69/69 feeds** with **0 feed failures**, **0 article failures**, and **42 new articles**; 31 eligible articles completed Gemini digestion.
  - Aggregation processed one window with 121 articles, created 15 events, updated existing clusters, and merged five duplicate event splits. The incremental replay fix was added immediately after this run to prevent that existing-event churn from recurring hourly.
  - Editorial cleanup completed all active events despite Gemini 3.7 high-demand 503s. One seven-source political event used the already-validated 3.5 Flash-Lite capacity fallback; its normal citation/schema checks passed and its real model provenance is recorded. Final corpus: **440** stories on 3.7 Flash and **1** on 3.5 Flash-Lite.
  - Removed three generated orphan story JSON files belonging to events deleted by deduplication; future merges clean these automatically.
  - Production now serves **441 active stories**, **890 managed static files**, and zero missing/currently pending editorial stories at [news-tldr.com](https://news-tldr.com/).
- **Final live state**:
  - Events: **1,392** = **441 active** + **951 archived**. Articles: **0 unfiltered/unassigned pending**. Editorial: **0 pending**.
  - SQLite `quick_check`: `ok`. Artifact validation: **3,094 articles**, **1,651 digests**, **1,392 events**, **441 stories**, **441 index entries**, **890 static files**, **0 errors**.
  - Health status: **healthy**; homepage, active-story API, and representative updated story returned HTTP 200 over HTTPS.
- **Tests & verification**:
  - Full suite: `PYTHONPATH=. ./.venv/bin/pytest -q` → **262 passed**.
  - `ruff check .`, `compileall`, `git diff --check`, scheduler `bash -n`, and `pip-audit -r requirements.txt` all passed; no known dependency vulnerabilities.
  - Live watchdog test spawns a verified process, expires its lock, confirms termination/zombie handling, reacquires the lock, and releases it cleanly.
- **Files touched**: `pipeline/{operations,aggregate,cli,config,lock,paths,state}.py`, `config/pipeline.json`, `scripts/run-scheduled.sh`, `deploy/cron/news-tldr.cron`, `tests/{test_operations,test_aggregate,test_cli,test_lock}.py`, `README.md`, `docs/{design,plan}.md`, and this handoff. No dependencies were added.
- **Not committed.** Source changes for Milestone 5 remain in the working tree. Runtime SQLite/JSON/static/log/health data and the installed crontab are outside source control.
- **Next steps**: The launch plan is complete. Ongoing work is operational only: review `data/state/scheduled-pipeline.log`/`health.json` if cron reports a failure, and use the backlog for optional post-launch features.

### State on August 24, 2026 (Milestone 4 Presentation + Automatic Production Publishing)

- **Scope**: Implemented the static presentation stage, integrated it after Editorial in the top-level pipeline, documented automatic publishing, and deployed the current 433-story site to [news-tldr.com](https://news-tldr.com/).
- **Presentation implementation** (`pipeline/present.py`):
  - Dependency-free Python renderer consumes `data/published/active-stories.json`, individual story JSON, category config, feed metadata, and source policy metadata.
  - Builds a 72-hour editorially ranked homepage with client-side category filters; individual story pages with TL;DR, facts, citations, uncertainties, framing, sources, and paywall badges; an active-story archive; 404 page; robots file; sitemap; and public JSON API files.
  - Treats editorial content as untrusted: HTML-escapes all strings, accepts only HTTP/HTTPS source URLs, rejects invalid/traversal story IDs, renders no raw upstream HTML, and emits a strict CSP with same-origin assets.
  - Builds into a temporary sibling directory and replaces ignored `dist/` only after the complete build succeeds.
- **Automatic publish integration**:
  - `pipeline.cli run` now holds one lock across maintenance → collect → digest → aggregate → editorial → presentation/publish.
  - `config/pipeline.json` enables production publishing by default with `site_url=https://news-tldr.com`, `rolling_window_hours=72`, and `publish_dir=/var/www/news-tldr.com`.
  - `run --no-publish` builds without deploying. Standalone `present --verbose` builds and publishes; `present --build-only` previews `dist/`; `--publish-dir` accepts an absolute override.
  - Deployment rejects unsafe/broad/relative/symlink destinations and source symlinks. It copies generated assets/pages before `index.html`, uses `.news-tldr-managed.json` to remove only previously managed stale paths, preserves unknown server files, and writes public files as `0644`.
- **Production state**:
  - Initial build: **874 generated files**, **433 story pages**, **433 stories in the homepage rolling window**.
  - Published **874 managed files** plus the deployment manifest to `/var/www/news-tldr.com/`.
  - HTTPS smoke checks returned **200** for the homepage, a representative story page, and `/api/active-stories.json`; all 875 production files are world-readable.
- **Tests & verification**:
  - New `tests/test_present.py` covers rolling-window/detail-page behavior, XSS escaping and URL rejection, CSP/API/sitemap generation, traversal rejection, safe destination validation, unknown-file preservation, stale managed-file cleanup, and public file permissions.
  - Updated CLI tests cover pipeline ordering, presentation stats/progress, automatic publish defaults, and build-only behavior.
  - Full suite: `PYTHONPATH=. ./.venv/bin/pytest -q` → **255 passed**.
  - Linter: `./.venv/bin/ruff check .` → clean.
  - Patch validation: `git diff --check` → clean.
- **Files touched**: `pipeline/present.py`, `pipeline/{cli,config,paths}.py`, `config/pipeline.json`, `tests/{test_present,test_cli}.py`, `README.md`, `docs/{design,plan}.md`, and this handoff. `dist/` remains ignored; production output is outside the repository. No dependencies were added.
- **Not committed.** Source changes from this presentation/publish pass remain in the working tree.
- **Next steps**:
  1. Configure the actual hourly scheduler for `./.venv/bin/python -m pipeline.cli run --verbose` when unattended runs are desired; automatic publishing is already part of that command.
  2. Add monitoring/alerting for failed pipeline stages and production smoke checks.
  3. Decide whether archived events should retain permanent public pages beyond the current active/stale archive.

### State on August 24, 2026 (Milestone 3 Editorial Complete)

- **Scope**: Implemented the complete per-event editorial stage, integrated it into the top-level pipeline, evaluated it live with Gemini 3.7 Flash, and published the current 433-event active dataset.
- **Implementation** (`pipeline/editorial.py`):
  - Incrementally selects active/stale events when `last_editorial_at` is null or older than `events.updated_at`; supports `--force`, `--limit`, repeatable `--event-id`, configurable concurrency, and bounded per-article/per-event context.
  - Retrieves assigned articles with mandatory `is_filtered = 0`, loads full text or the best retained excerpt/digest fallback, and attaches source-policy bias/reliability metadata.
  - Runs one structured-output `gemini-3.7-flash` call per event with low thinking. General prompt version is `editorial-v2`; the mixed-source political framing decision uses `editorial-framing-v1`.
  - Validates headline/dek/TL;DR cardinality, numeric editorial score, all fact/uncertainty citations against the offered article IDs, and political perspective citations against the matching left/right source-policy labels.
  - Writes `data/published/stories/<event_id>.json` atomically before advancing `events.last_editorial_at`; records LLM usage/errors under stage `editorial`. Per-event failures leave the prior artifact/checkpoint intact.
  - Computes an auditable importance score from Stage 2 global/category newsworthiness, editorial judgment, freshness, reliability, and distinct source count.
  - Regenerates `data/published/active-stories.json` from active/stale SQLite event state, excludes archived events, and sorts by importance.
- **CLI/config integration**:
  - Added `editorial --verbose [--force] [--limit N] [--concurrency N] [--event-id ID ...]`.
  - `pipeline.cli run` now executes maintenance → collect → digest → aggregate → editorial under one lock.
  - Added `editorial.concurrency=8`, `article_char_limit=12000`, and `event_char_limit=60000` to `config/pipeline.json`.
- **Live evaluation**:
  - Initial three-story evaluation covered a high-impact 11-article event, a politically eligible six-source event, and a singleton. Citations and uncertainty handling were strong; a sentence-case instruction was added as `editorial-v2` after headline title-case drift.
  - The first full pass completed 386/433 and isolated 47 Gemini high-demand 503s. Incremental cleanup completed 41/47, then 6/6 at concurrency 1 without regenerating successful stories.
  - An explicit framing-decision evaluation regenerated the five mixed-left/right eligible events. Two surfaced meaningful framing (Trump/Kim diplomacy and the Iryna Zarutska lawsuit); three correctly returned null, including a straight judicial ruling and nonpolitical disaster coverage.
- **Final live state**:
  - **433/433** active events have story JSON and non-null editorial checkpoints; `active-stories.json` contains **433** entries; no missing or orphan artifacts.
  - Current story prompt metadata: **428** `editorial-v2`, **5** `editorial-framing-v1`, all on `gemini-3.7-flash`.
  - Corpus audit found **0** citation, source-set, filtered-article, framing-side, metadata, parity, or index-ordering errors. SQLite `quick_check` is `ok`.
  - Importance range: 0.2855–0.9675. Category counts exactly match the 433 active events.
- **Tests & verification**:
  - Added `tests/test_editorial.py` coverage for incremental/forced selection, archived exclusion, citation rejection, normalization, balanced framing validation, prompt gating, importance audit data, persistence/checkpoints/usage, filtered-article exclusion, and index behavior.
  - Targeted editorial/CLI/state tests: **37 passed**.
  - Full suite: `PYTHONPATH=. ./.venv/bin/pytest -q` → **249 passed**.
  - Linter: `./.venv/bin/ruff check .` → clean.
  - Patch validation: `git diff --check` → clean.
- **Files touched**: `pipeline/editorial.py`, `pipeline/{cli,config,paths,state}.py`, `config/pipeline.json`, `tests/{test_editorial,test_cli}.py`, `README.md`, `docs/{design,plan}.md`, and this handoff. Published JSON and SQLite runtime state are ignored local data.
- **Next steps**:
  1. Begin Milestone 4 Presentation using `data/published/active-stories.json` and `data/published/stories/*.json` as the artifact contract.
  2. Add prompt-version-aware automatic editorial refresh if future prompt migrations should not require `editorial --force`.
  3. Consider a later cost/quality evaluation of 3.5 draft + selective 3.7 editorial review; current 3.7 output quality is good, but transient high-demand 503s required cleanup passes.

### State on August 24, 2026 (Gemini 3.5/3.7 Upgrade and Full Dataset Refresh)

- **Scope**: Upgraded the hosted LLM stack, evaluated a selective stronger-model second pass, hardened live collection/retry behavior, aligned pipeline lookbacks with retention, and refreshed the complete retained dataset so Milestone 3 can start from current data.
- **Model architecture**:
  - Bulk digestion, article classification/impact, grouping, active-event filtering, newsworthiness, and dedupe prescreening use `gemini-3.5-flash-lite` with `thinkingLevel=minimal`.
  - Borderline/conflicting article-filter decisions use `gemini-3.7-flash` with `thinkingLevel=low` and prompt version `article-filter-review-v1`.
  - Strict final event-pair merge adjudication uses `gemini-3.7-flash` with prompt version `deduplication-review-v1`; candidate discovery stays on 3.5 Flash-Lite.
  - `GEMINI_BULK_MODEL` and `GEMINI_REVIEW_MODEL` configure the tiers; `GEMINI_MODEL` remains the bulk fallback. `.env.example` and the ignored local `.env` were updated.
  - Deprecated Gemini 3 sampling parameters (`temperature`, `topP`, `topK`) were removed. Structured output and deterministic response validation remain mandatory.
- **Article-filter review**:
  - Review is triggered when category impact is within `0.10` of `aggregation.min_category_impact`, or when non-`ok` content quality conflicts with a high-impact rationale.
  - Review audit metadata preserves first-pass score/quality/model, reviewer result/rationale/model, and usage for both calls.
  - Across the refresh, 251 of 930 bulk article-digest calls were reviewed; 15 articles were rescued across the threshold and 33 were dropped. This supports selective adjudication rather than using 3.7 for all bulk work.
- **Live evaluation**:
  - Structured-output API smoke tests passed for both new model tiers.
  - Five curated duplicate/non-duplicate event pairs produced the same decisions on 3.5 and 3.7, with 3.7 generally giving tighter rationales.
  - Borderline article comparisons showed 3.7 moving scores in both directions; explicit no-sports/low-signal reviewer guidance improved the expected filtering behavior. The stronger model is therefore intentionally narrow.
- **Collection reliability**:
  - The first refresh exposed shared HTTP/2 connection-state failures across unrelated hosts (20/69 feeds failed). Collection now defaults to HTTP/1.1 and retries transient `httpx.TransportError` failures with bounded backoff.
  - Recovery collection reached all **69/69 feeds** with **0 feed failures**, added 297 missed articles, and reduced image failures from 208 to 11.
- **Retention/lookback correctness**:
  - Digest and aggregation defaults now start at `retention.staging_article_days` (3 UTC days), not a hardcoded one-day horizon.
  - Maintenance uses the same horizon and restores `filtered_expired` rows that are still inside it. This recovered 334 prematurely expired articles; all were digested/filtered and aggregated.
- **Final live state**:
  - SQLite quick check: `ok`.
  - Articles: **4,740** total. Current retained work has **647 assigned/completed** articles and **0 unfiltered pending** articles; all other retained rows are intentionally filtered.
  - Events: **1,384** total = **433 active** + **951 archived**; **1,384** `data/events/*.json` files (exact parity).
  - Active categories: world 77, business 64, technology 62, politics 59, entertainment 39, health 38, us 33, environment 28, science 23, automotive 10.
  - Published/editorial artifacts remain **0** because Milestone 3 has not been implemented.
  - The refresh merged **71** duplicate event clusters. One final dedupe pair review received a Gemini 503 and was logged for retry; it does not leave article work pending.
- **Verification**:
  - Full suite: `PYTHONPATH=. ./.venv/bin/pytest -q` → **239 passed**.
  - Linter: `./.venv/bin/ruff check .` → clean.
  - Patch validation: `git diff --check` → clean.
  - Final maintenance dry-run: restored 0, expired 0, reconciled 0, compacted 0, errors 0.
- **Files touched**: `.env.example`, `README.md`, `config/pipeline.json`, `pipeline/{llm,digest,aggregate,http_client,maintenance}.py`, tests for all affected modules, `docs/{design,plan}.md`, and this handoff. Live SQLite/staging/event data were refreshed and are not source-controlled.
- **Not committed.** The source working tree contains the upgrade and hardening changes.
- **Next steps**:
  1. Begin Milestone 3 Editorial: per-event neutral TL;DR generation, source-attributed key facts/uncertainty, importance ranking, story JSON, and `active-stories.json`.
  2. Use `gemini-3.7-flash` for editorial generation initially, then evaluate whether a 3.5 draft + selective 3.7 review is worthwhile once editorial fixtures exist.
  3. Build the Astro presentation after the editorial artifact contract is implemented.

### State on May 26, 2026 (Dedupe Candidate Gates: Keyword Overlap + LLM Pre-screen)

- **Scope of this pass**: Followed up on the missed-merge analysis (Ferrari Luce split across 2 events, Congo Ebola fragmented into 4 events, etc.) by widening post-aggregation dedupe candidate retrieval without weakening the strict per-pair merge decision.
- **Architecture**: The post-aggregation dedupe pipeline now unions candidate pairs from four complementary gates and feeds each unique pair into the existing per-pair LLM merge call (`should_merge` + confidence ≥ 0.8). Over-merging protection lives entirely in the per-pair call; recall is improved at the candidate-discovery layer.
  1. **Slug/title heuristics** — unchanged: `_base_slug` equality, `_titles_similar` (≥4 shared content tokens, or ≥3 if both titles are short), highly similar article headlines (`_events_have_similar_article_headline`).
  2. **Keyword-overlap gate** (`_keyword_overlap_candidates`) — restricted to one category-group batch at a time (politics_gov, news_business_{us,world,business}, sci_tech, leisure). Pairs events whose top-6 event keywords share ≥2 distinct tokens after stripping a global static stopword list and a per-batch *dynamic* stopword list.
  3. **LLM pre-screen gate** (`_llm_prescreen_candidates`) — one loose-recall LLM call per category-group batch (chunked at 40 events per call). Each event is rendered as `{id, title, keywords(top-6 filtered), headlines(top-3)}`. Prompt is intentionally inclusive; the strict per-pair call is the precision filter. Prompt version: `deduplication-prescreen-v1`. LLM usage is recorded under stage `deduplication_prescreen`.
  4. **Per-pair merge LLM** — unchanged.
- **Dynamic stopword design**: `_dynamic_keyword_stopwords` flags a keyword as "hot" only when it appears in ≥20% of the batch's events **and** in ≥4 events absolute (the absolute floor was raised from 2 to 4 after a live dry-run showed the Ferrari Luce pair being stopworded out of the leisure batch). Batches with fewer than 8 events get no dynamic stopwords at all.
- **New tunables** (`pipeline/aggregate.py`):
  - `DEDUPLICATION_KEYWORD_OVERLAP_MIN = 2`
  - `DEDUPLICATION_KEYWORDS_PER_EVENT = 6`
  - `DEDUPLICATION_HEADLINES_PER_EVENT = 3`
  - `DEDUPLICATION_HOT_STOPWORD_THRESHOLD = 0.2`
  - `DEDUPLICATION_MAX_EVENTS_PER_PRESCREEN_BATCH = 40`
  - `DEDUPLICATION_PRESCREEN_PROMPT_VERSION = "deduplication-prescreen-v1"`
- **`deduplicate_active_events_llm` changes**: SELECT now includes `keywords_json`; events get a `keywords: list[str]` field hydrated. Events are grouped by category-group batch (news_business further split by us/world/business). For each batch the keyword and pre-screen gates run; pairs are unioned into a `frozenset` set so duplicate candidates from multiple gates are deduped before the per-pair merge step. Progress callback now reports gate provenance: `candidate pairs total=X (heuristic=H, keyword_overlap_new=K, prescreen_new=P)`.
- **Tests added** (`tests/test_aggregate.py`, **+11 tests**, all green):
  - `_dynamic_keyword_stopwords` — flags above-threshold words, respects min_events guard, respects absolute_floor (the Ferrari Luce regression test).
  - `_filtered_event_keywords` — drops static + dynamic stopwords, dedupes case-insensitively.
  - `_keyword_overlap_candidates` — pairs distinctive-keyword sharers, respects dynamic stopwords.
  - `_llm_prescreen_candidates` — returns model pairs, drops invalid/self/unknown ids, short-circuits on single-event batches, handles LLM failures via the progress callback.
  - `deduplicate_active_events_llm` end-to-end — verifies keyword-overlap gate alone surfaces a Ferrari Luce-style pair that title heuristics miss, and verifies the LLM pre-screen gate surfaces a pair when keywords don't overlap.
- **Live dry-run on current data** (221 active events): keyword-overlap gate alone finds 13 candidate pairs that the slug/title heuristics would have missed, including the Ferrari Luce pair and 4 of the 4 Congo Ebola event pairs. The 5 of those that are not actual duplicates (e.g. China mine vs. Bangladesh truck, two separate Trump EOs) will be correctly rejected by the strict per-pair LLM. Politics group correctly stopworded "trump" via the dynamic stopword path.
- **Tests & Verification**:
  - Aggregate-only: `PYTHONPATH=. ./.venv/bin/pytest tests/test_aggregate.py -q` → **73 passed**.
  - Full suite: `PYTHONPATH=. ./.venv/bin/pytest -q` → **206 passed**.
  - Linter: `./.venv/bin/ruff check .` → clean.
- **Files touched in this pass**:
  - `pipeline/aggregate.py` (new helpers, tunables, `deduplicate_active_events_llm` updated)
  - `tests/test_aggregate.py` (+11 tests)
  - `docs/design.md` (post-aggregation dedup section rewritten)
  - `AGENTS.md` (this entry)
- **Not committed.** Working tree continues to accumulate uncommitted pipeline work.
- **Next Steps**:
  1. Re-run `aggregate --verbose` against the live data to actually merge the surfaced duplicate pairs (Ferrari Luce, Congo Ebola fragmentation). The dry-run found 13 keyword-overlap candidates; the live run will also exercise the LLM pre-screen which should find additional semantic matches.
  2. Inspect post-merge event count to confirm no over-merging happened (expect ~221 → ~215 events).
  3. Continue with the previously planned work: richer event-metadata LLM pass (Milestone 2), event lifecycle transitions, Milestone 3 Editorial Stage.

### State on May 26, 2026 (Sports Sources & Category Removal)

- **Scope of this pass**: Removed the `sports` category and all sports-only sources from the pipeline. Major sports stories that come through general-news sources (AP, NPR, BBC News, etc.) will now be categorized as `world` or `us` by scope rather than getting a dedicated bucket. Routine sports content from general sources is expected to fall out via the existing low-impact/low-signal filters.
- **Configs**:
  - `config/categories.json` — removed the `sports` entry. Remaining 10 categories: `world`, `us`, `politics`, `business`, `technology`, `science`, `health`, `environment`, `automotive`, `entertainment`.
  - `config/feeds.json` — removed 5 sports-only sources (`bbc-sport`, `abc-news-sports`, `guardian-sport`, `espn-top`, `cbs-sports`). Now 78 enabled sources (was 83).
  - `config/source-policy.json` — removed the matching 5 source-policy entries. Now 78 entries; symmetric with `feeds.json`.
- **Code**:
  - `pipeline/aggregate.py`
    - Dropped `sports` from the `leisure` category group (`CATEGORY_GROUPS`).
    - Removed `"sports": 0.03` weight from deterministic baseline newsworthiness scoring.
    - Removed `sports` from the entertainment/automotive lower-global-vertical penalty set.
    - Cleaned sports-specific language from the grouping prompt (deleted "For sports, group previews..." rule; dropped "sports result" / "a sports tournament" from event-type and not-grouping lists).
    - Cleaned sports-specific language from the newsworthiness and event-merge prompts (removed "routine sports results", "playoff final" example, `major_sports_result` rationale code mention, "sports game" / "sports games" wording).
  - `pipeline/digest.py`
    - Removed `major_sports_event` from the closed rationale-code vocabulary in the digest prompt.
    - Replaced "For sports injuries described with hedged language…" with the more general "For injuries described with hedged language…"; the `unconfirmed_injury` rationale code itself is unchanged.
    - Removed "sports" from the example list of legitimate verticals (now: automotive, technology, science, health, business, entertainment, local-news) and from the personal-health backstory caveat.
  - Digest and aggregation prompt versions were intentionally **not** bumped. Existing v3 digests / v6 aggregations stay valid; the prompt edits only affect future calls.
- **Tests** (`tests/test_aggregate.py`, `tests/test_digest.py`):
  - Replaced sports-category fixtures with `entertainment` (still in the `leisure` group) so the cross-group dedup and category-group-splitting tests still exercise three distinct groups.
  - Updated `_in_same_category_group` assertion to compare `politics` vs `entertainment` (was `politics` vs `sports`).
  - Updated the newsworthiness rationale-code fixture from `major_sports_result` → `entertainment_major_release`.
  - Updated the digest prompt-content assertion from `major_sports_event` → `critical_infrastructure`.
- **Docs**: Updated `docs/design.md` (leisure group definition, rationale-code example, unconfirmed-injury cap note).
- **Live data cleanup**:
  - Set `is_filtered=1` on all **115** articles from the 5 removed sports sources; aggregation_status set to `filtered_sports_source`, `event_id` cleared.
  - Set `is_filtered=1` on the **28** articles from general sources that had been assigned to the now-deleted sports events; aggregation_status set to `filtered_sports_category`, `event_id` cleared.
  - Deleted **70** `category=sports` rows from the `events` table and the matching **70** `data/events/*.json` files (every row had a matching file).
  - Deleted the **5** stale sports-source rows from the cached `feeds` table.
- **Final live state**:
  - Events remaining: **221** (was 291). Categories now: world 48, politics 39, business 32, technology 26, us 20, health 19, environment 17, science 10, entertainment 7, automotive 3. Zero sports.
  - Event JSON files on disk: 221, matching SQLite.
  - Unfiltered articles from removed sports sources: 0.
  - New `is_filtered=1` buckets in `aggregation_status`: `filtered_sports_source` (115), `filtered_sports_category` (28).
- **Tests & Verification**:
  - Full test suite passed: `PYTHONPATH=. ./.venv/bin/pytest -q` → **195 passed**.
  - Linter passed: `./.venv/bin/ruff check .` → clean.
  - `./.venv/bin/python -m pipeline.cli list-feeds | wc -l` → 78; no sports lines.
- **Files touched in this pass**:
  - `config/categories.json`
  - `config/feeds.json`
  - `config/source-policy.json`
  - `pipeline/aggregate.py`
  - `pipeline/digest.py`
  - `tests/test_aggregate.py`
  - `tests/test_digest.py`
  - `docs/design.md`
  - `AGENTS.md`
  - SQLite state and `data/events/` (live cleanup; not source-controlled)
- **Not committed.** Working tree remains intentionally dirty with these changes and all prior uncommitted pipeline work.
- **Next Steps**:
  1. Run a fresh `collect --verbose` to refresh from the 78 remaining sources (no sports feeds will be fetched anymore).
  2. Re-run `digest --verbose` to digest newly collected articles. Existing v3 digests are unaffected. Optionally `--force` if you want general-news sports articles re-scored against the trimmed rationale-code vocabulary.
  3. Re-run `aggregate --verbose`. New aggregations will no longer accept `sports` as a category; major sports stories from general sources should land in `world` or `us`.
  4. Continue with the previously planned work (richer event-metadata LLM pass, event lifecycle transitions, Milestone 3 Editorial Stage).

### State on May 25, 2026 (Aggregation Hardening, Clean Reset, and Verified Rerun)

- **Scope of this pass**: Fixed live aggregation failures and quality issues found after a clean full-pipeline run, reset aggregation artifacts, reran aggregation end-to-end, and inspected the resulting event clusters.
- **Implementation**:
  - `pipeline/aggregate.py`
    - Treats null-like `existing_event_id` values (`null`, `none`, `n/a`, empty strings, etc.) as absent instead of failing validation.
    - Splits aggregation batches by category group and further splits high-volume `news_business` into `world`, `us`, and `business` buckets, chunked to at most 50 articles per LLM grouping call.
    - Tracks `windows_partial_failed` separately and records `category_batch_count` in window stats.
    - Marks standalone opinion groups as `filtered_standalone_opinion` so they do not stay pending forever.
    - Adds deterministic grouping guardrails after LLM validation: weakly connected headline groups are split into smaller components, and an `existing_event_id` is preserved only when the component matches that active event title or already contains an article assigned to that event.
    - Removes `us`/`u`/`s` as meaningful headline-match tokens to avoid U.S.-keyword false positives.
    - Improves post-aggregation dedupe candidate discovery with highly similar article-headline matching, catching duplicate clusters split across categories even when event titles differ.
  - `pipeline/cli.py` / `pipeline/paths.py`
    - `clean-data --yes` now removes `data/events/` and `data/published/` in addition to SQLite state, staged articles, and fetch logs.
  - `config/pipeline.json` / `pipeline/http_client.py`
    - Collection same-origin rate limit is now 1 second instead of 2 seconds.
  - Docs updated in `README.md`, `docs/design.md`, and `docs/plan.md`.
- **Tests & Verification**:
  - Targeted aggregation/CLI tests passed: `PYTHONPATH=. ./.venv/bin/pytest tests/test_aggregate.py tests/test_cli.py -q` → **75 passed**.
  - Full test suite passed: `PYTHONPATH=. ./.venv/bin/pytest -q` → **195 passed**.
  - Linter passed: `./.venv/bin/ruff check .` → clean.
  - Reset aggregation state only (article `event_id` assignments, `events`, `aggregation_windows`, aggregation/dedupe/newsworthiness LLM usage/errors, and `data/events/*.json`), preserving collection and digest artifacts.
  - Reran live aggregation: `./.venv/bin/python -m pipeline.cli aggregate --verbose`.
- **Final live aggregation state**:
  - Aggregation run status: `success`.
  - Windows: **16 completed**, **0 failed**, **0 partial**.
  - Former failing windows:
    - `2026-05-24T15:00:00Z`: processed **20/20** articles, **5** category batches, no errors.
    - `2026-05-25T18:00:00Z`: processed **159/159** articles, **8** category batches, no errors.
  - Event artifacts are consistent: **291** SQLite event rows and **291** `data/events/*.json` files.
  - Article assignment summary: **412** assigned/unfiltered, **135** unassigned/unfiltered, **347** filtered. The unassigned/unfiltered rows are digest-stage pending/failed rows, not aggregation window failures.
  - Spot checks:
    - Rubio India trip is separate from Pope events.
    - Pope AI encyclical sources are grouped together.
    - Pope slavery apology is separate from Pope AI.
    - Hajj and Hezbollah stories are separate from the Iran deal cluster.
    - Iran deal cluster is broad but centered on the same negotiations/backlash/market-reaction arc.
- **Files touched in this pass**:
  - `pipeline/aggregate.py`
  - `pipeline/cli.py`
  - `pipeline/paths.py`
  - `pipeline/http_client.py`
  - `config/pipeline.json`
  - `tests/test_aggregate.py`
  - `tests/test_cli.py`
  - `README.md`
  - `docs/design.md`
  - `docs/plan.md`
  - `AGENTS.md`
- **Not committed.** Working tree remains intentionally dirty with these changes and prior uncommitted pipeline work.
- **Next Steps**:
  1. Implement the second validated LLM pass for richer event metadata (titles, slugs, keywords, entities, optional thread tags) — Milestone 2 checkbox in `docs/plan.md`.
  2. Implement event lifecycle transitions (`active` → `stale` → `archived`).
  3. Consider running `digest --verbose` to address the remaining digest-pending/failed rows before the next aggregation pass.
  4. Begin Milestone 3 (Editorial Stage).

### State on May 25, 2026 (Category-Group-based Window Splitting)

- **Scope of this pass**: Implemented category-group-based window splitting to prevent Gemini attention breakdown, eliminate "mega-event" grouping hallucinations, and keep batches under 40–50 articles.
- **Implementation**:
  - Defined `CATEGORY_GROUPS` partition layout in `pipeline/aggregate.py` to bucket articles into `politics_gov`, `news_business`, `sci_tech`, and `leisure` groups.
  - Restructured the window aggregation loop in `aggregate_once` to process loaded window articles separate per category group.
  - Safely accumulated window execution metrics (`elapsed_ms`, token usage, created/updated event counts) to support window completion tracking, guarding against `None` values to avoid `TypeError`.
  - Updated `deduplicate_active_events_llm` to check category group compatibility (`_in_same_category_group`) instead of strict category equality.
- **Tests & Verification**:
  - Added new unit tests verifying category group splitting and cross-category group deduplication within the same category group in `tests/test_aggregate.py`.
  - Ran `data/scratch/step_by_step_aggregate.py` completely over all 15 sliding windows. Window 14's massive 194-article load was successfully processed in smaller category group batches. Unrelated stories remained properly segregated.
  - All **175 tests** passed successfully (`PYTHONPATH=. ./.venv/bin/pytest`).
  - Linter (`ruff check .`) is clean.
- **Next Steps**:
  - Implement the second validated LLM pass for richer event metadata (titles, slugs, keywords, entities) — Milestone 2 checkbox at `docs/plan.md:121`.
  - Implement event lifecycle transitions (`active` → `stale` → `archived`) — `docs/plan.md:131`.
  - Begin Milestone 3 (Editorial Stage).

### State on May 25, 2026 (Refining Active Events Selection — Bug Fix)

- **Scope of this pass**: Diagnosed and fixed the "mega-event" grouping issue where unrelated events (e.g. Cuba rice shipment, Pakistan train blast, Hajj, and U.S.-Iran peace talks) were merged into a single event.
- **Root Cause**: The proactive active-event matching query checked if an active event's title shared *any* word (excluding stopwords) with the *union* of all article headlines in the window. In large windows, the union of words contains almost all common words, causing almost all active events (over 370 in Window 10) to match, cluttering the prompt and confusing the LLM into false-positive groupings.
- **Refined Selection Criteria**:
  - Modified `pipeline/aggregate.py` to match an active event only if its title shares **at least 2 content-bearing words** (excluding `_KEYWORD_STOPWORDS`) with **at least one individual article's headline** in the current window (or shares the single content-bearing word if the title only has one).
  - This reduced the candidate active-event matches sent to Gemini by 60–90% in large windows (e.g., from 371 to 112 matches in Window 10, and from 281 to 103 in Window 14).
  - Keeps the context window clean, prevents LLM grouping hallucinations, and properly separates unrelated stories (Cuba rice shipment has exactly 2 articles, and Pakistan train blast has exactly 7 articles, kept separate from the Trump-Iran event).
- **Verified in this pass**:
  - Run `data/scratch/step_by_step_aggregate.py` completely to completion over all 15 sliding windows. Verify that the Cuba event remained at exactly 2 articles and Pakistan blast at 6/7 articles.
  - All **172 tests** passed successfully (`PYTHONPATH=. ./.venv/bin/pytest`).
  - Linter (`ruff check .`) is clean.

### State on May 25, 2026 (Cross-Window Grouping & Deduplication)

- **Scope of this pass**: Addressed sliding window boundary splitting by implementing proactive matching (Option 2) and post-aggregation LLM deduplication (Option 1).
- **Proactive Active-Events Matching**:
  - `pipeline/aggregate.py` now queries the database for events updated within the last 48 hours that match the categories of the window articles.
  - Active events are passed to the grouping prompt (`_build_grouping_prompt`), containing their event IDs, categories, and descriptive titles (headlines).
  - The grouping response schema (`_grouping_response_schema`) and `validate_grouping_response` support an optional `existing_event_id` returned by the LLM.
  - `apply_grouping_result` successfully maps the group to `existing_event_id` and merges the window articles into the existing event, updating database assignments and JSON files on disk.
- **Reactive Post-Aggregation Deduplication**:
  - Added suffix conflict (`_base_slug`) and title overlap (`_titles_similar`) heuristics.
  - At the end of the aggregation pass, `deduplicate_active_events_llm` searches for candidate duplicate active events updated in the last 48 hours.
  - Candidate event pairs are sent to a targeted LLM call (`_build_event_merge_prompt`) to decide if they represent the exact same news event.
  - If verified, `merge_events` executes the merge in SQLite (`StateDB.merge_events_into`) and on disk (deletes loser, updates winner JSON article list, and re-runs `upsert_event`).
- **Tests Added**:
  - `tests/test_aggregate.py::test_group_articles_with_gemini_with_active_events` - verifies active events are included in prompt and parsed in grouping JSON.
  - `tests/test_aggregate.py::test_deduplicate_active_events_llm` - verifies post-aggregation pairing, LLM comparison, database merge, and JSON rewrite.
- **Verified in this pass**:
  - `./.venv/bin/python -m pytest` -> **170 passed** (all green).
  - `./.venv/bin/ruff check .` -> clean.

### State on May 25, 2026 (Digest Prompt + Cap Rework — article-digest-v3)

- **Scope of this pass:** Quality evaluation of v2 digests across a stratified sample (39 articles spanning all 11 categories with high/med/low/verylow impact bands plus 6 edge cases), followed by a targeted rewrite of the digest prompt and the impact-cap system. Five sub-agent reviewers each evaluated a batch in parallel; findings were consolidated into a single set of concrete changes and applied.
- **Prompt version bumped from `article-digest-v2` → `article-digest-v3`.** Re-running `digest` (with no `--force`) picks up every existing article because the stale-prompt query matches `digest_prompt_version != current`.
- **`pipeline/digest.py` — controlled rationale-code vocabulary + layered caps:**
  - New cap tiers (computed independently per axis using min-of-applicable):
    - `MULTI_TOPIC_IMPACT_CAP = 0.30` for `live_blog`, `newsletter_roundup` (both axes).
    - `VENDOR_ANNOUNCEMENT_*` asymmetric: global `0.55`, category `0.75` for `vendor_announcement`. Skipped when a HIGH rationale code is also present (e.g. a vendor disclosing an actively-exploited security flaw still scores high).
    - `UNCONFIRMED_INJURY_GLOBAL_CAP = 0.60` for `unconfirmed_injury` — global only, leaving category at its natural value so the sports vertical can still rank the story.
    - `recycled_content` added to `LOW_IMPACT_RATIONALE_CODES` (0.15 cap).
    - `paywalled` joined `thin`/`extraction_noise` in `NOISY_CONTENT_IMPACT_CAP` (0.65). HIGH-rationale bypass still applies.
  - `_apply_impact_caps` rewritten from an `elif` chain (first-match-wins, both-axes-cap-equal) to accumulate all applicable per-axis caps and take the minimum per axis. `impact_capped` is emitted only when at least one axis was actually reduced.
  - `_digest_response_schema` gained an optional `study_stage` enum (`preclinical`, `animal`, `early_human`, `trial_phase`, `approved`, `observational`, `lab_bench`, `not_applicable`, `unknown`). `_normalize_study_stage` drops `not_applicable` and any unrecognized value before persistence so the field is present only when meaningful. `study_stage` flows through both `_persist_pipeline_digest_result` and the reprint-copy path.
- **`pipeline/digest.py` — prompt rewrite (`_build_digest_prompt`).** Major additions:
  - Closed-vocabulary `rationale_codes` list inlined into the prompt.
  - Explicit `novelty` semantics: `breaking` only for time-sensitive events in the last ~48h; research papers are `analysis` or `evergreen`; thin/video-only pages are `low_signal`.
  - Tightened `scope` definition (reach of the actor/phenomenon, not the topic).
  - Discriminative `content_quality` rubric for `paywalled` vs `thin` vs `extraction_noise` vs `non_news`, including paywall-chrome examples like "Democracy Dies in Darkness" / "Read full article".
  - Block on inserting `published_at` year/month/day into the summary.
  - "Do not strengthen the source's language" guard + forbidden formal designations ("PHEIC", "state of emergency", "recession", etc.) unless those exact terms appear in the text.
  - Required explicit advertorial labeling in the summary when `content_quality=non_news` for a promotional reason.
  - Local human-interest → `low_signal` enforcement.
  - Cues for emitting `vendor_announcement`, `live_blog`/`newsletter_roundup`, `unconfirmed_injury`, and `recycled_content`.
  - `study_stage` instruction with `not_applicable` opt-out.
- **`tests/test_digest.py`:** 11 new tests (paywalled noise cap, paywalled + HIGH bypass, paywalled + LOW stacking, vendor asymmetric cap, vendor + HIGH bypass, multi-topic 0.30 cap, unconfirmed-injury global-only cap, recycled_content LOW cap, study_stage persistence, study_stage invalid-value drop, study_stage `not_applicable` drop). Existing `article-digest-v2` references bumped to `v3`; intentional v1 stale-prompt setup at `tests/test_digest.py:458/471` left untouched. Prompt-content assertions updated to v3 phrasing.
- **Verification of v3 against the same corpus (after the user re-ran `digest`):**
  - 938 articles re-digested at v3 (38 still at v2: video/non-extraction pre-filtered articles that don't go through the digest path).
  - Distribution shifts vs v2: `content_quality=paywalled` 0 → 27; `extraction_noise` 23 → 52; `novelty=low_signal` 40 → 116; `novelty=breaking` 294 → 246; `novelty=analysis` 60 → 156; `impact_capped` events ~40 → 176. `study_stage` populated on 49 research articles spanning the full enum (`observational` 19, `lab_bench` 15, `animal` 6, `unknown` 5, `trial_phase` 2, `early_human` 1, `preclinical` 1).
  - Spot-checks against originally-flagged articles confirmed fixes for: Google I/O vendor announcement (now 0.55/0.75 + capped), Guardian live-blog (now 0.30/0.30), Ars Technica truncation (now `paywalled`), Pakistan terror attack scope (now `international`), Messi unconfirmed injury (global capped to 0.45, category preserved at 0.75), NASA lunar materials (`study_stage: lab_bench`), Ozempic Reddit study (`novelty: analysis` + `study_stage: observational`), Wells Fargo advertorial (summary now opens with "This is a promotional credit-card review for…"), Durham grasslands restoration (now `low_signal`/`regional`), Flick YC hiring post (`vendor_announcement` + `non_news` → 0.05/0.05).
  - One earlier-flagged "PHEIC hallucination" was a false positive in the v2 review — the Scientific American body does contain the exact phrase.
- **Known minor residual issues (acknowledged, not blocking):**
  - The "do not insert `published_at` year/month/day into the summary" rule is followed most of the time but still drifts occasionally (e.g. the Shomo article summary still says "May 23, 2026" where the body only says "Saturday (May 23)").
  - One WaPo paywalled article was tagged `content_quality=thin` even though the body contains "Democracy Dies in Darkness". Downstream impact is unchanged because the noise-cap path now also fires on `paywalled`, but the bucketing is wrong.
  - `recycled_content` rationale never fired in this corpus (0/938) — either rare in this dataset or the trigger isn't strong enough; revisit if a known recycled-content fixture surfaces.
- **Important:** The `paywalled` → noise-cap addition (final fix of this pass) was made *after* the user re-ran v3 digest. Existing v3 digests in the corpus that exceeded the new cap will not be recomputed unless they are `--force` re-digested. New digests going forward apply the tighter cap automatically. No prompt-version bump was made for this code-only change; consider bumping to `article-digest-v4` if a fresh re-digest is desired.
- **Verified in this handoff:**
  - `./.venv/bin/python -m pytest -q` → **157 passed**.
  - `./.venv/bin/ruff check .` → clean.
- **Files touched (uncommitted, on top of prior digest-refactor handoff working tree):**
  - `pipeline/digest.py` — prompt rewrite, cap rework, study_stage schema/validation/persistence, paywalled-in-noise-cap.
  - `tests/test_digest.py` — 11 new tests, v2→v3 bumps, prompt-content assertions updated.
  - `docs/design.md` — schema example + cap-tier description + May 25 v3 review note.
  - `docs/plan.md` — added Milestone 2 line item for the v3 rework.
  - `AGENTS.md` — this handoff entry.
- **Next Steps (unchanged from prior pass):**
  1. Run a controlled aggregation pass: `./.venv/bin/python -m pipeline.cli aggregate --verbose --limit-windows 1`.
  2. Implement the second validated LLM pass for richer event metadata (titles, slugs, keywords, entities) — Milestone 2 checkbox at `docs/plan.md:114`.
  3. Implement event lifecycle transitions (`active` → `stale` → `archived`) — `docs/plan.md:124`.
  4. Begin Milestone 3 (Editorial Stage).

### State on May 25, 2026 (Post-Digest-Refactor Review)

- **Scope of this pass:** Code review of the standalone-digest refactor (digest now runs as a pre-aggregation stage) and three targeted fixes that came out of the review.
- **Bug fixes:**
  - **Assigned-status preservation under `--force`.** `pipeline/digest.py` previously called `update_article_aggregation_status(..., status="pending")` unconditionally whenever a digest was re-persisted (already-completed branch, generated-digest persist, reprint copy). With `--force`, this reset an article already assigned to an event back to `pending` while leaving `event_id` populated — an inconsistent state. Fixed by introducing a new helper `StateDB.set_article_aggregation_pending_if_unassigned(article_id)` that performs a single SQL UPDATE guarded by `event_id IS NULL`. All three digest call sites and the aggregate window-loop call site now use this helper.
  - **Sliding-window status churn.** `pipeline/aggregate.py:load_window_articles` previously wrote `aggregation_status='pending'` for every eligible article on every window pass. With `window_hours=6, step_hours=1`, each article was rewritten ~6 times per run. The new helper's WHERE clause also guards on `aggregation_status != 'pending'`, making repeated calls idempotent no-ops.
  - **`--force` now bypasses the digest `max_retries` gate** in `_load_article_for_pipeline_digest`. Previously, an operator running `--force` on rows already marked `max_retries_exceeded` would silently get no work done. The retry gate is now `if not force and retry_count >= max_retries`.
- **Intentional non-change:** Reviewer flagged `impact_capped` rationale + `NOISY_CONTENT_IMPACT_CAP` (`pipeline/digest.py:765`) interacting with `AGGREGATION_EXCLUDED_RATIONALE_CODES` (`pipeline/aggregate.py:35`) such that thin/noisy-extracted articles get filtered from aggregation. User confirmed this is the intended behavior — leaving as is.
- **Defensive code kept:** `AGGREGATION_EXCLUDED_RATIONALE_CODES` retains both `"archive_index"` and `"archival_index"`; both spellings are observed in model output.
- **New tests (4, total now 144):**
  - `tests/test_state.py::test_set_article_aggregation_pending_if_unassigned` — verifies the helper preserves assigned status, transitions filtered→pending, and is a no-op when already pending (sentinel reason survives).
  - `tests/test_digest.py::test_digest_force_preserves_assigned_aggregation_status` — `--force` regenerates the digest on an assigned article but leaves `aggregation_status='assigned'` and `event_id` intact.
  - `tests/test_digest.py::test_digest_force_bypasses_max_retries` — `--force` processes an article with 3 prior failures.
  - `tests/test_aggregate.py::test_load_window_articles_dry_run_does_not_mutate_status` — `mark_filtered=False` performs no DB writes (sentinel reasons survive on both eligible and excluded rows).
- **Verified in this handoff:**
  - `./.venv/bin/python -m pytest -q` → **144 passed**.
  - `./.venv/bin/ruff check .` → clean.
- **Files touched:**
  - `pipeline/state.py` — added `set_article_aggregation_pending_if_unassigned`.
  - `pipeline/digest.py` — three call sites use new helper; `--force` bypasses max_retries.
  - `pipeline/aggregate.py` — window-loop call site uses new helper, dropped redundant `event_id is None` check.
  - `tests/test_state.py`, `tests/test_digest.py`, `tests/test_aggregate.py` — 4 new tests.
- **Not committed.** Working tree still has all the broader digest-refactor changes (`AGENTS.md`, `README.md`, `config/pipeline.json`, `docs/design.md`, `docs/plan.md`, `pipeline/aggregate.py`, `pipeline/cli.py`, `pipeline/digest.py`, `pipeline/state.py`, and the four test files) plus this handoff edit. The user has not yet asked for a commit.
- **Next Steps (unchanged from prior pass, still pending):**
  1. Run a controlled aggregation pass: `./.venv/bin/python -m pipeline.cli aggregate --verbose --limit-windows 1`.
  2. Implement the second validated LLM pass for richer event metadata (titles, slugs, keywords, entities) — Milestone 2 checkbox at `docs/plan.md:114`.
  3. Implement event lifecycle transitions (`active` → `stale` → `archived`) — `docs/plan.md:124`.
  4. Begin Milestone 3 (Editorial Stage).

### State on May 25, 2026 (Late Pass)

- **Completed:** Milestones 0, 1, and the bulk of Milestone 2 (Article Digest + Story Aggregation). Stage 2 is fully implemented and tested.
- **Stage 1 status:** Collection is implemented, verified, and stable. Supports 83 enabled sources including custom AP News and MotorTrend scrapers, robots/SSRF protections, and image sidecar collection.
- **Global `is_filtered` Flag Added:**
  - Database schema bumped to **7**. Added a global `is_filtered` column (default `0`) with a composite index `idx_articles_is_filtered_published`.
  - Added Rule of Engagement 8 to enforce filtering on `is_filtered = 0` in all queries fetching articles for downstream stages (digestion, aggregation, editorial, presentation), preventing filtered non-news/spam content from leaking.
- **Digest Stage Enhancements:**
  - Implemented standalone `digest` stage that performs Gemini Developer API calls in parallel.
  - Handles skipped digests (thin content, missing JSON, max retries reached) by immediately setting `is_filtered = 1` and recording appropriate status prefixes (`filtered_*`), ensuring they do not remain stuck in `pending` aggregation state.
  - Implemented short-circuiting for CNN video transcripts and other obvious video pages: URLs with `/videos/` (case-insensitive) are marked as `filtered_video_or_carousel` and set to `is_filtered = 1`, skipping the LLM entirely.
- **Story Aggregation Stage Enhancements:**
  - The aggregation queries load only articles with `is_filtered = 0`, ensuring that skipped or filtered articles do not waste context tokens or cause redundant processing window reruns.
- **Schema status:** Schema version is **7**. Migrated using `./.venv/bin/python -m pipeline.cli init-db`.
- **Docs updated:** `README.md`, `docs/design.md`, and `docs/plan.md` reflect the `is_filtered` global column, rules of engagement, and deterministic URL short-circuiting.
- **Verified in this handoff:**
  - All **140 tests** passed (`./.venv/bin/python -m pytest`).
  - Linter (`ruff check .`) and compilation check passed.
- **Next Steps:**
  1. Run a controlled aggregation pass: `./.venv/bin/python -m pipeline.cli aggregate --verbose --limit-windows 1`.
  2. Implement the second validated LLM pass for richer event metadata (titles, slugs, keywords, entities).
  3. Implement event lifecycle transitions (`active` -> `stale` -> `archived`).
  4. Begin Milestone 3 (Editorial Stage).

### Previous State on May 24, 2026

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
  3. Design the batched LLM classification and event grouping prompt using headline, brief paragraph summary, source, publish date, and active event context. The current hosted default is the Gemini Developer API with `GEMINI_API_KEY` and `GEMINI_MODEL=gemini-3.1-flash-lite` loaded from local `.env`.
  4. Run `collect --verbose` when refreshing the corpus. The 20 new sources added on May 24 (NBC, Politico, The Hill, CNBC, MarketWatch, Scientific American, MIT Technology Review, Phys.org, NASA, Variety, The Hollywood Reporter, Billboard, DW, France 24, Electrek, Jalopnik, CBS Sports, CNET, 9to5Mac, 9to5Google) have not been fetched yet. Use `clean-data --yes` first only when a fully fresh local collection is desired.
