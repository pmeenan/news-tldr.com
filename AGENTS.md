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
