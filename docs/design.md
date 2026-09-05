# System Design: news-tldr.com

news-tldr.com is a filesystem-backed RSS aggregator that collects source articles, groups related coverage into durable events, writes neutral TL;DR summaries with political framing transparency, and publishes a CDN-cacheable static site.

## Design Goals

- No database server. Pipeline state uses SQLite (a single file), stage artifacts are JSON, and published news content remains static. The optional anonymous reader-sync endpoint is a small isolated PHP-FPM/SQLite exception at the origin.
- Every pipeline stage is inspectable: JSON files on disk, SQLite queryable with standard tools.
- Preserve source attribution from collection through presentation.
- Separate data collection, aggregation, editorial, and rendering so each stage can be tested and rerun independently.
- Prefer neutral factual summaries. Surface partisan framing transparently when present, without adopting it.
- Support hourly pipeline runs with incremental processing. Stories evolve as new articles arrive across runs.

## Technology Stack

- **Pipeline**: Python. Libraries: `feedparser`, pooled `httpx` HTTP/1.1 clients for collection and direct Gemini Developer API calls, `trafilatura` (article extraction), and `beautifulsoup4` (custom scrapers). SQLite is accessed through Python's standard-library `sqlite3` module. `h2` remains as a legacy declared dependency, but production deliberately disables HTTP/2 after shared-connection failures under collection concurrency.
- **Presentation**: Dependency-free Python renderer in `pipeline/present.py`. Generates static HTML, content-fingerprinted CSS/JavaScript, and JSON from published story artifacts.
- **State**: SQLite database for pipeline state, incremental processing tracking, and fast lookups. JSON files remain the human-readable artifacts for each stage.
- **Reader sync**: Optional same-origin PHP 8.4 endpoint in a dedicated resource-bounded PHP-FPM pool. It stores hashed capability groups and three days of story-ID/read-time state in `/var/lib/news-tldr-sync/sync.sqlite`, separate from pipeline state and outside the document root.
- **Deployment**: The pipeline environment (including the pipeline SQLite database, staging files, and lock states) is never web-accessible. Generated files from `dist/` are copied to the Nginx document root at `/var/www/news-tldr.com/`; the sync API source is installed separately under `/opt/news-tldr-sync/`. The checked-in `deploy/nginx/news-tldr.com` virtual host gives generated HTML a 10-minute freshness lifetime so Cloudflare can serve matching pages from its edge cache. Scheduling remains isolated from the public-facing site.

## High-Level Architecture

```mermaid
flowchart TD
    Cron[Hourly cron] --> Runner[pipeline.cli run]
    Runner --> Lock[Single-host pipeline lock]
    Lock --> Maintenance[0. Maintenance and retention]
    Maintenance --> Snapshot[Snapshot maximum article rowid]

    Snapshot -->|parallel branch| Collect[1. Collect feeds and pages]
    Feeds[config/feeds.json] --> Collect
    Collect --> Articles[data/staging/articles/]

    Snapshot -->|bounded existing work| Backlog[Drain editorial and upstream backlog]
    Backlog --> Interim[Publish safe backlog progress when needed]

    Articles --> Digest[2a. Article digest]
    Digest --> Aggregate[2b. Aggregation and deduplication]
    Aggregate --> Events[data/events/]
    Events --> Editorial[3. Story editorial]
    Editorial --> Stories[data/published/stories/]
    Editorial --> Curation[Homepage curation]
    Stories --> Present[4. Static presentation]
    Curation --> Present
    Present --> Site[dist static HTML, CSS, JS, and JSON]
    Site --> Publish[/var/www/news-tldr.com/]
    Publish --> Nginx[Nginx and Cloudflare]
    Nginx --> Browser[Reader browser]

    Browser -->|POST or DELETE /api/sync/v1| SyncAPI[Dedicated PHP-FPM sync API]
    SyncAPI --> SyncState[(Separate sync.sqlite)]

    State[(data/state/pipeline.db)] -.-> Maintenance
    State -.-> Snapshot
    State -.-> Collect
    State -.-> Backlog
    State -.-> Digest
    State -.-> Aggregate
    State -.-> Editorial
```

## Filesystem Layout

```text
config/
  feeds.json                 # Seed feed list and source metadata.
  categories.json            # Category definitions, IDs, and sort order.
  pipeline.json              # Operational tunables: timeouts, thresholds, retention.
  source-policy.json         # Paywall hints, bias labels, reliability notes.

data/
  state/
    pipeline.db              # SQLite: article index, event mappings, run history, lock state.
    pipeline.lock            # Lock file with timestamp for concurrency control.
  staging/
    articles/YYYY/MM/DD/     # One JSON per collected article.
    fetch-log/YYYY-MM-DD.jsonl
  events/
    <event_id>.json          # One file per durable event with article list and metadata.
  published/
    stories/<event_id>.json  # Editorial story JSON, one per event.
    active-stories.json      # Index of active/stale stories plus presentation ranks and curation.

pipeline/present.py          # Static renderer and production deployment logic.
server/sync/                 # Origin sync API, SQLite store, cleanup, local router.
deploy/php-fpm/              # Dedicated sync PHP-FPM pool configuration.
dist/                        # Ignored generated static output; only this tree is publishable.
```

Article staging directories use the article's **publish date** (from the feed or page metadata). When publish date is missing or unparseable, fall back to **fetch date** and set a `publish_date_estimated: true` flag in the article JSON. Collection is text-only and does not download publisher images. Historical same-stem image sidecars may remain in existing staging data; cleanup continues to recognize them when deleting their corresponding historical article artifacts.

Artifact writes are atomic: code writes a temporary sibling and then renames it.
Ownership is progressive rather than strictly immutable. Collection creates an
article JSON file, Digest atomically enriches that same file with `llm_digest`,
and Maintenance may later compact its full text. Aggregation owns event JSON,
Editorial owns story JSON and the active index, and Presentation owns `dist/`.
Every mutation retains enough metadata and prompt provenance to explain the
result.

## Stable IDs

Use deterministic IDs wherever possible.

- `source_id`: short stable slug from `config/feeds.json`, such as `ap`, `reuters`, `ars-technica`.
- `article_id`: SHA-256 hash of the canonical URL. Fallback: hash of `source_id` + feed GUID when canonical URL is unavailable. The chosen input is recorded in the article JSON so collisions can be investigated.
- `event_id`: `YYYY-MM-DD-slug`, where the date comes from the earliest article publication time and the slug is derived deterministically from the selected source headline. Example: `2026-05-24-iran-talks-resume`.
- `story_id`: matches the `event_id` of the event it covers. One story per event.

Event IDs survive later title changes and ordinary reruns because existing event
assignments are reused. Code validates uniqueness against SQLite and appends a
numeric suffix (`-2`, `-3`, and so on) on collision. The grouping model does not
generate IDs or slugs. All IDs (`source_id`, `article_id`, `event_id`) are
strictly sanitized before they are used in file paths.

Events can carry an optional `thread` tag (e.g., `iran-conflict-2026`) for linking related events over time. Threads are free-form metadata strings, not a separate registry. The presentation layer can use thread tags to build "related coverage" links.

## Stage 0: Maintenance & Retention

The completed local pipeline starts with an idempotent maintenance stage before collection. It is exposed as:

```bash
./.venv/bin/python -m pipeline.cli maintenance --verbose
```

Responsibilities:

- Advance event lifecycle state from `active` to `stale` after `aggregation.stale_threshold_hours` without updates, and from `stale` to `archived` after `retention.archived_event_days`. Archived events are excluded from active-event aggregation context.
- Expire old unassigned, unfiltered articles outside the configured staging-retention horizon—three UTC days by default—by setting `aggregation_status = filtered_expired` and `is_filtered = 1`. The article rows remain in SQLite for auditability and dedup/source history.
- Reconcile active/stale event artifacts against SQLite by rebuilding `article_ids`, `article_count`, status, event path, keywords, and newsworthiness from current unfiltered article assignments. Empty active/stale events are deleted from SQLite and `data/events/`.
- Compact old article JSON only when the article is already filtered or belongs to an archived event: remove `content_text`, keep a compact `content_excerpt`, preserve digest/source metadata, and record `content_text_compacted_at`.

The stage records its own `pipeline_runs` entry, supports `--dry-run`, and uses
`--verbose` progress output to stderr. In the top-level command it completes
under the shared lock before collection begins concurrently with any bounded
pre-existing backlog work. See the [pipeline reference](pipeline.md) for the
exact orchestration.

## Stage 1: Data Collection

Inputs:

- `config/feeds.json`, the seed RSS/Atom feed list and scraper targets
- RSS/Atom feeds (HTTP)
- HTML homepages for Custom Scraper targets
- Article pages when feed entries have partial content

Responsibilities:

- Fetch each feed with conditional headers (`If-Modified-Since`, `ETag`) where supported. The pipeline bypasses robots.txt checks for feed URLs configured by the operator but strictly enforces robots.txt for all article page fetches.
- Parse publication time, updated time, headline, summary, feed content, GUID,
  canonical URL, author/byline, tags, and source metadata. Reject feeds that
  contain DTD or entity declarations before passing their bytes to `feedparser`,
  preventing external-entity expansion and related XML attacks.
- Fetch full article text when feed content is incomplete, using HTTP GET with readability-style extraction (`trafilatura` or similar). Feed content is deemed incomplete if it is less than 600 characters, equal to the summary, or not substantially longer than the summary (less than or equal to `len(summary) + 200` characters). Extraction should favor recall for full article coverage, fall back through alternate extractor modes, and keep the existing feed text when page extraction does not improve on it.
- Ignore image metadata and media enclosures in feeds, scraper results, and article pages; do not issue image requests or store image metadata in article JSON.
- Detect likely paywalls and store a `paywall` flag with supporting signals.
- Normalize text enough for downstream processing while preserving raw source fields.
- Store one JSON file per collected source article.
- Record each collected article in the pipeline state database for incremental processing.
- Skip articles already recorded in the state database (deduplicate by `article_id`).

### Stage 1 Implementation

The collection implementation lives in the `pipeline` Python package:

- `pipeline/cli.py`: command-line entrypoint. `./.venv/bin/python -m pipeline.cli init-db` initializes the state database, `./.venv/bin/python -m pipeline.cli run --verbose` runs maintenance through production publish under one full-duration pipeline lock, `run --dry-run` performs a non-mutating/no-network/no-LLM preflight, individual stage commands expose focused controls, and `clean-data --yes` removes local generated pipeline state for a fresh run.
- `pipeline/state.py`: SQLite schema and migration entrypoint. The schema includes feeds, feed conditional request state, articles, article fingerprints, events, pipeline runs, item errors, and LLM usage.
- `pipeline/lock.py`: atomic lock file acquisition/release with PID and Linux process start-time verification, plus stale-lock recovery based on the configured watchdog timeout.
- `pipeline/http_client.py`: async HTTP client with browser-like desktop Chrome request headers, per-domain rate limiting, robots.txt and crawl-delay enforcement, retry/backoff handling, and manual redirect validation.
- `pipeline/security.py`: SSRF guardrails. Every initial URL and redirect target is restricted to `http`/`https`, resolved before fetch, and rejected when it maps to loopback, private, link-local, multicast, reserved, unspecified, or blocked-port destinations.
- `pipeline/collect.py`: feed collection, feed parsing, scraper engine routing, article extraction, paywall signal detection, article JSON writes, database registration, and fetch-log writes.
- `pipeline/scrapers/`: modular engine for custom site scrapers (e.g., AP News, MotorTrend) that generate synthetic feed entries using `beautifulsoup4` when standard RSS feeds are unavailable.

Collection writes article JSON under `data/staging/articles/YYYY/MM/DD/` and appends run logs to `data/staging/fetch-log/YYYY-MM-DD.jsonl`. The SQLite database stores only state/index fields plus JSON metadata needed by later stages; full extracted article text remains in the staging JSON.

Collection also records durable per-source run accounting in `source_run_stats`, one row per `run_id` and `source_id`. These rows track feed status, feed HTTP status, entries seen, articles written, synced existing articles, old/duplicate skips, article failures, and collection error counts. Legacy image outcome columns remain in the schema for historical compatibility and stay zero for new runs. Articles store `collection_run_id`, allowing source-health analysis to join collection behavior to later digest and aggregation outcomes (`is_filtered`, `aggregation_status`, `event_id`) without parsing fetch-log JSONL.

The HTTP client sends a desktop Windows Chrome user-agent plus common browser navigation headers (`Accept`, `Accept-Language`, `Sec-Fetch-*`, cache headers, and `Upgrade-Insecure-Requests`) while retaining RSS/Atom/XML-compatible accept values. It does not use a headless browser, login cookies, or paywall bypass behavior.

### HTTP Client Policy

- **User-Agent**: Use a current desktop Windows Chrome UA string. Update the UA string periodically (at least monthly) to track current Chrome stable releases. Illustrative example (the exact version tracked in `pipeline/http_client.py` may be newer): `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36`.
- **Rate limiting**: Enforce a per-domain delay between requests (default: 1 second). Track last-request-time per domain in memory during a pipeline run.
- **Backoff**: On HTTP 429 or 5xx responses, use exponential backoff (initial 5s, max 60s, 3 retries). Retry status is emitted through the same optional verbose progress callback as collector status, keeping non-verbose stdout/stderr machine-friendly. After max retries, log the failure and move on.
- **Robots.txt**: Respect `robots.txt` directives and `Crawl-delay` when present. Cache robots.txt per domain for the duration of a pipeline run.
- **Timeouts**: Connection timeout 10s, read timeout 10s, and total decoded download timeout 15s. Configurable through `config/pipeline.json`.
- **Concurrency**: Fetch feeds and articles concurrently. Defaults are set high (100 for feeds, 1000 for articles) so all domains run in parallel without a blocking global bottleneck, while the async per-domain lock handles rate-limiting.
- **SSRF Protection**: Only fetch `http` and `https` URLs. Resolve each hostname before requesting it and block loopback, private, link-local, multicast, reserved, and documentation IP ranges for both IPv4 and IPv6, including metadata-service addresses such as `169.254.169.254`. Re-run the same validation for every redirect target and abort redirect chains that change to a blocked scheme, host, port, or resolved IP.
- **Response Size Cap**: Stream response bodies and reject any response whose `Content-Length` header or actual decoded byte count exceeds the configured cap (default 25 MiB). The cap defends against OOM from oversized feeds and gzip-bomb decompression. To prevent decompression errors, the client removes stale decompression headers (`Content-Encoding`, original `Content-Length`, `Transfer-Encoding`) before constructing the returned `httpx.Response`; `httpx` may then set a fresh decoded `Content-Length`.
### Feed Config

Entries in `config/feeds.json` include: `source_id`, `source_name`, `feed_url`, optional `site_url`, default category, content/paywall hints, and fetch behavior overrides. Sources can be staged with `"enabled": false`.

The current source catalog contains 69 enabled sources, including the AP News and MotorTrend custom scrapers. `config/source-policy.json` is kept aligned with `config/feeds.json` by `source_id` so editorial stages can resolve source metadata without guessing.

Custom scraper entries use `"feed_type": "scraper"` and a `fetch.scraper_module` value such as `pipeline.scrapers.ap` or `pipeline.scrapers.motortrend`. Scraper module names are restricted to the `pipeline.scrapers.*` namespace at load time. Scrapers must verify that resolved entry URLs stay on the configured `site_url` host and match an anchored article-path pattern so off-site links and unrelated paths are not enqueued. Each candidate anchor must also look like a headline link — either nested inside an `<article>` / `h1`–`h6` ancestor or itself wrapping a heading element — so subscribe/sign-in/site-chrome anchors that happen to share the article path prefix are filtered out. Scrapers return feed-like entries; any legacy `image_url` field they provide is ignored by the collector.

### Article JSON Sketch

```json
{
  "article_id": "sha256-of-canonical-url",
  "source_id": "example-news",
  "source_name": "Example News",
  "url": "https://example.com/story",
  "canonical_url": "https://example.com/story",
  "guid": "feed-guid",
  "headline": "Headline",
  "summary": "Feed summary or extracted lead",
  "content_text": "Extracted article text",
  "llm_digest": {
    "summary": "Neutral factual digest generated from content_text",
    "key_facts": ["Important fact with source-article context"],
    "content_quality": "ok | thin | extraction_noise | paywalled | non_news",
    "study_stage": "preclinical | animal | early_human | trial_phase | approved | observational | lab_bench | unknown",
    "impact": {
      "global": 0.72,
      "category": 0.88,
      "scope": "local | regional | national | international | niche",
      "novelty": "breaking | update | analysis | evergreen | low_signal",
      "rationale_codes": ["public_safety", "economic_impact"]
    },
    "generated_at": "2026-05-24T12:50:00Z",
    "model": "gemini-3.5-flash-lite",
    "prompt_version": "article-digest-v6",
    "content_chars_used": 12000
  },
  "published_at": "2026-05-24T12:30:00Z",
  "publish_date_estimated": false,
  "fetched_at": "2026-05-24T12:45:00Z",
  "authors": ["Reporter Name"],
  "tags": ["politics", "iran"],
  "paywall": {
    "status": "none | suspected | confirmed",
    "signals": []
  },
  "content_type": "news | opinion | analysis | review | unknown",
  "language": "en",
  "collection": {
    "feed_url": "https://example.com/rss",
    "run_id": "collection-20260601T120000Z-abc12345",
    "http_status": 200,
    "extractor": "feed | readability",
    "article_id_source": "canonical_url | guid"
  }
}
```

## Stage 2a: Article Digest

Inputs:

- Article JSON files from `data/staging/articles/`
- Article records and fingerprints from the pipeline state database

Responsibilities:

- Generate factual per-article LLM digests as a standalone pipeline step before aggregation.
- Write `llm_digest` into each article JSON with summary, standalone key facts suitable for later editorial summarization, content-quality signal, article-level impact, model, prompt version, timestamp, and input character count.
- Run with bounded parallelism (`digest.concurrency` in `config/pipeline.json`) and a verbose CLI mode that streams progress to stderr while final stats stay on stdout.
- Avoid redundant spend by copying completed digests across exact reprints that share content-text or canonical-URL fingerprints. Headline hashes are not used for digest copying because generic shared headlines can describe unrelated stories.
- Validate impact scores and normalize common model scale mistakes: requested output is `0.0` through `1.0`, but otherwise sensible `1-10` and percentage-like values are converted to the configured decimal scale.
- Clamp contradictory low-signal impact scores after validation using a controlled `rationale_codes` vocabulary and `content_quality`. Cap tiers are layered (the minimum applicable cap per axis wins, with global and category capped independently):
  - `non_news` → both axes capped at `0.10`.
  - `LOW_IMPACT_RATIONALE_CODES` (affiliate, archival_index, gambling, low_public_interest, product_recommendation, promotional, puzzle_guide, recycled_content) → both axes capped at `0.15`.
  - `MULTI_TOPIC_RATIONALE_CODES` (`live_blog`, `newsletter_roundup`) → both axes capped at `0.30`.
  - `VENDOR_ANNOUNCEMENT_RATIONALE_CODES` (`vendor_announcement`) → global capped at `0.55`, category capped at `0.75` (asymmetric: vendor keynotes are legitimate vertical news but should not lead a general homepage). Skipped when any HIGH rationale code (`public_health`, `national_security`, `public_safety`, etc.) is also present.
  - `UNCONFIRMED_INJURY_RATIONALE_CODES` (`unconfirmed_injury`) → global capped at `0.60`; category left alone so the vertical can still rank the story.
  - `content_quality in {thin, extraction_noise, paywalled}` (without a HIGH rationale code) → both axes capped at `0.65`.
- Schema-emit an optional `study_stage` enum on medical/biological/materials research articles (`preclinical`, `animal`, `early_human`, `trial_phase`, `approved`, `observational`, `lab_bench`, `unknown`); `not_applicable`, unrecognized values, and stages attached to uncovered domains such as climate, astronomy, aeronautics, software, or general engineering research are dropped before persistence so the field stays present only when meaningful.
- Reset aggregation status to `pending` when a digest is generated or refreshed, so a changed impact score can make a previously filtered article eligible again.
- Use `gemini-3.5-flash-lite` for the first digest. When category impact is within `digest.filter_review_margin` of the aggregation threshold, or a non-`ok` quality label conflicts with a high-impact rationale, run `article-filter-review-v1` on `gemini-3.7-flash`. Persist the first-pass score/quality/model, final reviewer model, rationale, and both usage records. The reviewer may rescue or drop an article; it is not a one-way promotion pass.

The CLI entrypoint is:

```bash
./.venv/bin/python -m pipeline.cli digest --verbose
```

Optional `--range-start`, `--range-end`, `--limit`, and `--concurrency` flags make the stage practical to debug independently from aggregation. `--force` regenerates current-version digests instead of treating them as completed, which is useful after prompt or validation changes. By default, if no range is specified, the stage starts at the configured staging-retention boundary (three UTC days by default), ensuring that late-arriving articles from recovered feeds are digested before maintenance expires them.

## Stage 2b: Story Aggregation

Inputs:

- Digested, unprocessed article records from the pipeline state database
- Article JSON files from `data/staging/articles/`
- Existing event files from `data/events/`
- `config/categories.json` for valid category IDs

Responsibilities:

- Query the state database for articles not yet assigned to an event.
- Use completed article digests instead of site-provided teaser summaries where available.
- Filter out non-news, promotional/affiliate/product/deals, puzzle/game help, gambling picks, archive/index, video-carousel, and no-substantive-content artifacts before grouping. Remaining low-impact articles are filtered using the article digest's category/vertical impact score and `aggregation.min_category_impact` from `config/pipeline.json`; `aggregation.min_category_impact_overrides` raises the floor for specific categories, resolved from the feed's default category (production: entertainment 0.40). Filtered articles are marked with `aggregation_status = filtered_*` so completed windows do not rerun forever on intentionally skipped rows.
- Video/carousel exclusion does not rely only on the digest model. Aggregation also applies deterministic URL/path and collection-signal checks for obvious media pages (for example `/videos/`, `/video/`, gallery/carousel paths, and untitled pages whose extracted text is dominated by video listings) before grouping.
- Deduplicate near-identical exact article reprints (e.g., wire service stories) using content and canonical URL hashes during digest and aggregation stages. Headline and summary hashes remain available in `article_fingerprints` for collection-time near-duplicate suppression, but are not used to short-circuit digest generation.
- Classify each article's content type: news, opinion, analysis, review, or unknown.
- Assign each article to a category from `config/categories.json`.
- Group articles into durable story clusters: either match an existing event or create a new one. A cluster may include multiple angles on the same developing news subject, such as updates, reactions, analysis, and local/international framing. The editorial stage is responsible for separating those angles in the summary. Match using the headline and a brief paragraph summary, not the full article text.
- Maintain lightweight event keywords for matching. The schema preserves optional entity and thread fields, but production does not currently generate them automatically.
- Score each cluster's initial newsworthiness on two axes: global homepage impact and impact within its own category/vertical.
- Update event files and the state database with new article assignments.
- Filter standalone opinion pieces from event creation (opinion articles can be attached to an existing event but should not create one alone).

### LLM Integration for Aggregation

Aggregation uses the Gemini Developer API by default, authenticated with an AI
Studio API key in local `.env` configuration:

```bash
GEMINI_API_KEY=your-ai-studio-api-key
GEMINI_BULK_MODEL=gemini-3.5-flash-lite
GEMINI_REVIEW_MODEL=gemini-3.7-flash
GEMINI_REVIEW_FALLBACK_MODELS=gemini-3.6-flash,gemini-3.5-flash
GEMINI_REVIEW_LITE_FALLBACK_MODEL=gemini-3.5-flash-lite
```

Bulk calls use `gemini-3.5-flash-lite` with minimal thinking. Selective review
and editorial calls use the ordered full-Flash chain 3.7 → 3.6 → 3.5 with low
thinking. A retryable transport, 429, or 5xx failure opens a five-minute
per-process circuit for that tier so concurrent and subsequent calls bypass it.
Deduplication alone may use 3.5 Flash-Lite as a final capacity fallback; Lite
can reject a candidate but cannot authorize a destructive event merge.
If every full-Flash tier returns an empty response for safety-sensitive
editorial input, editorial retries the same full-Flash chain once with compact
digest/key-fact context and records a distinct compact prompt version; it still
never falls back to Lite.
`GEMINI_MODEL` remains a bulk fallback. Calls go through `generativelanguage.googleapis.com` with the
`x-goog-api-key` header and a pooled `httpx` HTTP/1.1 client. Requests omit the
deprecated Gemini 3 sampling fields (`temperature`, `topP`, and `topK`) and use
structured output (`responseMimeType: application/json` plus a JSON schema).
API keys must not be written to logs, command output, JSON artifacts, or
committed files.

Aggregation runs over fixed UTC publish-time chunks with a short overlap lookahead. The default aggregation chunk is 3 hours with an additional 1-hour overlap, so actual LLM windows are 4 hours wide and anchored to UTC boundaries (`00:00-04:00`, `03:00-07:00`, `06:00-10:00`, `09:00-13:00`, `12:00-16:00`, `15:00-19:00`, `18:00-22:00`, `21:00-01:00`). Hourly cron runs keep returning to those same fixed windows rather than shifting the window start based on the current hour or first unassigned article. The default planning horizon matches `retention.staging_article_days` (three days), so late-arriving articles accepted by collection are not outside digest/aggregation coverage. The digest stage runs before aggregation; aggregation then filters non-news/spammy/video-carousel artifacts and articles whose digest category/vertical impact score is below the configured threshold. The state database records completed aggregation windows; normal pipeline runs use sparse planning, selecting only fixed window starts that have unassigned articles in that publish-time bucket, plus the latest completed window when it falls in range. This avoids rerunning every intervening old window when late-arriving articles appear with older publication timestamps. Forced aggregation remains continuous so reset coverage is explicit.

For each aggregation window, the pipeline loads all eligible articles published
within those hours—both assigned and unassigned—and sends headline, digest
summary/key facts, source, and publication metadata to the LLM. The model
classifies content type and category, groups article indexes into clusters, and
may attach a cluster to one of the existing event IDs explicitly offered in the
prompt. It does not generate new IDs, slugs, entities, or thread tags.

Prompt shape is part of the contract. The grouping pass avoids free-text output
and returns compact, order-preserving structured data using article indexes.
Deterministic code maps rows back to inputs, verifies that every index appears
exactly once, derives new event titles, IDs, slugs, and keywords from source
headlines, and rejects unoffered existing-event IDs.

#### Category Group Partitioning

To solve LLM laziness and incorrect grouping when a single window contains a large volume of articles (which can exceed 100+ articles in high-traffic windows), the window articles and active events are partitioned into related **Category Groups** before invoking the grouping LLM. Oversized groups are split into bounded batches of at most 50 articles before prompting, preventing attention breakdown and hallucinations.

The predefined category groups are:
- `politics_gov`: `politics`
- `news_business`: `us`, `world`, `business`
- `sci_tech`: `technology`, `science`, `health`, `environment`
- `leisure`: `entertainment`, `automotive`

For each window chunk:
1. Load all eligible window articles.
2. Group the articles by their default categories into the four Category Groups. The high-volume `news_business` group is split first by `us`, `world`, and `business`, then any oversized bucket is chunked to the configured batch maximum.
3. For each non-empty Category Group batch, fetch active events matching the batch categories. The matching set intentionally bridges `politics` with `us` and `world` so civic/political stories from general-news feeds can still attach to political events without combining all those articles into one large grouping batch.
4. Run active event filtering, Gemini story grouping, and newsworthiness scoring separately for each batch. These LLM-only batch jobs run concurrently up to `aggregation.category_batch_concurrency` in `config/pipeline.json` (default: 8). SQLite/event-file writes still happen afterward on the main thread in deterministic batch order.
5. Accumulate token usage, execution times, and counts across all processed Category Group batches to finalize the window's run metrics.

After the grouping response validates, deterministic code also splits weakly connected headline groups into smaller components. Components only inherit an `existing_event_id` when they have enough headline overlap with that active event title (or already contain an article assigned to that event), which prevents broad-keyword clusters from attaching unrelated articles to an existing event. If the model returns an `existing_event_id` that was not offered in the prompt, validation drops that ID instead of failing the whole category batch.

Post-aggregation deduplication runs on every aggregation invocation, including
passes with no new window work, and collects candidate event pairs from four
complementary gates before running the same strict per-pair LLM merge call on
the union. The default 72-hour lookback includes active and stale events for the
complete homepage window:

1. **Slug / title heuristics** — base-slug equality, title-word overlap (`_titles_similar`), and highly similar individual article headlines (catches reprints with different lead facts).
2. **Keyword-overlap gate** (`_keyword_overlap_candidates`) — within a single category-group batch, pairs events that share at least 2 distinctive event-level keywords after stripping a global static stopword list and a per-batch *dynamic* hot-keyword stopword list (`_dynamic_keyword_stopwords`). The dynamic list flags any keyword appearing in ≥20% of the batch's events AND in ≥4 events (an absolute floor that prevents tiny batches from stopwording distinctive entities like "ferrari" that only appear in the 2 candidate duplicates).
3. **LLM pre-screen gate** (`_llm_prescreen_candidates`) — loose-recall LLM calls over category-group batches (`politics_gov`, `news_business_{us,world,business}`, `sci_tech`, `leisure`), sending each event's id, title, top-6 filtered keywords, and top-3 article headlines. The model is instructed to err on the side of inclusion; false positives are filtered by the strict per-pair merge call. Each call is bounded to `DEDUPLICATION_MAX_EVENTS_PER_PRESCREEN_BATCH = 40` events and chunked when batches exceed that. For oversized batches, the top high-article-count anchor events are repeated into every prescreen chunk so large ongoing stories can still be compared with later singleton updates that would otherwise land in a different chunk. `news_business` also gets an additional parent-level cross-category prescreen because market, business, U.S., and world framings often describe the same underlying event from different verticals. All prescreen chunks across all category-group batches share one worker pool up to `aggregation.deduplication_concurrency` (4 in the checked-in production configuration; the code fallback is 16). Prompt version: `deduplication-prescreen-v1`. Token usage and errors are recorded under stage `deduplication_prescreen`.
4. **Per-pair merge LLM** — for every unique candidate pair from the union of (1)–(3), `_build_event_merge_prompt` decides `should_merge` with confidence ≥ `DEDUPLICATION_MERGE_CONFIDENCE_THRESHOLD` (0.8). Prompt version `deduplication-review-v2` treats immediate reactions, consequences, implementation details, and alternate reporting angles as one evolving homepage story when they share the same core development, while explicitly keeping separate incidents and materially distinct developments apart. This is the single source of truth for actually merging; over-merging risk lives here only. Candidate-pair reviews run concurrently in rounds of disjoint event IDs, then accepted merges are applied serially in deterministic order so one merge cannot race another merge touching the same event. Full-Flash decisions are cached against both event `updated_at` values and the prompt version, so unchanged negative pairs are not paid for or retried every hour. The bounded 40-pair queue orders exact/base-slug and strong title/headline candidates before keyword-overlap candidates, which in turn precede broad LLM-prescreen candidates; recency breaks ties within each confidence tier. This prevents obvious same-headline splits from being crowded out by a burst of newer loose-recall candidates. A changed event invalidates its cached pair decisions automatically.

Over-merging protection comes from the strict per-pair call; recall comes from the diversity of candidate gates. Adding a new gate cannot weaken the merge decision — at worst it adds extra per-pair LLM calls that return `should_merge=false`.

The chunk-plus-overlap approach balances context window limits, cost, latency, and retry blast radius. The hosted Gemini path supports much larger contexts than the local Ollama evaluation, but smaller chunks keep batches under the LLM attention-breakdown threshold and isolate failure blast radius to a single chunk plus overlap.
Matching uses only the headline and a digest/brief paragraph summary, plus compact key facts when available, to keep grouping payloads small and avoid processing full article text inside the grouping call. Full article text stays on disk for digest generation and later editorial passes.

Deterministic code validates all LLM outputs: checks category IDs against `config/categories.json`, enforces event ID uniqueness, and rejects malformed suggestions.

After grouping, aggregation derives event newsworthiness from article-level digest impact scores when available. This avoids an extra LLM call for groups whose articles already have impact metadata. For groups missing digest impact, aggregation can still run a smaller newsworthiness scoring pass over each cluster/singleton. This keeps the grouping prompt focused on "same underlying event" while still capturing editorial ranking signals early. The scoring prompt uses headline, brief summary, source count, article count, and category hint, and returns:

- `global`: importance to a broad general-news homepage.
- `category`: importance within the story's own vertical.
- `rationale_codes`: compact audit tags such as `geopolitical_escalation`, `public_safety`, `economic_impact`, `entertainment_major_release`, or `low_public_impact`.

Scores are normalized from `0.0` to `1.0`, validated by deterministic code, stored in event JSON, and mirrored in SQLite for ranking queries. If digest impact is unavailable and the LLM scoring call fails, is unavailable, or omits a cluster, deterministic baseline scoring is used so every event has a usable signal.

### Stage 2 Implementation

The production aggregation implementation lives in `pipeline/aggregate.py` and is exposed by `./.venv/bin/python -m pipeline.cli aggregate`. It plans 3-hour aggregation chunks with a 1-hour overlap lookahead (4-hour LLM windows fixed to `00/03/06/09/12/15/18/21` UTC starts), skips completed windows using `aggregation_windows`, loads category-impact-eligible articles in each planned window, calls Gemini with headline + digest/summary metadata in category-bounded batches, validates that every article index appears exactly once, scores resulting groups for newsworthiness from digest impact or fallback scoring, writes `data/events/<event_id>.json`, upserts the `events` table, assigns `articles.event_id`, and records completed windows. Windows remain sequential because each window should see event state from earlier windows; within a window the LLM-only category batch work is concurrent. The command supports `--range-start`, `--range-end`, `--limit-windows`, `--dry-run`, `--force`, and `--verbose`. By default, if no range is specified, aggregation bounds cover the configured staging-retention horizon, while non-force planning sparsely selects only fixed UTC windows that have unassigned articles in their publish-time bucket, plus the latest completed window when applicable. If no unassigned articles exist within this horizon, the run plans no windows but still drains bounded deduplication review work. With `--force`, default planning instead uses completed digests in the same retention horizon, clears prior event assignments and aggregation-stage filter decisions for the actual planned window coverage, deletes or trims affected event artifacts, and then reruns the continuous window range even if windows were previously marked completed.

Event naming is deterministic and intentionally simple: existing event IDs are
reused when a group contains already-assigned articles; otherwise code derives a
stable date plus headline slug and stores lightweight keyword metadata. A richer
metadata-generation pass remains an optional backlog item.

Digest regeneration and review on May 25, 2026 (article-digest-v3) verified the layered cap system and controlled-vocabulary rationale codes against a stratified sample (39 across all 11 categories + 6 edge cases). Compared with v2: `paywalled` started being used (0 → 27 articles), `extraction_noise` doubled (23 → 52), `novelty=low_signal` nearly tripled (40 → 116), `novelty=breaking` dropped from 294 → 246 (research papers no longer auto-tagged as breaking), `novelty=analysis` more than doubled (60 → 156), `impact_capped` events roughly quadrupled (~40 → 176) as the new vendor_announcement / multi-topic / unconfirmed_injury / recycled_content rationales started firing, and `study_stage` was populated on 49 research articles with the full enum spread. Asymmetric caps (vendor_announcement, unconfirmed_injury) and HIGH-rationale cap bypass (public_health Ebola wires) were both confirmed working on sampled articles. Known residual minor issues: the date-leak rule against inserting `published_at` year/month/day into the summary is followed most of the time but still drifts occasionally, and one paywalled WaPo article was tagged `thin` despite the prompt explicitly naming "Democracy Dies in Darkness" as a paywall signal (caps still fired correctly via the noise-cap path so downstream behavior is unaffected).

Prompt version `article-digest-v6` includes URL, canonical URL, and estimated-publish-date metadata in the digest prompt; tightens guidance for video, gallery, profile/background, media-transcript, stale estimated-date, and stale archive/background pages; narrows `study_stage` to covered medical/biological/pharmaceutical/materials research; and gives category-impact guidance for legitimate vertical stories whose global impact is low. The digest stage also deterministically filters obvious `/video/`, `/videos/`, `/gallery/`, and `/galleries/` URL paths before LLM calls, filters stale estimated-date pages when URL or live-page text dates are clearly older than the collected timestamp, and drops irrelevant `study_stage` values before writing `llm_digest`. The code-side `study_stage` gate uses word-boundary matching and excludes climate, space, aeronautical, software, paleontology, and general earth-science contexts unless there is a strong biomedical or materials signal.

#### Event Merging and Reassignment

To handle stories that develop over longer periods and span across different 3-hour windows, the system implements a two-layered event merging strategy:

1. **Proactive Active-Events Matching**: During the window aggregation pass, the aggregator queries the SQLite database for events updated within the last 48 hours matching the categories of the window articles. These active events are filtered to only include those whose title shares at least 2 non-stopword words with at least one article's headline in the current window (or shares the single non-stopword if the event title only has one). This prevents context bloat and false-positive groupings in the LLM. The matched active events are passed to the grouping LLM call (containing their IDs, categories, and titles/headlines). The LLM is instructed to assign window articles directly to these existing events where appropriate, returning their `existing_event_id` in the JSON groups response. The validator only preserves event IDs that were actually included in that prompt.
2. **Reactive Post-Aggregation Deduplication**: At the end of every aggregation invocation, a reactive deduplication process runs over active and stale events updated in the configured 72-hour lookback, even when no aggregation window changed. It checks for candidate event pairs using suffix conflicts (e.g. `event-name` vs `event-name-2` date-slug collisions), title word overlaps, highly similar article headlines, distinctive keyword overlap, and an inclusive 3.5 Flash-Lite prescreen. Each unique candidate pair then goes to the strict `deduplication-review-v2` call on `gemini-3.7-flash`, which evaluates full article lists, headlines, and digests. A merge requires both `should_merge=true` and confidence of at least `0.80`.

When a merge is triggered (either proactively via the window LLM or reactively via post-aggregation deduplication), the aggregator selects a winning event ID, loads historical article IDs from both events, merges their article lists, updates the winning event's JSON and database assignments, deletes the merged-away events' JSON files, and removes their SQLite database entries to prevent historical data loss.

Model evaluation notes:

- Local model evaluation on May 24, 2026 found that `gemma4:26b` with `think: false` and compact numeric schema produced valid structured output in about 29 seconds for an 8-article CPU batch, but larger title-clustering experiments were too slow for the pipeline's needs.
- `qwen3.6:27b` with `think: false` produced good structured output but was much slower, about 227 seconds for the same 8-article CPU batch.
- `llama3.1:8b` produced valid structured output but lower classification quality.
- Free-text JSON fields and calls without local-model thinking controls caused empty responses, looping, malformed JSON, or poor reliability in local tests.
- August 24, 2026 live structured-output smoke tests succeeded for `gemini-3.5-flash-lite` and `gemini-3.7-flash`. A curated comparison found that 3.7 added useful judgment on some borderline article-impact decisions but also shifted scores in both directions; it is therefore used only for near-threshold/conflicting article-filter decisions and strict final event-pair adjudication. Bulk digestion, grouping, scoring, and dedupe candidate discovery remain on 3.5 Flash-Lite.

### Event JSON Sketch

```json
{
  "event_id": "2026-05-24-iran-talks-resume",
  "title": "Talks resume after overnight strikes",
  "category": "world",
  "thread": "iran-conflict-2026",
  "keywords": ["iran", "talks", "strikes"],
  "entities": [
    {
      "name": "Iran",
      "type": "place"
    }
  ],
  "created_at": "2026-05-24T12:45:00Z",
  "updated_at": "2026-05-24T18:30:00Z",
  "status": "active",
  "article_ids": [
    "sha256-abc123",
    "sha256-def456"
  ],
  "article_count": 2,
  "confidence": 0.86,
  "newsworthiness": {
    "global": 0.82,
    "category": 0.91,
    "rationale_codes": ["geopolitical_escalation", "multi_source"],
    "scored_at": "2026-05-24T18:31:00Z",
    "model": "gemini-3.5-flash-lite",
    "prompt_version": "newsworthiness-v1"
  }
}
```

Event `status` values:

- `active`: received new articles within the staleness threshold (default: 48 hours, configurable via `stale_threshold_hours` in `config/pipeline.json`).
- `stale`: no new articles beyond the staleness threshold. Still visible on the site but deprioritized.
- `archived`: older than the configured retention window. Excluded from active story generation.

## Stage 3: Editorial

Editorial selects changed active/stale events using `last_editorial_at` and
`events.updated_at`. All article retrieval requires `is_filtered = 0`.

### Evidence, drafting, and verification

`pipeline/editorial.py` and `pipeline/evidence.py` implement three full-Flash
operations per ordinary story, using the existing ordered 3.7 → 3.6 → 3.5 chain:

1. **Evidence (`editorial-evidence-v1`)**: choose essential claims, attribution,
   contradictions, numbers, dates and qualifications. Every claim carries one
   or more short verbatim passages and an offered article ID. Code checks each
   passage against the supplied article text after whitespace normalization.
   Invalid extraction receives one bounded retry with validation feedback.
2. **Draft (`editorial-v4`, or `editorial-framing-v3`)**: generate a sentence-case
   headline, dek, 2–4 TL;DR bullets, exactly two short briefing bullets (15–22
   words, at most 230 characters), cited key facts and uncertainties. Headline,
   dek and both summary forms link to ledger claim IDs. Citations containing any
   unknown source ID are rejected. The draft schema restricts article citations
   to reports present in the ledger. Deterministic checks reject a headline that
   copies a source headline or reads as title case (three quarters or more of
   its eligible words capitalized), and, when every report shares one publisher,
   require the dek and first briefing bullet to name that outlet or say
   "according to". Overlong fields fail validation rather than being silently
   truncated; every rejection feeds the single repair attempt.
3. **Verification (`editorial-verification-v2`)**: a separate model call checks
   the actual quoted evidence against every draft assertion and qualification.
   It also compares with the previous story to distinguish substantive changes
   from rewording or added citations. A rejected draft gets one repair and
   verification attempt. Failure retains the previous artifact and checkpoint.
   Validation rejections remain pending and unhealthy, but do not block unrelated
   downstream news. Transport/capacity failures still gate backlog processing.
   A meaningful revision's `change_summary` must be one reader-facing sentence
   of news; a summary that describes the edit ("Added details…") or repeats a
   bullet is rejected and the verifier is re-asked once with that feedback.

These are automated checks, not a guarantee of truth or independence. The
quoted passage can itself report an allegation or contain a publisher error;
that distinction must survive summarization. A separate human evaluation rubric
is maintained in [editorial-evaluation.md](editorial-evaluation.md).

Input selection gives useful excerpts to relevant, diverse reports instead of
uniformly dividing the budget over arbitrarily large clusters. Candidates rank
by overlap with the event anchor; one report per publisher is preferred before
additional same-publisher reports. Up to eight reports fit within the configured
per-article/event character limits (production 12,000/40,000 characters, aiming
for at least 3,000 per selected report). Digests remain contextual aids, not
substitutes for quoted evidence verification. The existing compact draft retry
uses the same verified ledger, with `editorial-v4-compact` or
`editorial-framing-v3-compact` provenance.

Stories published before evidence verification migrate through a bounded
backfill after each run's normal editorial work. `editorial_backfill_rows`
selects current-window active/stale events whose story file lacks
`evidence_verification`, highest global newsworthiness and article count first,
skipping events with an editorial error inside the cooldown window. Work stops
being submitted once the time budget is spent; unstarted stories defer to a
later run. Backfill failures are recorded like ordinary editorial failures but
never enter the combined run's backlog gate, because those events are not
pending. Forced or event-specific runs do not backfill.

Story files retain a private `_evidence` ledger for audit. Public JSON excludes
underscore-prefixed fields and exposes only claim-to-source mappings, summaries,
source links, verification version/status and revision metadata. Only reports
represented in the ledger enter the new story's public source list. All completed
model operations, including rejected drafts, retain per-operation model/prompt
usage records. Files are written atomically before the editorial checkpoint moves.

### Event boundaries and publisher identity

Aggregation v7 requires one specific shared real-world development. The bulk
model is no longer told that editorial will separate broad topics later.
`event-membership-v1` reviews proposed new attachments to existing events before
assignment. A rejected attachment becomes a separate event for later deduplication.
Existing events cannot merge implicitly because their assigned articles appeared
in one sliding-window group; destructive merges require the full-Flash reviewer.

`pipeline/coherence.py` reviews a bounded set of existing active/stale clusters
within the 72-hour window. The budget is `coherence_reviews_per_run` (default 10).
The cache signature includes the coherence prompt version and ordered article
membership. A valid partition must cover every unfiltered article exactly once,
with confidence at least 0.90; Lite cannot authorize it. SQLite replaces all
memberships in one transaction. The original event ID follows its original
anchor, and split events receive deterministic IDs and recomputed scores. Split
freshness follows the latest constituent publication time, so an old angle does
not become today's news merely because its membership was repaired. Event JSON
is then written. The prior original story is retained privately with
`_pending_coherence` set and excluded from the active index until regenerated.
Its creation/revision metadata remains available for read-history continuity.
SQLite remains authoritative if interruption requires maintenance reconciliation.
Failures retain recoverable state and are logged with incremental progress.

Deduplication v3 includes current published headlines in candidate discovery and
rejects a merge when the complete constituent reporting contains unrelated events.
Broader topics remain presentation sections, not oversized event clusters.

`config/source-policy.json` maps feed IDs to canonical `publisher_id` values.
Coverage and importance use publisher identity rather than feed display names.
Explicit AP/Reuters wire attribution is retained as `reporting_origin` where
recognized; missing provenance stays unknown. `source_count` means publisher
count. Neither that count nor separate article URLs establish independent
corroboration. The index also exposes known origins and provenance completeness.

### Revisions and read identity

Stories have `revision`, `revision_at`, and `change_summary`. A first story starts
at revision 1. The verifier increments the revision only for substantive new
facts, resolved uncertainty or corrections. A cosmetic regeneration preserves
revision/time/change summary. Existing story `created_at` is always preserved.

Revision 1 uses the legacy story ID and original creation order. Later revisions
use `r` plus SHA-256 of `story_id:revision` as their read ID, paired with the first
publication time of that revision. The article's public route remains stable.
This separate, immutable identity lets existing sync watermarks and sparse read
maps coexist with new developments. An earlier story read cannot cover a revision
published after that read watermark. The card's “Updated since you read” note
is rendered hidden and revealed only for readers whose local history covers the
earlier revision; first-time readers see the bullets alone. Story pages always
show the latest change summary.

### Political framing and ranking

Mixed left/right source-policy coverage still gates optional political framing.
The prompt requires actual, attributable differences in article content and
rejects invented symmetry; the evidence verifier checks the resulting text.
Source labels do not prove truth or establish equal support for competing claims.

Importance retains its existing weighting of global/category impact, editorial
judgment, freshness and source quality, but publisher counts replace feed counts.
The active index contains active/stale stories, presentation ranks and homepage
curation. Global and category views keep their respective rank order; news
freshness remains based on event updates rather than forced editorial timestamps.

### Story contract additions

The existing headline/dek/TL;DR/facts/uncertainties/source contract remains readable.
New artifacts additionally contain:

- `briefing`: exactly two deliberately composed, complementary bullets.
- `claim_links`: ledger claim IDs for headline, dek, TL;DR and briefing.
- `claim_sources`: claim IDs mapped to source article IDs, without private quotes.
- `_evidence`: private quoted support; excluded from public API output.
- `evidence_verification`: verifier version and approved status.
- `revision`, `revision_at`, `change_summary`: meaningful-update history.
- Source `source_id`, `publisher_id`, and optional `reporting_origin`.

Older artifacts remain supported and are not represented as having passed the
new verification process. A build alone does not perform an editorial migration.

## Stage 4: Presentation

`pipeline/present.py` remains a dependency-free static renderer. It escapes all
untrusted text, restricts source URLs to HTTP/HTTPS, validates story paths and
builds into a temporary sibling before replacing ignored `dist/`.

The main briefing contains at most 12 stories. The cohort is selected for the
current category/coverage preference **before** read filtering, preferring
curated Top News then rank. Reading a card cannot cause a replacement to enter
that briefing. A clear stopping message separates the briefing from the topic groups and
category remainders, which render inline below it on every viewport; nothing
on the homepage is collapsed.

Cards use smaller headlines and two complementary bullets, without a redundant
dek. New artifacts supply `briefing`; older artifacts fall back to the first
TL;DR item plus their first uncertainty, or two TL;DR items when no uncertainty
exists. Each card names its distinct publishers in source order, showing two
names plus a “+N” remainder (deduplicated by publisher identity and display
name), and links directly to `#sources` on the story page. Story pages
retain full TL;DR, facts, uncertainties, optional framing, and source reports;
new TL;DR bullets have claim-backed source links. `/methodology/` explains the
process, source-count limitations, read behavior and public correction reporting.

The existing warm palette, category-family tints, serif typography, same-origin
assets, noindex/social metadata and favicon remain. All colors are CSS tokens; a
dark token set applies under `prefers-color-scheme: dark` unless the root carries
`data-theme="light"`, and `data-theme="dark"` forces it. A fingerprinted
`assets/theme.<hash>.js` runs synchronously in `<head>` on every page, applies
the saved `newsTldrThemeV1` preference before the stylesheet loads, and drives
the masthead toggle. Larger controls and complete
labels improve mobile operation. The sticky toolbar contains category, New/All,
All/2+ outlets, and Mark read controls. New browsers default to all outlets so
consequential original reporting is not automatically hidden. Saved preferences
remain respected. `coverage=top` explicitly requires two publishers; the legacy
`coverage=all` URL remains accepted. The masthead counts unread briefing items.
Both site and individual story relative timestamps refresh each minute.

A title at least 60% visible for **one second** still counts as read, intentionally
supporting headline skimming. Cards remain stable during the scan; filters apply
read history on the next render. Mark read affects currently displayed cards,
excluding collapsed coverage. Optional sync keeps its existing three-day state,
private fragment link, one bounded initial pull and silent background writes.

Publishing, CSP, noindex/social metadata and cache contracts are unchanged:
HTML has a 10-minute freshness lifetime; fingerprinted CSS/JavaScript have one
year immutable caching. Deployment preserves unknown files and old asset paths,
copies assets/pages before replacing `index.html`, and removes only stale managed
files. Presentation v23 added the methodology route to the managed build/sitemap;
v24 adds the retained `theme.<hash>.js` asset, which artifact validation expects
exactly once alongside the site CSS and JavaScript.

## Anonymous Reader-Sync Origin

The sync service is deliberately separate from both the static document root and
`data/state/pipeline.db`:

- `server/sync/api.php` exposes create, merge, and delete mutations under
  `/api/sync/v1/`; all other sync paths return 404 at Nginx.
- `server/sync/lib.php` owns schema creation, validation, atomic `BEGIN
  IMMEDIATE` merges, retention, and capacity enforcement.
- `/opt/news-tldr-sync/` contains root-owned installed PHP source;
  `/var/lib/news-tldr-sync/sync.sqlite` is writable only through the dedicated
  `www-data` PHP-FPM pool and is outside the public tree.
- `server/sync/cleanup.php` prunes expired reads/groups and old creation counters;
  `/etc/cron.d/news-tldr-sync` runs it daily as `www-data`.

### Capability and Merge Model

Group creation generates 32 random bytes and returns their unpadded base64url
encoding. The browser treats this token as a bearer capability. SQLite stores
only `SHA-256(token)`, the versioned JSON read state, a revision, and lifecycle
timestamps. The token never appears in an API path or query string. The share URL
places it in the fragment, which client JavaScript consumes and removes before
network synchronization. API calls carry it in `Authorization: Bearer` and
responses set `Cache-Control: private, no-store`.

`POST /api/sync/v1/merge` validates the offered versioned state, loads the group
inside an immediate SQLite transaction, takes the maximum read-prefix watermark,
unions remaining IDs using the maximum read timestamp, and prunes ordered IDs
covered by the watermark plus state beyond the three-day window. The group
revision advances only when read state changes. A browser includes its last
applied revision; when it still matches the pre-merge server revision, the server
returns only revision/status metadata rather than the complete state. This is
idempotent and avoids a fetch-then-write race. Legacy flat ID/timestamp maps remain
readable and are normalized into the versioned envelope when changed. The client
applies returned state only during initial/link import; ordinary background writes
use the same atomic endpoint but leave the current display unchanged. There is intentionally no manual unread operation;
adding one would require tombstones or another conflict model.

### Containment and Privacy

The default application ceilings are 2,000 active groups, 100 new groups per UTC
day, 2,000 read IDs per group, 256 KiB request/state payloads, 180-day group
inactivity expiry, and a persistent 256 MiB SQLite `max_page_count`. WAL
autocheckpointing and a 16 MiB journal size limit bound transient database growth.
Nginx adds separate Cloudflare-client and direct-peer request limits, four
connections per peer, a 256 KiB body cap, and short FastCGI timeouts. The dedicated
ondemand PHP-FPM pool allows at most three 32 MiB workers, disables process/shell
functions, and confines PHP filesystem access to the installed code, sync state,
and `/tmp`.

Requests require JSON and an exact allowed `Origin`; cross-origin access is never
enabled. Retained read state consists of story IDs, millisecond read timestamps,
immutable first-publication order values for compactable IDs, a read-prefix
watermark, and a monotonic revision; the service stores no account, article text,
or browsing history outside that state. Daily cleanup and every mutation remove
expired groups; active merges remove expired reads. The sync database should not
enter general backups, because backup retention would defeat the reader-facing
expiry promise. Local reading remains fully functional during API failure.

`scripts/install-sync-origin.sh` installs the PHP source, pool, cron file, and
Nginx virtual host, initializes the database as `www-data`, validates both service
configurations, binds the dedicated socket to the worker identity declared by
the host's Nginx configuration, and reloads PHP-FPM/Nginx.

## Pipeline Operations

### CLI Output Contract

Pipeline commands that can run long enough to feel idle in an interactive shell
support `--verbose`. Progress and status go to stderr while the final
machine-readable JSON remains on stdout. This applies to the combined run and
the maintenance, collection, digest, aggregation, editorial, presentation,
validation, and health commands.

The `clean-data` command removes local generated pipeline state for a fresh run: the SQLite database and sidecars, staged article files, event JSON, published JSON, and fetch logs by default. It requires `--yes`, refuses to run while `data/state/pipeline.lock` exists, and can keep fetch logs with `--keep-fetch-log` or override the lock guard with `--ignore-lock`. It does not remove `dist/` or the production document root.

### Validation, Health, and Usage Reporting

`validate-data --verbose` performs deterministic validation across the complete
artifact boundary: feed/source-policy symmetry, category IDs, SQLite
`quick_check`, LLM usage prompt provenance, article/digest schema, event schema,
story citations and source URLs, active-index parity/ranking, and required static
pages/API assets. Invalid results are emitted as machine-readable JSON and exit
nonzero.

`health --verbose` combines artifact validation with operational checks. It
requires recent successful runs for maintenance, collection, digest,
aggregation, editorial, and presentation; detects stale running jobs; evaluates
failed feeds/articles from the latest collection; and requests the public
homepage and active-story API over HTTPS. Its latest report is atomically stored
at `data/state/health.json`, and any failed check produces a nonzero exit.

Every LLM call records run ID, stage, model, prompt version, input/output tokens,
optional cost, and timestamp in `llm_usage`. `llm-usage --hours N` groups those
records for operational/cost review. The artifact validator also rejects usage
rows or generated artifacts with missing prompt provenance.

`run --dry-run` acquires and releases the real pipeline lock, previews
maintenance, counts enabled feeds and pending digest/aggregation/editorial work,
checks presentation inputs, and runs validation. It performs zero network or LLM
calls and makes no database, artifact, static-build, or production changes.

### Scheduled Production Runs

The production user crontab runs `scripts/run-scheduled.sh` at minute 17 every
hour. The wrapper invokes the complete pipeline and its automatic production
publish, then runs the health check. It retains detailed output in
`data/state/scheduled-pipeline.log`, rotates at 10 MiB, and exits nonzero when
either the pipeline or health check fails so cron's normal error-mail path can
alert the operator. `deploy/cron/news-tldr.cron` is the checked-in schedule
source. The pipeline lock prevents overlap if an earlier run is still active.

At the start of every combined run, interrupted `pipeline_runs` rows are marked
failed for auditability. After maintenance, the runner snapshots the maximum
article SQLite `rowid` and starts collection in a dedicated thread/event loop.
Pending editorial work is processed and published while feed/article HTTP
requests run. Retained digest and aggregation work is restricted to the
snapshotted row boundary, then flows through editorial and another publish;
articles inserted by the concurrent collection cannot move that backlog's
finish line. If the snapshot backlog remains, collection still completes and
checkpoints, but new downstream work is deferred and the command exits nonzero.
SQLite connections use a 30-second connection/busy timeout so the collection
writer and short editorial/usage writes serialize safely under WAL mode.

Sparse aggregation deliberately revisits the newest completed window to catch
late arrivals, but replaying a group whose articles are already present in the
same event is a no-op: its event JSON and `updated_at` stay unchanged. This keeps
hourly runs incremental and prevents unnecessary editorial LLM regeneration.

### Concurrency Control

Only one pipeline run may execute at a time. Concurrency is controlled by a lock file at `data/state/pipeline.lock`. Individual stage commands acquire this lock for that stage. The combined `run` command acquires the same lock once. After maintenance it overlaps collection with any snapshotted editorial/digest/aggregation backlog, waits for collection before admitting its new article rows downstream, and then follows digest → aggregate → editorial → presentation/publish. Inner stage implementations do not reacquire the lock, so another scheduled run or manual stage command cannot slip between stages; the collection thread remains owned by the same locked top-level run.

**Lock acquisition:**
1. Create `pipeline.lock` atomically using exclusive creation (`O_CREAT | O_EXCL`) or an atomic lock directory. Do not use a separate check-then-create sequence.
2. The lock file contains `run_id`, `pid`, `hostname`, `boot_id` when available, process start time when available, `cwd`, and `started_at`.
3. If atomic creation succeeds, proceed.
4. If the lock already exists, read it and verify the recorded process identity:
   - If the recorded hostname differs from the current host, treat PID checks as unverifiable and exit unless the deployment provides an external single-run guarantee.
   - If the hostname matches but the recorded `boot_id` differs from the current boot, treat the lock as stale. Log a warning, remove it, and retry atomic acquisition.
   - If the recorded process no longer exists, or the PID exists but its process start time does not match the lock, treat the lock as stale. Log a warning, remove it, and retry atomic acquisition.
   - If the recorded process identity matches and `started_at` is within the **watchdog timeout** (default: 30 minutes, configurable), exit immediately with a log message. Do not queue or wait.
   - If the recorded process identity matches and exceeds the watchdog timeout, terminate only that verified process, log an alert, remove the lock, and retry atomic acquisition.
5. On pipeline completion (success or failure), release the lock only if the lock's `run_id` still matches the current run.

Lock release is wrapped in a `try/finally` to ensure cleanup on ordinary failures. If the process is killed by the watchdog or OS, the next run detects the stale lock through process identity checks.

> [!WARNING]
> This file-lock design assumes execution on a single persistent machine or node (such as a single VPS running cron). If deployed to ephemeral containers, serverless jobs, or multiple workers, the deployment strategy must provide an external single-instance lock instead of relying on local process identity.

### Incremental Processing

The pipeline state database (`data/state/pipeline.db`) tracks what has been processed:

- **Articles**: Each collected article is registered with its `article_id`, `source_id`, file path, publish date, fetch date, assigned `event_id` (null until aggregation processes it), and global exclusion flag `is_filtered` (so subsequent stages ignore spam/low-impact/media-only content).
- **Events**: Event ID, status, category, title, keywords, last-updated timestamp, and article count for fast lookups without scanning event JSON files.
- **Pipeline runs**: Run ID, start/end timestamps, status, counts of articles fetched/processed/errors.

The aggregation stage queries for articles where `event_id` is null to find unprocessed articles. The editorial stage queries for events where `updated_at` is newer than the event's last editorial generation timestamp.

### Database Schema & Migrations

SQLite schema changes are versioned. The database stores the current schema version in a `schema_version` table (or `PRAGMA user_version`), and migrations run automatically before any pipeline stage executes.

Migrations are organized as an append-only list of `(version, sql)` pairs in `pipeline/state.py::MIGRATIONS`. Each migration is the SQL needed to bring the database from the previous version up to its own version. Once released, a migration is immutable; later schema changes are added as new entries with higher version numbers. `migrate()` reads the highest recorded version from `schema_version` and runs every newer migration in order, then records each applied version. Fresh installs run all migrations; existing databases only run the deltas.

Current schema tables (schema version 9):

- `schema_version`: current schema version and applied timestamp.
- `feeds`: current source configuration keyed by source ID.
- `feed_state`: conditional request metadata (`etag`, `last_modified`), last fetch status, and failure state.
- `articles`: article/source identity, URLs, headline/summary, artifact path,
  publish/fetch timestamps, content type/language, collection metadata JSON,
  digest status/provenance/error, aggregation status/reason, collection run ID,
  `is_filtered` (the global exclusion flag), and nullable `event_id` (an article
  can belong to only one event). Full text and paywall signals remain in article
  JSON rather than dedicated columns.
- `article_fingerprints`: article ID plus canonical-URL, headline, summary, and content hashes retained after full staging cleanup.
- `events`: event ID, title, category, thread, keywords/entities JSON, status, created/updated timestamps, last editorial timestamp, article count, and confidence.
- `aggregation_windows`: fixed-window completion, prompt/model provenance, and per-window statistics used for sparse idempotent planning.
- `deduplication_reviews`: final event-pair decisions cached against both event input versions and the review prompt version.
- `source_run_stats`: durable per-source collection yield, skip, failure, and HTTP accounting for each run.
- `pipeline_runs`: run ID, stage, start/end timestamps, status, counters, and error summary.
- `item_errors`: per-feed, per-article, or per-event errors with retry counts and last error details.
- `llm_usage`: run ID, stage, model, prompt version, input/output/thinking/cached token counts, the service tier reported by the API, estimated cost, and occurrence time.
- `deduplication_prescreens`: cached prescreen candidate pairs keyed by the exact chunk content signature and prompt version, pruned after seven days.

Indexes must cover common incremental queries: articles with `event_id is null`, events by status and `updated_at`, events needing editorial regeneration, articles by canonical URL/hash input, and errors eligible for retry.

### Database Security

All SQLite interactions must use parameterized queries to prevent SQL injection. String concatenation for building SQL queries is strictly prohibited.

### Error Recovery

- Each stage logs errors per-item (per feed, per article, per event) without aborting the entire run. A failed feed fetch does not block other feeds. Collection uses HTTP/1.1 and retries transient transport/protocol errors with bounded backoff; this avoids shared HTTP/2 connection-state failures seen under live high concurrency. A failed article extraction does not block aggregation.
- Failed items are logged in the state database with error details and retry count. Items with repeated failures are skipped after a configurable retry limit (default: 3).
- If a pipeline run is killed by the watchdog, the next run picks up where the state database left off. Partially written JSON artifacts are avoided by the atomic write pattern (write to temp, then rename).

### Retention & Cleanup

Configurable retention windows (defaults in `config/pipeline.json`):

- **Staging articles**: Full extracted article JSON can be compacted after 3 days, but cleanup must retain durable article metadata, source links, canonical URL hashes, fingerprints, event assignments, and citation references in SQLite. Do not delete article rows that are needed for deduplication, archives, or published story source attribution.
- **Full article text**: Extracted `content_text` may be removed or replaced with a compact excerpt after the staging retention window if the article is no longer needed for active editorial regeneration.
- **Expired pending work**: Digest, aggregation, and maintenance use the same full staging-retention horizon. Unassigned articles older than that horizon are marked `filtered_expired`; maintenance restores `filtered_expired` rows that are still inside the horizon, which self-heals earlier horizon changes or premature expiration.
- **Stale events**: Events transition from `active` to `stale` after the configured stale threshold, then to `archived` after the retention window. Archived events are excluded from aggregation context and active story generation.
- **Event artifacts**: Maintenance reconciles active/stale event JSON against SQLite article assignments and deletes empty active/stale events.
- **Published stories**: Stories for archived events are removed from `active-stories.json`, but story JSON files are retained for auditability and possible future permanent archive pages.

Cleanup is idempotent and safe to skip (the pipeline grows slowly between runs), but the top-level pipeline runs it first so active aggregation context stays bounded. Retention values are tunable in `config/pipeline.json`.

## LLM Integration

### Boundaries

LLMs suggest and draft; deterministic code owns:

- JSON schema validation.
- ID uniqueness and format enforcement.
- File writes.
- Feed parsing and URL normalization.
- Source registry lookups.
- Build output.
- Category validation against `config/categories.json`.

### Auditability

LLM-generated artifacts store:

- Model name and version.
- Prompt version identifier.
- Generation timestamp.
- Input identity in the artifact contract: article IDs on events, event and
  source-article IDs on stories, and story IDs in homepage curation.

This keeps decisions auditable and makes reruns straightforward when prompts change.

### Model Flexibility

Pipeline stages depend on a small `JsonGenerator`-style boundary instead of a
vendor SDK. Production currently implements that boundary with the Gemini
Developer API: 3.5 Flash-Lite for bulk work and the ordered full-Flash chain for
selective review, destructive merge decisions, editorial, and curation. Local
models were evaluated during development but are not a configured runtime
fallback. Adding another hosted or local backend remains a backlog item and can
use the same structured-output boundary without changing stage logic.

### Cost Awareness

Model chains are built per purpose by `create_gemini_client`. Review work runs
3.8 Flash with 3.7 Flash as the capacity fallback; 3.5 Flash costs twice as
much and is appended only for editorial verification. Bulk work (digests,
grouping, prescreen, evidence extraction, the regeneration gate, category
sections) runs 3.5 Flash-Lite. Each chain starts with one flex-tier attempt at
half price on the flex model, bounded by `llm.flex_budget_seconds[purpose]`;
flex requests can be shed with 429/503 and are never upgraded server-side, so
the fallback chain treats a shed or overrun flex attempt like any capacity
failure and cools that model-plus-tier for five minutes. Implicit context
caching is not pursued: the reusable instruction prefixes are far below the
4,096-token minimum and the costly tokens are per-article content.

Token discipline: drafts see digests plus the verified ledger, not full article
text; evidence quotes are capped at three per claim and 320 characters;
homepage curation runs once per hourly run on compact cards and is reused when
the window's story set is unchanged; prescreen chunks are content-cached and
hash-bucketed; and a Lite "material update" gate skips the three full-Flash
calls when a verified story's new reports add no facts. `llm-usage` estimates
dollars from `llm.prices`, including thinking tokens billed as output.

- Aggregation uses fixed chunk-plus-overlap window calls with short summaries (headline + lead) to minimize token usage.
- Editorial uses **per-event** calls with full article text where quality matters most.
- Track token usage per run in the pipeline state database for monitoring.
- Use `run --dry-run` for a non-mutating preflight with no network or LLM calls,
  database changes, artifact writes, static build, or publish. Use
  `aggregate --dry-run` to inspect window and category-batch planning without an
  LLM client or mutations.

## Open Design Questions

- Should archived events eventually retain permanent public story pages beyond the current active/stale archive?
