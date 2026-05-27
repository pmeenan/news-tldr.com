# System Design: news-tldr.com

news-tldr.com is a filesystem-backed RSS aggregator that collects source articles, groups related coverage into durable events, writes neutral TL;DR summaries with political framing transparency, and publishes a CDN-cacheable static site.

## Design Goals

- No database server or application runtime. Pipeline state uses SQLite (a single file). Stage artifacts are JSON.
- Every pipeline stage is inspectable: JSON files on disk, SQLite queryable with standard tools.
- Preserve source attribution from collection through presentation.
- Separate data collection, aggregation, editorial, and rendering so each stage can be tested and rerun independently.
- Prefer neutral factual summaries. Surface partisan framing transparently when present, without adopting it.
- Support hourly pipeline runs with incremental processing. Stories evolve as new articles arrive across runs.

## Technology Stack

- **Pipeline**: Python. Libraries: `feedparser`, `httpx` (HTTP/2-enabled collection client and pooled HTTP/1.1 Gemini client), `h2`, `trafilatura` (article extraction), `beautifulsoup4` (custom scrapers), hosted LLM API client (Gemini Developer API by default). SQLite is accessed through Python's standard-library `sqlite3` module.
- **Presentation**: Astro (or similar frontend-focused SSG). Generates static HTML/CSS/JSON from published story artifacts.
- **State**: SQLite database for pipeline state, incremental processing tracking, and fast lookups. JSON files remain the human-readable artifacts for each stage.
- **Deployment**: The pipeline environment (including the SQLite database, staging files, and lock states) must **never** be web-accessible. The published SSG content in `dist/` is pushed to a separate web hosting location, which is a CDN-fronted static file server. The pipeline runs on a schedule (cron, GitHub Actions, or similar) strictly isolated from the public-facing site.

## High-Level Architecture

```mermaid
graph TD
    Feeds[config/feeds.json] --> Collect[1. Data Collection]
    Collect --> Articles[data/staging/articles/]
    Articles --> Digest[2a. Article Digest]
    Digest --> Aggregate[2b. Story Aggregation]
    Aggregate --> Events[data/events/]
    Events --> Editorial[3. Editorial]
    Editorial --> Stories[data/published/stories/]
    Stories --> Present[4. Presentation Build]
    Present --> Site[dist/ — Static HTML/CSS/JSON]

    State[(data/state/pipeline.db)] -.-> Collect
    State -.-> Digest
    State -.-> Aggregate
    State -.-> Editorial

    Collect -->|HTTP + readability| Pages[Article pages]
    Digest -->|Article LLM| ArticleDigest[Summaries, key facts & impact]
    Aggregate -->|Window LLM| Classify[Grouping & classification]
    Editorial -->|Per-event LLM| Summarize[Summary & framing]
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
    articles/YYYY/MM/DD/     # One JSON per collected article plus optional same-stem image sidecar.
    fetch-log/YYYY-MM-DD.jsonl
  events/
    <event_id>.json          # One file per durable event with article list and metadata.
  published/
    stories/<event_id>.json  # Editorial story JSON, one per event.
    active-stories.json      # Index of currently active stories for the presentation layer.

site/                        # Astro source and templates.
dist/                        # Generated CDN-deployable output.
```

Article staging directories use the article's **publish date** (from the feed or page metadata). When publish date is missing or unparseable, fall back to **fetch date** and set a `publish_date_estimated: true` flag in the article JSON. When a usable lead image is found, it is stored next to the article JSON with the same base filename and an image extension such as `.jpg`, `.png`, `.webp`, or `.gif`.

Each stage owns clear input and output directories. Stages write new files atomically (write to a temp file, then rename), never mutate upstream artifacts, and record enough metadata to explain later decisions.

## Stable IDs

Use deterministic IDs wherever possible.

- `source_id`: short stable slug from `config/feeds.json`, such as `ap`, `reuters`, `ars-technica`.
- `article_id`: SHA-256 hash of the canonical URL. Fallback: hash of `source_id` + feed GUID when canonical URL is unavailable. The chosen input is recorded in the article JSON so collisions can be investigated.
- `event_id`: `YYYY-MM-DD-slug`, where the date is when the event was first observed and the slug is LLM-suggested and code-validated for uniqueness. Example: `2026-05-24-iran-talks-resume`.
- `story_id`: matches the `event_id` of the event it covers. One story per event.

Event IDs should survive title changes, daily reruns, and duplicate articles. The LLM can suggest slugs, but code validates uniqueness against existing events in the state database. On slug collision within the same date, progressively refine the date component: try `YYYY-MM-DD-HH-slug`, then `YYYY-MM-DD-HHMM-slug`, until unique. All IDs (`source_id`, `article_id`, `event_id`) must be strictly sanitized to strip directory traversal sequences (like `../`) and invalid characters before being used in file paths.

Events can carry an optional `thread` tag (e.g., `iran-conflict-2026`) for linking related events over time. Threads are free-form metadata strings, not a separate registry. The presentation layer can use thread tags to build "related coverage" links.

## Stage 1: Data Collection

Inputs:

- `config/feeds.json`, the seed RSS/Atom feed list and scraper targets
- RSS/Atom feeds (HTTP)
- HTML homepages for Custom Scraper targets
- Article pages when feed entries have partial content

Responsibilities:

- Fetch each feed with conditional headers (`If-Modified-Since`, `ETag`) where supported. The pipeline bypasses robots.txt checks for feed URLs configured by the operator but strictly enforces robots.txt for all article page fetches.
- Parse publication time, updated time, headline, summary, feed content, GUID, canonical URL, author/byline, tags, and source metadata. Configure the XML parser (`feedparser` / `lxml`) to disable external entity expansion and DTD processing to protect against XXE injection and XML bomb DoS attacks.
- Extract lead-image candidates from feed media tags, enclosures, inline feed HTML, custom scraper card metadata, and article-page Open Graph/Twitter metadata when the article page is fetched.
- Fetch full article text when feed content is incomplete, using HTTP GET with readability-style extraction (`trafilatura` or similar). Feed content is deemed incomplete if it is less than 600 characters, equal to the summary, or not substantially longer than the summary (less than or equal to `len(summary) + 200` characters). Extraction should favor recall for full article coverage, fall back through alternate extractor modes, and keep the existing feed text when page extraction does not improve on it.
- Fetch one supported lead image per article when candidates are available, store it as a same-stem sidecar next to the article JSON, and record image metadata in the article JSON. Supported image formats are JPEG, PNG, WebP, and GIF.
- Detect likely paywalls and store a `paywall` flag with supporting signals.
- Normalize text enough for downstream processing while preserving raw source fields.
- Store one JSON file per collected source article.
- Record each collected article in the pipeline state database for incremental processing.
- Skip articles already recorded in the state database (deduplicate by `article_id`).

### Stage 1 Implementation

The collection implementation lives in the `pipeline` Python package:

- `pipeline/cli.py`: command-line entrypoint. `./.venv/bin/python -m pipeline.cli init-db` initializes the state database, `./.venv/bin/python -m pipeline.cli run --verbose` runs the completed pipeline stages in order (`collect`, `digest`, `aggregate`) under one full-duration pipeline lock, `./.venv/bin/python -m pipeline.cli collect` runs collection, `./.venv/bin/python -m pipeline.cli collect --verbose` streams incremental progress to stderr while preserving final JSON stats on stdout, and `./.venv/bin/python -m pipeline.cli clean-data --yes` removes local generated pipeline state for a fresh run.
- `pipeline/state.py`: SQLite schema and migration entrypoint. The schema includes feeds, feed conditional request state, articles, article fingerprints, events, pipeline runs, item errors, and LLM usage.
- `pipeline/lock.py`: atomic lock file acquisition/release with PID and Linux process start-time verification, plus stale-lock recovery based on the configured watchdog timeout.
- `pipeline/http_client.py`: async HTTP client with browser-like desktop Chrome request headers, per-domain rate limiting, robots.txt and crawl-delay enforcement, retry/backoff handling, and manual redirect validation.
- `pipeline/security.py`: SSRF guardrails. Every initial URL and redirect target is restricted to `http`/`https`, resolved before fetch, and rejected when it maps to loopback, private, link-local, multicast, reserved, unspecified, or blocked-port destinations.
- `pipeline/collect.py`: feed collection, feed parsing, scraper engine routing, article extraction, paywall signal detection, article JSON writes, database registration, and fetch-log writes.
- `pipeline/scrapers/`: modular engine for custom site scrapers (e.g., AP News, MotorTrend) that generate synthetic feed entries using `beautifulsoup4` when standard RSS feeds are unavailable.

Collection writes article JSON under `data/staging/articles/YYYY/MM/DD/` and appends run logs to `data/staging/fetch-log/YYYY-MM-DD.jsonl`. The SQLite database stores only state/index fields plus JSON metadata needed by later stages; full extracted article text remains in the staging JSON.

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
- **Image Fallback Strategy**: If explicit primary lead image metadata is present, general body images (`html_img`) are discarded. If the chosen primary lead image fails to fetch or is blocked (e.g. by robots.txt), the collector does not fall back to other candidate images.
- **Origin Circuit Breaker**: If image fetches from a specific scheme + host origin experience 3 consecutive failures (due to HTTP status >= 400, connection errors, timeouts, or robots.txt blocks), the origin is disabled for the rest of the collection pass, and subsequent image requests to it are skipped.


### Feed Config

Entries in `config/feeds.json` include: `source_id`, `source_name`, `feed_url`, optional `site_url`, default category, content/paywall hints, and fetch behavior overrides. Sources can be staged with `"enabled": false`.

The current source catalog contains 83 enabled sources, including the AP News and MotorTrend custom scrapers. `config/source-policy.json` is kept aligned with `config/feeds.json` by `source_id` so editorial stages can resolve source metadata without guessing.

Custom scraper entries use `"feed_type": "scraper"` and a `fetch.scraper_module` value such as `pipeline.scrapers.ap` or `pipeline.scrapers.motortrend`. Scraper module names are restricted to the `pipeline.scrapers.*` namespace at load time. Scrapers must verify that resolved entry URLs stay on the configured `site_url` host and match an anchored article-path pattern so off-site links and unrelated paths are not enqueued. Each candidate anchor must also look like a headline link — either nested inside an `<article>` / `h1`–`h6` ancestor or itself wrapping a heading element — so subscribe/sign-in/site-chrome anchors that happen to share the article path prefix are filtered out. Scrapers return feed-like entries and may include an `image_url` discovered from listing-card markup, but they do not download image bytes themselves. Image downloads are centralized in the collector so RSS feeds and scrapers share the same safety checks, content-type validation, size limits, and sidecar write behavior.

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
    "model": "gemini-3.1-flash-lite",
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
  "image": {
    "url": "https://example.com/image.jpg",
    "path": "data/staging/articles/2026/05/24/sha256-of-canonical-url.jpg",
    "content_type": "image/jpeg",
    "bytes": 12345,
    "source": "media_content",
    "width": 1200,
    "height": 800
  },
  "collection": {
    "feed_url": "https://example.com/rss",
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

The CLI entrypoint is:

```bash
./.venv/bin/python -m pipeline.cli digest --verbose
```

Optional `--range-start`, `--range-end`, `--limit`, and `--concurrency` flags make the stage practical to debug independently from aggregation. `--force` regenerates current-version digests instead of treating them as completed, which is useful after prompt or validation changes. By default, if no range is specified, the processing window is restricted to articles published within the current and previous UTC days (starting at 00:00:00 UTC of yesterday) to avoid processing older retained data.

## Stage 2b: Story Aggregation

Inputs:

- Digested, unprocessed article records from the pipeline state database
- Article JSON files from `data/staging/articles/`
- Existing event files from `data/events/`
- `config/categories.json` for valid category IDs

Responsibilities:

- Query the state database for articles not yet assigned to an event.
- Use completed article digests instead of site-provided teaser summaries where available.
- Filter out non-news, promotional/affiliate/product/deals, puzzle/game help, gambling picks, archive/index, video-carousel, and no-substantive-content artifacts before grouping. Remaining low-impact articles are filtered using the article digest's category/vertical impact score and `aggregation.min_category_impact` from `config/pipeline.json`. Filtered articles are marked with `aggregation_status = filtered_*` so completed windows do not rerun forever on intentionally skipped rows.
- Video/carousel exclusion should not rely only on the digest model. Aggregation should also apply deterministic URL/path and collection-signal checks for obvious media pages (for example `/videos/`, `/video/`, gallery/carousel paths, and untitled pages whose extracted text is dominated by video listings) before grouping.
- Deduplicate near-identical exact article reprints (e.g., wire service stories) using content and canonical URL hashes during digest and aggregation stages. Headline and summary hashes remain available in `article_fingerprints` for collection-time near-duplicate suppression, but are not used to short-circuit digest generation.
- Classify each article's content type: news, opinion, analysis, review, or unknown.
- Assign each article to a category from `config/categories.json`.
- Group articles into durable story clusters: either match an existing event or create a new one. A cluster may include multiple angles on the same developing news subject, such as updates, reactions, analysis, and local/international framing. The editorial stage is responsible for separating those angles in the summary. Match using the headline and a brief paragraph summary, not the full article text.
- Maintain event matching metadata (`keywords` and major named entities) for future aggregation context.
- Score each cluster's initial newsworthiness on two axes: global homepage impact and impact within its own category/vertical.
- Update event files and the state database with new article assignments.
- Filter standalone opinion pieces from event creation (opinion articles can be attached to an existing event but should not create one alone).

### LLM Integration for Aggregation

Aggregation uses the Gemini Developer API by default, authenticated with an AI
Studio API key in local `.env` configuration:

```bash
GEMINI_API_KEY=your-ai-studio-api-key
GEMINI_MODEL=gemini-3.1-flash-lite
```

The initial hosted model is `gemini-3.1-flash-lite`, called through the
`generativelanguage.googleapis.com` API with the `x-goog-api-key` header using
a pooled `httpx` HTTP/1.1 client. The LLM client intentionally avoids HTTP/2
because the pipeline's small concurrent request pool benefits more from
predictable parallel connections than from multiplexing.
Aggregation requests should use deterministic generation settings and Gemini
structured output (`responseMimeType: application/json` plus a response JSON
schema). API keys must not be written to logs, command output, JSON artifacts,
or committed files.

Aggregation runs over fixed UTC publish-time chunks with a short overlap lookahead. The default aggregation chunk is 3 hours with an additional 1-hour overlap, so actual LLM windows are 4 hours wide and anchored to UTC boundaries (`00:00-04:00`, `03:00-07:00`, `06:00-10:00`, `09:00-13:00`, `12:00-16:00`, `15:00-19:00`, `18:00-22:00`, `21:00-01:00`). Hourly cron runs keep returning to those same fixed windows rather than shifting the window start based on the current hour or first unassigned article. The digest stage should run before aggregation; aggregation then filters non-news/spammy/video-carousel artifacts and articles whose digest category/vertical impact score is below the configured threshold. The state database records completed aggregation windows; normal pipeline runs use sparse planning, selecting only fixed window starts that have unassigned articles in that publish-time bucket, plus the latest completed window when it falls in range. This avoids rerunning every intervening old window when late-arriving articles appear with older publication timestamps. Forced aggregation remains continuous so reset coverage is explicit. For each aggregation window, we load all eligible articles published within those hours (both assigned and unassigned) and send their metadata (headline + digest summary/key facts when available, otherwise collected summary + source + publish date) to the LLM to:

1. Classify each article's content type and category.
2. Group articles into story clusters (matching and referencing existing event IDs where applicable) by identifying multiple outlets and angles reporting on the same developing news subject.
3. Suggest new event IDs and slugs for novel events.
4. Suggest optional `thread` tags for linking related events.

Prompt shape is part of the contract. The first aggregation pass should avoid free-text fields and ask for compact, order-preserving structured output using numeric enum codes rather than copied article IDs. Deterministic code maps each output row back to the input article by position. This reduces malformed JSON, repeated text inside string fields, and ID-copying errors. Event naming, keyword/entity generation, and slug suggestions should happen in a second smaller call after candidate event assignments are known.

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

Post-aggregation deduplication collects candidate event pairs from four complementary gates and then runs the same strict per-pair LLM merge call on the union:

1. **Slug / title heuristics** — base-slug equality, title-word overlap (`_titles_similar`), and highly similar individual article headlines (catches reprints with different lead facts).
2. **Keyword-overlap gate** (`_keyword_overlap_candidates`) — within a single category-group batch, pairs events that share at least 2 distinctive event-level keywords after stripping a global static stopword list and a per-batch *dynamic* hot-keyword stopword list (`_dynamic_keyword_stopwords`). The dynamic list flags any keyword appearing in ≥20% of the batch's events AND in ≥4 events (an absolute floor that prevents tiny batches from stopwording distinctive entities like "ferrari" that only appear in the 2 candidate duplicates).
3. **LLM pre-screen gate** (`_llm_prescreen_candidates`) — loose-recall LLM calls over category-group batches (`politics_gov`, `news_business_{us,world,business}`, `sci_tech`, `leisure`), sending each event's id, title, top-6 filtered keywords, and top-3 article headlines. The model is instructed to err on the side of inclusion; false positives are filtered by the strict per-pair merge call. Each call is bounded to `DEDUPLICATION_MAX_EVENTS_PER_PRESCREEN_BATCH = 40` events and chunked when batches exceed that. For oversized batches, the top high-article-count anchor events are repeated into every prescreen chunk so large ongoing stories can still be compared with later singleton updates that would otherwise land in a different chunk. `news_business` also gets an additional parent-level cross-category prescreen because market, business, U.S., and world framings often describe the same underlying event from different verticals. All prescreen chunks across all category-group batches share one worker pool up to `aggregation.deduplication_concurrency` (default: 16). Prompt version: `deduplication-prescreen-v1`. Token usage and errors are recorded under stage `deduplication_prescreen`.
4. **Per-pair merge LLM** — for every unique candidate pair from the union of (1)–(3), the existing `_build_event_merge_prompt` call decides `should_merge` with confidence ≥ `DEDUPLICATION_MERGE_CONFIDENCE_THRESHOLD` (0.8). This is the single source of truth for actually merging; over-merging risk lives here only. Candidate-pair reviews run concurrently in rounds of disjoint event IDs, then accepted merges are applied serially in deterministic order so one merge cannot race another merge touching the same event.

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

The initial aggregation implementation lives in `pipeline/aggregate.py` and is exposed by `./.venv/bin/python -m pipeline.cli aggregate`. It plans 3-hour aggregation chunks with a 1-hour overlap lookahead (4-hour LLM windows fixed to `00/03/06/09/12/15/18/21` UTC starts), skips completed windows using `aggregation_windows`, loads category-impact-eligible articles in each planned window, calls Gemini with headline + digest/summary metadata in category-bounded batches, validates that every article index appears exactly once, scores resulting groups for newsworthiness from digest impact or fallback scoring, writes `data/events/<event_id>.json`, upserts the `events` table, assigns `articles.event_id`, and records completed windows. Windows remain sequential because each window should see event state from earlier windows; within a window the LLM-only category batch work is concurrent. The command supports `--range-start`, `--range-end`, `--limit-windows`, `--dry-run`, `--force`, and `--verbose`. By default, if no range is specified, aggregation bounds still cover unassigned articles in the current and previous UTC days (starting at 00:00:00 UTC of yesterday), but non-force planning sparsely selects only the fixed UTC windows that have unassigned articles in their own publish-time bucket, plus the latest completed window when applicable. If no unassigned articles exist within this window, the run completes early without planning windows. With `--force`, default planning instead uses recently completed digests in the current and previous UTC days, clears prior event assignments and aggregation-stage filter decisions for the actual planned window coverage, deletes or trims affected event artifacts, and then reruns the continuous window range even if windows were previously marked completed.

Event naming in this first pass is deterministic and intentionally simple: existing event IDs are reused when a group contains already-assigned articles; otherwise code derives a stable date + headline slug and stores lightweight keyword metadata. Richer title/slug/entity generation remains a follow-up LLM pass.

Digest regeneration and review on May 25, 2026 (article-digest-v3) verified the layered cap system and controlled-vocabulary rationale codes against a stratified sample (39 across all 11 categories + 6 edge cases). Compared with v2: `paywalled` started being used (0 → 27 articles), `extraction_noise` doubled (23 → 52), `novelty=low_signal` nearly tripled (40 → 116), `novelty=breaking` dropped from 294 → 246 (research papers no longer auto-tagged as breaking), `novelty=analysis` more than doubled (60 → 156), `impact_capped` events roughly quadrupled (~40 → 176) as the new vendor_announcement / multi-topic / unconfirmed_injury / recycled_content rationales started firing, and `study_stage` was populated on 49 research articles with the full enum spread. Asymmetric caps (vendor_announcement, unconfirmed_injury) and HIGH-rationale cap bypass (public_health Ebola wires) were both confirmed working on sampled articles. Known residual minor issues: the date-leak rule against inserting `published_at` year/month/day into the summary is followed most of the time but still drifts occasionally, and one paywalled WaPo article was tagged `thin` despite the prompt explicitly naming "Democracy Dies in Darkness" as a paywall signal (caps still fired correctly via the noise-cap path so downstream behavior is unaffected).

Prompt version `article-digest-v6` includes URL, canonical URL, and estimated-publish-date metadata in the digest prompt; tightens guidance for video, gallery, profile/background, media-transcript, stale estimated-date, and stale archive/background pages; narrows `study_stage` to covered medical/biological/pharmaceutical/materials research; and gives category-impact guidance for legitimate vertical stories whose global impact is low. The digest stage also deterministically filters obvious `/video/`, `/videos/`, `/gallery/`, and `/galleries/` URL paths before LLM calls, filters stale estimated-date pages when URL or live-page text dates are clearly older than the collected timestamp, and drops irrelevant `study_stage` values before writing `llm_digest`. The code-side `study_stage` gate uses word-boundary matching and excludes climate, space, aeronautical, software, paleontology, and general earth-science contexts unless there is a strong biomedical or materials signal.

#### Event Merging and Reassignment

To handle stories that develop over longer periods and span across different 3-hour windows, the system implements a two-layered event merging strategy:

1. **Proactive Active-Events Matching**: During the window aggregation pass, the aggregator queries the SQLite database for events updated within the last 48 hours matching the categories of the window articles. These active events are filtered to only include those whose title shares at least 2 non-stopword words with at least one article's headline in the current window (or shares the single non-stopword if the event title only has one). This prevents context bloat and false-positive groupings in the LLM. The matched active events are passed to the grouping LLM call (containing their IDs, categories, and titles/headlines). The LLM is instructed to assign window articles directly to these existing events where appropriate, returning their `existing_event_id` in the JSON groups response. The validator only preserves event IDs that were actually included in that prompt.
2. **Reactive Post-Aggregation Deduplication**: At the end of the aggregation pass, a reactive deduplication process runs over all active events updated in the last 48 hours. It checks for candidate event pairs using suffix conflicts (e.g. `event-name` vs `event-name-2` date-slug collisions), title word overlaps, and highly similar article headlines. For each candidate pair, a targeted LLM call evaluates their full article lists, headlines, and digests to decide if they represent the exact same real-world event. A merge requires both `should_merge=true` and confidence of at least `0.80`.

When a merge is triggered (either proactively via the window LLM or reactively via post-aggregation deduplication), the aggregator selects a winning event ID, loads historical article IDs from both events, merges their article lists, updates the winning event's JSON and database assignments, deletes the merged-away events' JSON files, and removes their SQLite database entries to prevent historical data loss.

Model evaluation notes:

- Local model evaluation on May 24, 2026 found that `gemma4:26b` with `think: false` and compact numeric schema produced valid structured output in about 29 seconds for an 8-article CPU batch, but larger title-clustering experiments were too slow for the pipeline's needs.
- `qwen3.6:27b` with `think: false` produced good structured output but was much slower, about 227 seconds for the same 8-article CPU batch.
- `llama3.1:8b` produced valid structured output but lower classification quality.
- Free-text JSON fields and calls without local-model thinking controls caused empty responses, looping, malformed JSON, or poor reliability in local tests.
- Direct Gemini Developer API smoke tests with an AI Studio key succeeded for `gemini-2.5-flash-lite` and `gemini-3.1-flash-lite`; the project default is now `gemini-3.1-flash-lite`.

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
    "model": "gemini-3.1-flash-lite",
    "prompt_version": "newsworthiness-v1"
  }
}
```

Event `status` values:

- `active`: received new articles within the staleness threshold (default: 48 hours, configurable via `stale_threshold_hours` in `config/pipeline.json`).
- `stale`: no new articles beyond the staleness threshold. Still visible on the site but deprioritized.
- `archived`: older than the configured retention window. Excluded from active story generation.

## Stage 3: Editorial

Inputs:

- Event files with updated article lists (events that gained new articles since last editorial run)
- Source article JSON files for those events
- `config/source-policy.json` for bias/reliability metadata

Responsibilities:

- Generate one neutral TL;DR story per event.
- Include the important facts, relevant uncertainty, and known missing context.
- Attribute claims to source articles without copying article prose.
- For clearly political events where sources frame the story in divergent ways, extract left-perspective and right-perspective summaries with source attribution.
- Rank stories by importance using the Stage 2 newsworthiness scores plus freshness, source quality, source count, and editorial judgment.
- Write or update story JSON files.
- Update `data/published/active-stories.json` with the current set of active stories.

### LLM Integration for Editorial

Editorial LLM calls are **per-event**. Each call receives:

- The event metadata (title, category, article count).
- Full text (or substantial excerpts) of all source articles for that event.
- Source policy metadata (bias labels, reliability) for each source.
- Instructions for neutral summarization and political framing extraction.

Each event is a separate LLM call because the model needs the full article context to produce accurate summaries and detect framing. Grouping multiple events into one call would exceed context limits and reduce quality.

### Editorial Rules

- Prefer "what happened, who is affected, what changed, what remains uncertain."
- Do not amplify unsupported claims because they are repeated.
- Do not infer motive unless sources provide evidence.
- Do not flatten genuine disagreement into false certainty.
- Opinion-only pieces can be cited as examples of reaction/framing but should not drive the story's factual claims.

### Political Framing

For events in categories likely to have partisan framing (primarily `politics`, `us`, and sometimes `world`), the editorial stage extracts divergent perspectives:

- The TL;DR and key facts remain **neutral**.
- A `political_framing` section surfaces clearly left-leaning and clearly right-leaning source perspectives with brief summaries of how each side frames the story.
- Sources are attributed to each perspective based on `config/source-policy.json` labels and article-level framing analysis.
- This section is **only present** when meaningful divergence exists. Do not force framing onto non-political stories or stories where sources largely agree.

The goal is transparency: readers see the neutral summary AND can optionally see how partisan outlets are spinning the story.

### Published Story JSON Sketch

```json
{
  "story_id": "2026-05-24-iran-talks-resume",
  "event_id": "2026-05-24-iran-talks-resume",
  "category": "world",
  "thread": "iran-conflict-2026",
  "headline": "Talks resume after overnight strikes",
  "dek": "Negotiators returned to talks as officials reported new strikes and disputed casualty counts.",
  "tldr": [
    "Negotiators resumed talks on Sunday after overnight strikes.",
    "Officials gave different accounts of the damage and casualties.",
    "The next scheduled diplomatic step is expected later this week."
  ],
  "key_facts": [
    {
      "text": "Talks resumed on May 24, 2026.",
      "source_article_ids": ["sha256-abc123"]
    }
  ],
  "uncertainties": [
    {
      "text": "Casualty figures remain disputed across sources.",
      "source_article_ids": ["sha256-abc123", "sha256-def456"]
    }
  ],
  "political_framing": null,
  "sources": [
    {
      "article_id": "sha256-abc123",
      "source_name": "Reuters",
      "headline": "Talks restart after strikes",
      "url": "https://reuters.com/..."
    }
  ],
  "importance": {
    "score": 0.82,
    "signals": ["multiple_sources", "fresh", "international_impact"]
  },
  "created_at": "2026-05-24T15:00:00Z",
  "updated_at": "2026-05-24T19:00:00Z",
  "llm_metadata": {
    "model": "model-name",
    "prompt_version": "editorial-v1",
    "generated_at": "2026-05-24T19:00:00Z"
  }
}
```

When political framing is present:

```json
"political_framing": {
  "summary": "Coverage diverges on whether the strikes were proportionate.",
  "left_perspective": {
    "summary": "Coverage emphasizes civilian casualties and questions the military rationale.",
    "source_article_ids": ["sha256-ghi789"]
  },
  "right_perspective": {
    "summary": "Coverage emphasizes security threats and frames the strikes as a necessary response.",
    "source_article_ids": ["sha256-jkl012"]
  }
}
```

### Active Stories Index

`data/published/active-stories.json` is a lightweight index regenerated each pipeline run:

```json
{
  "generated_at": "2026-05-24T19:00:00Z",
  "stories": [
    {
      "story_id": "2026-05-24-iran-talks-resume",
      "category": "world",
      "headline": "Talks resume after overnight strikes",
      "importance_score": 0.82,
      "source_count": 5,
      "created_at": "2026-05-24T15:00:00Z",
      "updated_at": "2026-05-24T19:00:00Z"
    }
  ]
}
```

The presentation layer reads this index to decide which stories to render and how to order them, then reads individual story JSON files for full content. The presentation layer owns the time window (e.g., "last 24 hours", "last 48 hours") and filtering logic.

## Stage 4: Presentation

Inputs:

- `data/published/active-stories.json`
- Individual story JSON files
- `config/categories.json`

Responsibilities:

- Build static pages from story JSON using Astro (or similar frontend-focused SSG). Treat all imported JSON content (headlines, summaries, extracted text) as untrusted; ensure Astro templates auto-escape this content and use `DOMPurify` (or Astro's native equivalent) if any raw HTML must be rendered.
- The main page shows all active stories in a **rolling time window**, editorially ranked by importance. Category tabs (one per category from `config/categories.json`, plus an "All" default) filter the same ranked list client-side — they are not separate pages with independent layouts.
- Build individual story pages with TL;DR, key facts, uncertainties, source links, and political framing sections.
- Build archive pages or indexes for older stories.
- Render source links with paywall indicators and uncertainty notes.
- Generate lightweight JSON API files for potential future client-side features.
- Output must be fully static and CDN-cacheable with no server runtime.
- The presentation layer must implement a strict Content Security Policy (CSP) (via `<meta>` tags or CDN headers) and use Subresource Integrity (SRI) for any external assets.

The presentation build runs after each pipeline run. It reads the current state of published stories and regenerates the site. Pages for stories that haven't changed can be cached or skipped (incremental builds) if the SSG supports it.

## Pipeline Operations

### CLI Output Contract

Pipeline commands that can run long enough to feel idle in an interactive shell must support `--verbose`. Verbose progress/status is written to stderr, while final machine-readable output remains on stdout. The collection command currently implements this contract with `./.venv/bin/python -m pipeline.cli collect --verbose`.

The `clean-data` command removes local generated pipeline state for a fresh run: the SQLite database and sidecars, staged article files, event JSON, published JSON, and fetch logs by default. It requires `--yes`, refuses to run while `data/state/pipeline.lock` exists, and can keep fetch logs with `--keep-fetch-log` or override the lock guard with `--ignore-lock`.

### Concurrency Control

Only one pipeline run may execute at a time. Concurrency is controlled by a lock file at `data/state/pipeline.lock`. Individual stage commands acquire this lock for that stage. The combined `run` command acquires the same lock once for the entire collect → digest → aggregate sequence and calls the inner stage implementations without reacquiring it, so another scheduled run or manual stage command cannot slip between stages.

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

Initial tables:

- `schema_version`: current schema version and applied timestamp.
- `feeds`: source ID, feed URL, conditional request metadata (`etag`, `last_modified`), last fetch status, and timestamps.
- `articles`: article ID, source ID, canonical URL, URL hash input, headline, summary, file path, publish/fetch timestamps, language, content type, paywall status, retry/error state, `is_filtered` (global exclusion flag), and `event_id` nullable until aggregation (articles can only belong to one event).
- `article_fingerprints`: article ID, normalized headline fingerprint, content hash, compact text fingerprint, and near-duplicate metadata retained after full staging cleanup.
- `events`: event ID, title, category, thread, keywords/entities JSON, status, created/updated timestamps, last editorial timestamp, article count, and confidence.
- `pipeline_runs`: run ID, stage, start/end timestamps, status, counters, and error summary.
- `item_errors`: per-feed, per-article, or per-event errors with retry counts and last error details.
- `llm_usage`: run ID, stage, backend/model, prompt version, token counts, cost fields when available, and input/output artifact references.

Indexes must cover common incremental queries: articles with `event_id is null`, events by status and `updated_at`, events needing editorial regeneration, articles by canonical URL/hash input, and errors eligible for retry.

### Database Security

All SQLite interactions must use parameterized queries to prevent SQL injection. String concatenation for building SQL queries is strictly prohibited.

### Error Recovery

- Each stage logs errors per-item (per feed, per article, per event) without aborting the entire run. A failed feed fetch does not block other feeds. A failed article extraction does not block aggregation.
- Failed items are logged in the state database with error details and retry count. Items with repeated failures are skipped after a configurable retry limit (default: 3).
- If a pipeline run is killed by the watchdog, the next run picks up where the state database left off. Partially written JSON artifacts are avoided by the atomic write pattern (write to temp, then rename).

### Retention & Cleanup

Configurable retention windows (defaults in `config/pipeline.json`):

- **Staging articles**: Full extracted article JSON can be compacted after 3 days, but cleanup must retain durable article metadata, source links, canonical URL hashes, fingerprints, event assignments, and citation references in SQLite. Do not delete article rows that are needed for deduplication, archives, or published story source attribution.
- **Full article text**: Extracted `content_text` may be removed or replaced with a compact excerpt after the staging retention window if the article is no longer needed for active editorial regeneration.
- **Stale events**: Events transition to `archived` status after the retention window. Archived events are excluded from aggregation context and active story generation.
- **Published stories**: Stories for archived events are removed from `active-stories.json` but story JSON files are retained indefinitely for archive pages.

Cleanup is idempotent and safe to skip (the pipeline grows slowly between runs). Retention values are tunable in `config/pipeline.json`.

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

Every LLM-generated artifact stores:

- Model name and version.
- Prompt version identifier.
- Generation timestamp.
- Input artifact references (which article IDs or event IDs were used as input).

This keeps decisions auditable and makes reruns straightforward when prompts change.

### Model Flexibility

The LLM integration should abstract the model backend. The Stage 2 aggregation
default is the Gemini Developer API with `gemini-3.1-flash-lite`; other
backends remain swappable behind the same interface. The system may use:

- Hosted API models for aggregation and high-quality editorial summaries.
- Local models for fallback, development, or privacy-sensitive experiments when latency is acceptable.
- Different models for different stages (e.g., inexpensive hosted model for aggregation, stronger model for editorial).

The abstraction should support swapping backends without changing pipeline logic.

### Cost Awareness

- Aggregation uses fixed chunk-plus-overlap window calls with short summaries (headline + lead) to minimize token usage.
- Editorial uses **per-event** calls with full article text where quality matters most.
- Track token usage per run in the pipeline state database for monitoring.
- Support dry-run mode that processes everything except LLM calls (useful for testing collection and aggregation logic).

## Open Design Questions

- Which Astro plugins/integrations are needed for the initial site build?
