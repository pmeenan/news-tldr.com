# news-tldr.com

Source code for the RSS aggregator and summarizer at https://news-tldr.com.

## Overview

news-tldr.com is a filesystem-backed RSS aggregator that parses web feeds, extracts article content, groups related coverage into durable topics/events, and uses AI/LLMs to generate concise neutral TL;DR summaries. Published news content is fully static and served at [news-tldr.com](https://news-tldr.com/); an isolated origin endpoint optionally synchronizes anonymous read history.

## Key Features

- **RSS/Atom Feed Parsing**: Periodic updates of subscribed sources.
- **Text-Only Collection**: Article feeds and page text are retained without downloading publisher image assets.
- **Filesystem Pipeline**: JSON artifacts and a SQLite state database connect collection, aggregation, editorial, and presentation stages without a server runtime.
- **AI-Powered Summaries**: Automatic generation of brief, sourced summaries across multiple articles covering the same event.
- **Static Presentation**: A dependency-free Python renderer builds the reader interface from editorial JSON and publishes it without a server runtime.
- **Optional Anonymous Sync**: Readers can share a private capability link to synchronize three days of read-story state without an account.

## Project Resources

For developers and agents working on the project, refer to these documents:

- **[docs/design.md](docs/design.md)**: Canonical system architecture, data contracts, trust boundaries, and deployment design.
- **[docs/pipeline.md](docs/pipeline.md)**: Current pipeline execution model, stage ownership, concurrency, and failure behavior.
- **[docs/plan.md](docs/plan.md)**: Completed milestones and remaining backlog.
- **[docs/style.md](docs/style.md)**: Python and generated-frontend coding conventions.
- **[AGENTS.md](AGENTS.md)**: Agent working agreements and chronological handoff history.

## Getting Started

Add RSS or Atom sources to `config/feeds.json`. Copy the example entry, set a stable `source_id`, provide the `feed_url`, and set `enabled` to `true` when the source should be collected.

## Development Setup

The project uses a Python virtual environment located in `.venv` for local python3 dependencies.

### Virtual Environment

To activate the virtual environment and install dependencies:
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

This installs the feed parser, HTTP client, article extraction, and scraper dependencies used by the collection pipeline.

Any scripts or pipeline executions should run using the Python interpreter inside `.venv` (e.g., `./.venv/bin/python`).

### Hosted LLM Setup

Article digestion, story aggregation, editorial generation, and homepage curation
use the Gemini Developer API through the project's `httpx`-based client.
Create a local `.env` file with an AI Studio API key and the two model tiers:

```bash
GEMINI_API_KEY=your-ai-studio-api-key
GEMINI_BULK_MODEL=gemini-3.5-flash-lite
GEMINI_REVIEW_MODEL=gemini-3.7-flash
GEMINI_REVIEW_FALLBACK_MODELS=gemini-3.6-flash,gemini-3.5-flash
GEMINI_REVIEW_LITE_FALLBACK_MODEL=gemini-3.5-flash-lite
```

`GEMINI_MODEL` remains a supported fallback for the bulk tier. The `.env` file
is ignored by git and must not be committed. Bulk digestion, grouping, scoring,
and candidate discovery use 3.5 Flash-Lite with minimal thinking. Borderline
article-filter decisions, editorial, and final event-merge decisions try 3.7,
3.6, then 3.5 Flash with low thinking when capacity is constrained.
Deduplication may fall back once more to 3.5 Flash-Lite, but a Lite result can
only reject or defer a pair and can never authorize a merge. All calls use
Gemini structured output and validated responses. Safety-sensitive editorial
inputs that produce empty responses across all full-Flash tiers get one compact
digest/key-fact retry on the same full-Flash chain; editorial never uses Lite.

### Pipeline Commands

Initialize or migrate the SQLite state database:
```bash
./.venv/bin/python -m pipeline.cli init-db
```

Run stage 1 data collection:
```bash
./.venv/bin/python -m pipeline.cli collect
```

Run the complete logical pipeline (`maintenance`, `collect`, `digest`,
`aggregate`, `editorial`, then `presentation`). After maintenance, the runner
overlaps collection with bounded pre-existing backlog work before continuing
through the ordinary downstream pass. Presentation builds `dist/` and, with the
checked-in production configuration, publishes it to `/var/www/news-tldr.com/`.
The command holds the shared pipeline lock for the full duration so scheduled
invocations do not interleave with each other or with individual stage commands:
```bash
./.venv/bin/python -m pipeline.cli run --verbose
```

After maintenance, the combined command snapshots the existing article queue
and starts collection in parallel with backlog processing. Existing editorial
work is drained and published first; prior digest and aggregation work is then
processed only through the snapshot boundary, so newly fetched articles cannot
move the finish line. If backlog remains, collection is still checkpointed but
new downstream work is deferred and the command exits nonzero. SQLite writers
use a 30-second busy timeout to tolerate brief collection/editorial contention.

Force stages that support forced recomputation (`digest`, `aggregate`, and `editorial`) while
running the completed pipeline:
```bash
./.venv/bin/python -m pipeline.cli run --verbose --force
```

Build the site during a full run without updating production:

```bash
./.venv/bin/python -m pipeline.cli run --verbose --no-publish
```

Preflight a complete run without network calls, LLM calls, database changes,
artifact writes, or publishing:

```bash
./.venv/bin/python -m pipeline.cli run --dry-run --verbose
```

Run the maintenance/retention stage on its own. It advances old events through
the active/stale/archived lifecycle, expires old unassigned articles outside
the configured staging-retention horizon, restores mistakenly expired articles
that are still inside that horizon, reconciles active/stale event JSON with
SQLite assignments, and compacts old filtered or archived article JSON:
```bash
./.venv/bin/python -m pipeline.cli maintenance --verbose
```

Preview maintenance work without mutating SQLite state or JSON artifacts:
```bash
./.venv/bin/python -m pipeline.cli maintenance --verbose --dry-run
```

Print incremental collection progress to stderr while keeping final stats on stdout:
```bash
./.venv/bin/python -m pipeline.cli collect --verbose
```

Run the article digest stage. This generates factual per-article summaries,
key facts, and article-level impact scores before story aggregation. With no
explicit range, it covers the configured staging retention horizon:
```bash
./.venv/bin/python -m pipeline.cli digest --verbose
```

Regenerate existing current-version digests after prompt or validation changes:
```bash
./.venv/bin/python -m pipeline.cli digest --verbose --force
```

Run stage 2 story aggregation. Aggregation consumes completed article digests,
filters out non-news/promotional/video-carousel items and low category-impact
articles according to `config/pipeline.json`, then groups eligible articles
into events. Its default lookback matches the staging retention horizon:
```bash
./.venv/bin/python -m pipeline.cli aggregate --verbose
```

Force aggregation to re-plan from recently completed digests, clear prior
event assignments/filter decisions in the planned window coverage, and rerun
those windows even if they were already marked completed:
```bash
./.venv/bin/python -m pipeline.cli aggregate --verbose --force
```

Run stage 3 editorial generation. Editorial performs one generation operation
per changed active/stale event through the ordered full-Flash fallback chain,
validates source citations, writes
`data/published/stories/<event_id>.json`, and regenerates the active story index:
```bash
./.venv/bin/python -m pipeline.cli editorial --verbose
```

Use `--force` to regenerate unchanged stories, `--limit` for a controlled batch,
or repeat `--event-id <id>` to evaluate specific events:
```bash
./.venv/bin/python -m pipeline.cli editorial --verbose --limit 10
```

Build and publish the static presentation without running upstream stages:

```bash
./.venv/bin/python -m pipeline.cli present --verbose
```

Use `--build-only` to preview `dist/` without publishing, or `--publish-dir`
with an absolute path to override the configured destination:

```bash
./.venv/bin/python -m pipeline.cli present --verbose --build-only
```

Presentation defaults live under `presentation` in `config/pipeline.json`.
`publish_enabled: true` makes every normal top-level `run` publish automatically;
`site_url`, `rolling_window_hours`, and `publish_dir` control canonical URLs,
homepage freshness, and the production root. Deployment copies only generated
static files, removes only ordinary stale files recorded in its managed manifest,
preserves prior and legacy CSS/JavaScript paths for cached pages, preserves
unknown server files, and replaces `index.html` last so readers do not see a new
homepage before its referenced pages and assets are present.

The production Nginx virtual host is checked in at
`deploy/nginx/news-tldr.com`. Generated HTML pages receive a 10-minute freshness
lifetime (`Cache-Control: max-age=600` plus the corresponding `Expires` header),
allowing the site's Cloudflare HTML cache rule to serve them from the edge before
revalidating with the origin. CSS and JavaScript filenames include a 16-character
SHA-256 content fingerprint, change only when their contents change, and receive a
one-year immutable cache policy.

The optional anonymous read-history service uses the existing origin's PHP-FPM
runtime plus a dedicated SQLite database outside the document root. Install the
API code, isolated PHP-FPM pool, daily cleanup job, and updated Nginx route with:

```bash
sudo ./scripts/install-sync-origin.sh
```

The installer requires PHP-FPM and `pdo_sqlite`, initializes
`/var/lib/news-tldr-sync/sync.sqlite` as `www-data`, validates PHP-FPM and Nginx,
binds the dedicated socket to the configured Nginx worker identity, and reloads
both services. After it succeeds, set
`presentation.reader_sync_enabled` to `true` in `config/pipeline.json` and run
`present --verbose`; keeping the flag false prevents a sync control from being
published before its API exists. To update only the static Nginx configuration, install
and validate it before reloading Nginx:

```bash
sudo install -o root -g root -m 0644 deploy/nginx/news-tldr.com /etc/nginx/sites-available/news-tldr.com
sudo nginx -t
sudo systemctl reload nginx
```

The homepage ranks the All view by fresh global impact and re-ranks each category
by fresh category impact. Its global New/All control defaults to New and saves
the reader's choice in local storage across visits and category sections.
The category selector and Latest Briefing controls remain together in a sticky
toolbar while stories scroll beneath them. Changing categories returns the
reader to the top of the page, with reduced-motion preferences respected.
An independent Sources control defaults to Top, showing only stories with at
least two distinct sources; All restores every story in the rolling window.
That preference is also device-local, and the non-default All state is reflected
as `coverage=all` in the URL.
Stories are marked read after their title remains visible for one second, retain
a subtle read indicator, and are remembered for three days. The masthead reports
the live unread count for the current category/source view and decrements it as
stories become read without moving cards during the scan. New mode re-applies
that local history whenever the reader changes category or view; a header action
marks every currently visible story read. Reading history remains browser-local
unless the reader explicitly starts anonymous synchronization from the header
icon. Starting sync creates a private capability link; another browser that opens
the link unions its local read state with the group. The fragment token is removed
from the address bar immediately, including for same-page fragment navigation;
writes are debounced and sent silently without changing the current view. A new
page visit checks the shared revision before displaying stories and downloads
state only when that revision changed. Contiguous read prefixes are represented
by one immutable story-order watermark, leaving only newer out-of-order reads as
individual IDs. Tab changes and cross-tab writes do not fetch or rerender.
Disconnecting preserves the browser's local history.
Anyone possessing the private link can join the group, so the UI identifies it as
sensitive.

An editorial curation pass selects up to 12 distinct Top News stories and groups
the highest-ranked related story cards under specific topic headings. It favors
coherent groups of three or more and can broaden an overly narrow label to a
meaningful regional or subject desk. Unmatched cards—and topic groups reduced to
one visible card by the reader's filters—are collected under per-category
remainders such as More World News or More Science instead of one Everything Else
dump. On mobile, every section is collapsed to its heading by default;
the heading toggles that section and an Expand All/Collapse All action controls
the current set of sections. Top News remains visible within focused category
views when a curated story belongs to that category. Topic sections are ordered
by recent coverage breadth, normalized against the active source pool for each
category, with a capped boost for multiple story angles from one publisher and
an editorial-importance adjustment. The combined view uses quiet category-family
tints whose depth tracks source count; focused categories retain the neutral
white/tan card treatment.

Generated pages carry `noindex` metadata, and `robots.txt` blocks major search
index crawlers while leaving ordinary/social-preview access allowed. The home,
archive, and story pages publish complete Open Graph and X card metadata with a
same-origin 1200×630 branded poster; story shares retain their own headline and
description. Every page also references the checked-in multi-resolution
newspaper favicon at `site/assets/favicon.ico`. The homepage status line stays
compact by showing the live unread count and a client-calculated relative generation
time that refreshes every minute.

### Anonymous Read-History Sync

The same-origin API exposes only three mutation endpoints:

- `POST /api/sync/v1/groups` creates a group and imports the browser's current reads.
- `POST /api/sync/v1/merge` atomically unions local and server read state.
- `DELETE /api/sync/v1/group` deletes the shared group for every browser.

The 256-bit bearer token is returned only at creation, stored in browser local
storage, transported in the `Authorization` header, and persisted on the origin
only as a SHA-256 hash. Responses are `no-store`; requests require an allowed
same-origin `Origin` and JSON content type. The server stores only story IDs,
millisecond read timestamps, immutable first-publication order values for
compactable IDs, a read-prefix watermark, and a monotonic group revision.
Matching revisions return a small response without the complete state.

Default containment limits are 2,000 read IDs per group, 2,000 active groups,
100 group creations per UTC day, a 256 KiB request/state size, a 256 MiB SQLite
page ceiling, three PHP-FPM workers, and layered client/peer Nginx request limits.
Reads expire after three days and unused groups after 180 days. Daily cleanup is
installed from `deploy/cron/news-tldr-sync`; successful cleanup is silent and
failures use cron's normal error-mail path. The sync database should be excluded
from backups so expired reader history is not retained elsewhere.

Run cleanup manually with machine-readable stdout and optional progress on stderr:

```bash
sudo -u www-data env SYNC_DB_PATH=/var/lib/news-tldr-sync/sync.sqlite \
  /usr/bin/php /opt/news-tldr-sync/cleanup.php --verbose
```

### Operations and Monitoring

Validate configuration symmetry, SQLite integrity, article/event/story schemas,
LLM prompt provenance, citation references, the active index, and generated
static output:

```bash
./.venv/bin/python -m pipeline.cli validate-data --verbose
```

Check that every pipeline stage completed successfully within the configured
freshness limit, inspect the latest collection failures, validate artifacts,
and verify the public homepage and JSON API over HTTPS:

```bash
./.venv/bin/python -m pipeline.cli health --verbose
```

The latest machine-readable health result is written to
`data/state/health.json`. A failed check exits nonzero. `--no-network` and
`--no-validate` provide narrower diagnostic modes.

Summarize recorded model calls and token usage by stage, model, and prompt:

```bash
./.venv/bin/python -m pipeline.cli llm-usage --hours 24
```

The production machine runs `scripts/run-scheduled.sh` from the user's crontab
at minute 17 of every hour. The wrapper runs the complete pipeline, publishes
successful output, runs the health check, rotates its log at 10 MiB, and exits
nonzero on pipeline or health failure. Detailed output is stored in
`data/state/scheduled-pipeline.log`; cron's normal mail/error path provides the
alert signal. The reproducible crontab entry is in
`deploy/cron/news-tldr.cron`.

Verify the installed schedule:

```bash
crontab -l | grep news-tldr
```

### Configuration Catalog

- **Feeds**: Add a unique sanitized `source_id` to `config/feeds.json` with
  `source_name`, `feed_url`, optional `site_url`, `enabled`, a valid
  `default_category`, category/content hints, and fetch overrides. Scraper
  sources must use a module inside `pipeline.scrapers`.
- **Source policy**: Add the same `source_id` to `config/source-policy.json`
  with `bias_label`, `reliability`, `paywall`, and an explanatory note. Feed
  and policy ID sets must remain exactly symmetric; `validate-data` enforces it.
- **Categories**: Add a sanitized `id`, display `name`, `description`, and
  unique `sort_order` to `config/categories.json`. Add a concise `short_name`
  for the homepage navigation, then update the aggregation
  category-group mapping and prompt guidance if the new category changes event
  grouping behavior.

Remove the local generated SQLite state, staged articles, event files, published files, and fetch logs:
```bash
./.venv/bin/python -m pipeline.cli clean-data --yes
```

Run verification:
```bash
./.venv/bin/pip-audit -r requirements.txt
./.venv/bin/python -m pytest
```
