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

Run stage 3 editorial generation. Editorial extracts a passage-backed evidence ledger, drafts a summary, and independently
checks it against the quoted evidence through the ordered full-Flash fallback chain.
A rejected draft gets one repair attempt; unresolved failures retain the previous story
and checkpoint. Successful generation writes
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

The homepage opens with a finite briefing of up to 12 developments. Its cohort is
chosen before read-history filtering, so reading a story does not pull another
one into the briefing. Additional topic sections and category remainders sit
behind **Explore more coverage**. Top News opens on mobile; secondary sections
remain individually expandable. Smaller headlines and two complementary bullets
replace the repeated headline/dek/summary treatment on cards. New editorial
output generates those two bullets deliberately, including material uncertainty;
legacy stories use the first TL;DR bullet and their first uncertainty when present.

The paper palette and serif typography remain. The sticky toolbar provides
category navigation, New/All read history, All/2+ outlets coverage, and Mark read.
All outlets is the default for new browsers, allowing important single-outlet
reporting into the briefing. Existing saved coverage preferences are honored.
The optional `coverage=top` URL selects multiple publishers; `coverage=all` remains
accepted. The count explicitly reports unread items **in the briefing**. Sources
on each card links directly to the story's reporting. A public
[/methodology/](https://news-tldr.com/methodology/) page explains selection,
evidence checks, source counts, read behavior, and correction reporting.

Source policy entries have a canonical `publisher_id`. Multiple feeds from one
publisher count as one outlet. Known AP/Reuters wire provenance is identified
from explicit byline/origin text where available; unknown provenance is not
assumed independent. Outlet counts measure coverage, not corroboration.

A title at least 60% visible for **one second** still counts as read, intentionally
supporting headline skimming. Read markers persist for three days; cards stay in
place during a scan. Mark read applies to displayed cards, excluding collapsed
coverage. Meaningful new facts or corrections increment a story's revision and
can return in New with a short change summary. Rewording, extra citations and
regeneration alone do not increment it. The optional private-link sync protocol
also synchronizes these revision identities without changing its endpoints.

Each revision has an immutable publication-order value. Revision 1 retains the
legacy story ID and original creation order; later revisions use a deterministic
opaque ID and their first revision-publication time. This keeps existing read
watermarks compatible without letting an old read suppress a newly published
revision. Disconnecting sync preserves local history.

Generated pages remain static, cacheable, noindex, and protected by same-origin
CSP. Fingerprinted CSS/JavaScript retain their one-year cache policy; pages retain
the existing 10-minute policy. Story timestamps refresh in the browser alongside
the site's build timestamp. Social previews and the newspaper favicon remain.

### Editorial Quality Evaluation

The synthetic fixtures in `tests/fixtures/editorial-evaluation.json` cover
unsupported numeric claims, rumor attribution, research limitations, competing
claims, vendor performance claims, and event-boundary regressions. Generate a
private report for human review using the configured full-Flash model chain:

```bash
./.venv/bin/python scripts/evaluate-editorial.py --verbose
```

Reports default to ignored `data/evaluations/editorial.json`. Use `--output PATH`
to override or `--dry-run` to list fixtures without network calls. Automatic
validation is not a human quality score; the report leaves human judgments blank.
See [docs/editorial-evaluation.md](docs/editorial-evaluation.md) for the rubric.

Aggregation now checks incoming attachments to existing events, reserves merges
for the full-Flash deduplication review, and reviews up to
`aggregation.coherence_reviews_per_run` existing clusters per invocation (default
10). Coherence decisions are cached against exact article membership and prompt
version. High-confidence complete partitions can split contaminated clusters,
retaining the original event ID for its original anchor. Published headlines
also participate in duplicate discovery so stale event slugs cannot hide obvious
reader-facing duplicates. Historical repair is incremental; a presentation-only
build does not regenerate old editorial artifacts.

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
compactable story/revision IDs, a read-prefix watermark, and a monotonic group revision.
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
  with `publisher_id`, `bias_label`, `reliability`, `paywall`, and an explanatory note. Feed
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
