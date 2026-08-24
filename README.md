# news-tldr.com

Source code for the RSS aggregator and summarizer at https://news-tldr.com.

## Overview

news-tldr.com is a filesystem-backed RSS aggregator that parses web feeds, extracts article content, groups related coverage into durable topics/events, and uses AI/LLMs to generate concise neutral TL;DR summaries. The published site is fully static and served at [news-tldr.com](https://news-tldr.com/).

## Key Features

- **RSS/Atom Feed Parsing**: Periodic updates of subscribed sources.
- **Lead Image Capture**: Download of supported article images as sidecar files next to staged article JSON.
- **Filesystem Pipeline**: JSON artifacts and a SQLite state database connect collection, aggregation, editorial, and presentation stages without a server runtime.
- **AI-Powered Summaries**: Automatic generation of brief, sourced summaries across multiple articles covering the same event.
- **Static Presentation**: A dependency-free Python renderer builds the reader interface from editorial JSON and publishes it without a server runtime.

## Project Resources

For developers and agents working on the project, refer to these documents:

- **[AGENTS.md](file:///home/pmeenan/src/news-tldr.com/AGENTS.md)**: Rules of engagement, agent workspace instructions, and current task state.
- **[docs/design.md](file:///home/pmeenan/src/news-tldr.com/docs/design.md)**: System architecture and data flow.
- **[docs/plan.md](file:///home/pmeenan/src/news-tldr.com/docs/plan.md)**: Development milestones and backlog.

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

Article digestion and Stage 2 story aggregation use the Gemini Developer API.
Create a local `.env` file with an AI Studio API key and the two model tiers:

```bash
GEMINI_API_KEY=your-ai-studio-api-key
GEMINI_BULK_MODEL=gemini-3.5-flash-lite
GEMINI_REVIEW_MODEL=gemini-3.7-flash
```

`GEMINI_MODEL` remains a supported fallback for the bulk tier. The `.env` file
is ignored by git and must not be committed. Bulk digestion, grouping, scoring,
and candidate discovery use 3.5 Flash-Lite with minimal thinking. Borderline
article-filter decisions and final event-merge decisions use 3.7 Flash with low
thinking. All calls use Gemini structured output and validated responses.

### Pipeline Commands

Initialize or migrate the SQLite state database:
```bash
./.venv/bin/python -m pipeline.cli init-db
```

Run stage 1 data collection:
```bash
./.venv/bin/python -m pipeline.cli collect
```

Run the complete pipeline in order (`maintenance`, `collect`, `digest`,
`aggregate`, `editorial`, then `presentation`). Presentation builds `dist/` and,
with the checked-in production configuration, publishes it to
`/var/www/news-tldr.com/`. The command holds the shared pipeline lock for the
full duration so scheduled invocations do not interleave with each other or
with individual stage commands:
```bash
./.venv/bin/python -m pipeline.cli run --verbose
```

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

Run stage 3 editorial generation. Editorial uses one Gemini 3.7 Flash call per
changed active/stale event, validates source citations, writes
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
static files, removes only stale files recorded in its managed manifest, preserves
unknown server files, and replaces `index.html` last so readers do not see a new
homepage before its referenced pages and assets are present.

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
  unique `sort_order` to `config/categories.json`, then update the aggregation
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
