# news-tldr.com

Source code for the RSS aggregator and summarizer at https://news-tldr.com.

## Overview

news-tldr.com is a filesystem-backed RSS aggregator that parses web feeds, extracts article content, groups related coverage into durable topics/events, and uses AI/LLMs to generate concise neutral TL;DR summaries. The published site is intended to be static and CDN-cacheable.

## Key Features

- **RSS/Atom Feed Parsing**: Periodic updates of subscribed sources.
- **Lead Image Capture**: Download of supported article images as sidecar files next to staged article JSON.
- **Filesystem Pipeline**: JSON artifacts and a SQLite state database connect collection, aggregation, editorial, and presentation stages without a server runtime.
- **AI-Powered Summaries**: Automatic generation of brief, sourced summaries across multiple articles covering the same event.
- **Static Presentation**: A clean reader interface generated from JSON and deployable without a server runtime.

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

This installs the feed parser, HTTP client with HTTP/2 support, article extraction, and scraper dependencies used by the collection pipeline.

Any scripts or pipeline executions should run using the Python interpreter inside `.venv` (e.g., `./.venv/bin/python`).

### Pipeline Commands

Initialize or migrate the SQLite state database:
```bash
./.venv/bin/python -m pipeline.cli init-db
```

Run stage 1 data collection:
```bash
./.venv/bin/python -m pipeline.cli collect
```

Print incremental collection progress to stderr while keeping final stats on stdout:
```bash
./.venv/bin/python -m pipeline.cli collect --verbose
```

Remove the local generated SQLite state, staged articles, and fetch logs:
```bash
./.venv/bin/python -m pipeline.cli clean-data --yes
```

Run verification:
```bash
./.venv/bin/pip-audit -r requirements.txt
./.venv/bin/python -m pytest
```
