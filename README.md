# news-tldr.com

Source code for the RSS aggregator and summarizer at https://news-tldr.com.

## Overview

news-tldr.com is a filesystem-backed RSS aggregator that parses web feeds, extracts article content, groups related coverage into durable topics/events, and uses AI/LLMs to generate concise neutral TL;DR summaries. The published site is intended to be static and CDN-cacheable.

## Key Features

- **RSS/Atom Feed Parsing**: Periodic updates of subscribed sources.
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

*(Runtime setup will be added once the technology stack and dependencies are finalized.)*
