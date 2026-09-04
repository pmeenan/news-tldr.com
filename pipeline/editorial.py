from __future__ import annotations

import json
import math
import re
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from pipeline.config import load_pipeline_config, load_source_policy
from pipeline.evidence import (
    EVIDENCE_VERSION,
    REVIEW_VERSION,
    collect_evidence,
    validate_claim_links,
    verify_story,
)
from pipeline.llm import GeminiEmptyResponseError, GeminiResult, create_gemini_client
from pipeline.lock import PipelineLock
from pipeline.paths import ACTIVE_STORIES_PATH, LOCK_PATH, PROJECT_ROOT, STORY_DIR
from pipeline.sources import publisher_id, reporting_origin
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z

EDITORIAL_PROMPT_VERSION = "editorial-v3"
EDITORIAL_FRAMING_PROMPT_VERSION = "editorial-framing-v2"
EDITORIAL_COMPACT_PROMPT_VERSION = "editorial-v3-compact"
EDITORIAL_FRAMING_COMPACT_PROMPT_VERSION = "editorial-framing-v2-compact"
HOMEPAGE_CURATION_PROMPT_VERSION = "homepage-curation-v5"
DEFAULT_ARTICLE_CHAR_LIMIT = 12_000
DEFAULT_EVENT_CHAR_LIMIT = 60_000
DEFAULT_CURATION_TOP_STORIES = 12
DEFAULT_CURATION_MAX_SECTIONS = 180
DEFAULT_CURATION_MAX_SECTIONS_PER_CATEGORY = 12
DEFAULT_CURATION_TOP_CANDIDATES = 150
DEFAULT_CURATION_STORIES_PER_CATEGORY = 100
CURATION_COVERAGE_WINDOW_HOURS = 24
CURATION_EDITORIAL_WEIGHT = 10.0
POLITICAL_CATEGORIES = frozenset({"politics", "us", "world"})
LEFT_BIAS_LABELS = frozenset({"left", "center-left"})
RIGHT_BIAS_LABELS = frozenset({"right", "center-right"})
RELIABILITY_SCORES = {"high": 1.0, "medium": 0.6, "low": 0.25}


class JsonGenerator(Protocol):
    model: str

    def generate_json(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_schema: dict[str, Any],
        max_output_tokens: int | None = None,
        thinking_level: str | None = None,
    ) -> GeminiResult: ...


@dataclass(frozen=True)
class EditorialArticle:
    article_id: str
    source_id: str
    source_name: str
    headline: str
    url: str
    published_at: str | None
    content: str
    digest_summary: str | None
    digest_key_facts: tuple[str, ...]
    bias_label: str
    reliability: str
    publisher_id: str = ""
    reporting_origin: str | None = None


@dataclass(frozen=True)
class EditorialEvent:
    event_id: str
    title: str
    category: str
    thread: str | None
    status: str
    created_at: str
    updated_at: str
    newsworthiness: dict[str, Any]
    articles: tuple[EditorialArticle, ...]


def editorial_once(
    *,
    limit: int | None = None,
    concurrency: int | None = None,
    force: bool = False,
    event_ids: Sequence[str] | None = None,
    client: JsonGenerator | None = None,
    progress: Callable[[str], None] | None = None,
    acquire_lock: bool = True,
    story_dir: Path = STORY_DIR,
    active_stories_path: Path = ACTIVE_STORIES_PATH,
) -> dict[str, Any]:
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    config = load_pipeline_config()
    selected_concurrency = int(concurrency or config.editorial.get("concurrency", 8))
    if selected_concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    article_char_limit = int(
        config.editorial.get("article_char_limit", DEFAULT_ARTICLE_CHAR_LIMIT)
    )
    event_char_limit = int(config.editorial.get("event_char_limit", DEFAULT_EVENT_CHAR_LIMIT))
    if article_char_limit < 1 or event_char_limit < 1:
        raise ValueError("editorial content limits must be positive")

    lock_timeout = timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
    run_id = f"editorial-{uuid.uuid4().hex}"
    owns_client = client is None
    generator = client or create_gemini_client("review")
    state = StateDB()
    stats: dict[str, Any] = {"run_id": run_id}
    try:
        lock_context = PipelineLock(LOCK_PATH, lock_timeout, run_id=run_id) if acquire_lock else nullcontext()
        with lock_context:
            state.start_run(run_id, "editorial")
            status = "success"
            try:
                stats.update(
                    generate_editorial_stories(
                        state=state,
                        run_id=run_id,
                        client=generator,
                        limit=limit,
                        concurrency=selected_concurrency,
                        force=force,
                        event_ids=event_ids,
                        article_char_limit=article_char_limit,
                        event_char_limit=event_char_limit,
                        progress=progress,
                        story_dir=story_dir,
                    )
                )
                index_stats = write_active_stories_index(
                    state=state,
                    story_dir=story_dir,
                    output_path=active_stories_path,
                    curation_client=generator,
                    run_id=run_id,
                    progress=progress,
                )
                stats.update(index_stats)
                if stats.get("failed"):
                    status = "partial_failure" if stats.get("completed") else "failed"
            except Exception:
                status = "failed"
                raise
            finally:
                state.finish_run(run_id, status, stats)
        return stats
    finally:
        state.close()
        if owns_client:
            close = getattr(generator, "close", None)
            if callable(close):
                close()


def generate_editorial_stories(
    *,
    state: StateDB,
    run_id: str,
    client: JsonGenerator,
    limit: int | None = None,
    concurrency: int = 8,
    force: bool = False,
    event_ids: Sequence[str] | None = None,
    article_char_limit: int = DEFAULT_ARTICLE_CHAR_LIMIT,
    event_char_limit: int = DEFAULT_EVENT_CHAR_LIMIT,
    progress: Callable[[str], None] | None = None,
    story_dir: Path = STORY_DIR,
) -> dict[str, Any]:
    source_policy = load_source_policy()
    rows = editorial_candidate_rows(
        state=state,
        force=force,
        event_ids=event_ids,
        limit=limit,
    )
    stats: dict[str, Any] = {
        "candidates": len(rows),
        "completed": 0,
        "failed": 0,
        "rejected_event_ids": [],
        "forced": force,
        "model": client.model,
        "prompt_versions": [
            EDITORIAL_PROMPT_VERSION,
            EDITORIAL_FRAMING_PROMPT_VERSION,
            EDITORIAL_COMPACT_PROMPT_VERSION,
            EDITORIAL_FRAMING_COMPACT_PROMPT_VERSION,
        ],
        "usage": {"promptTokenCount": 0, "candidatesTokenCount": 0},
    }
    if progress:
        progress(
            f"editorial: {len(rows)} event(s) need stories, concurrency={concurrency}, model={client.model}"
        )
    if not rows:
        return stats

    events: list[EditorialEvent] = []
    for row in rows:
        try:
            events.append(
                _load_editorial_event(
                    state,
                    row,
                    source_policy=source_policy,
                    article_char_limit=article_char_limit,
                    event_char_limit=event_char_limit,
                )
            )
        except Exception as exc:
            stats["failed"] += 1
            state.record_error(run_id, "editorial", "event", row["event_id"], None, exc)
            if progress:
                progress(f"editorial: failed loading {row['event_id']}: {exc}")

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_event = {
            executor.submit(
                generate_story, event, client=client,
                previous=_read_json(story_dir / f"{event.event_id}.json"),
            ): event for event in events
        }
        processed = 0
        for future in as_completed(future_to_event):
            event = future_to_event[future]
            processed += 1
            try:
                generated = future.result()
                generated_at = isoformat_z()
                path = story_dir / f"{event.event_id}.json"
                story = build_story_payload(
                    event,
                    generated,
                    generated_at=generated_at,
                    existing_story=_read_json(path),
                )
                atomic_write_json(path, story)
                state.mark_event_editorial_completed(event.event_id, generated_at)
                usage = generated["usage"]
                _record_editorial_usage(state, run_id, generated.get("usage_records") or [{
                    "model": generated["model"], "prompt_version": generated["prompt_version"], "usage": usage,
                }])
                for key in stats["usage"]:
                    stats["usage"][key] += int(usage.get(key) or 0)
                stats["completed"] += 1
                if progress:
                    progress(
                        f"editorial: {processed}/{len(events)} completed {event.event_id}"
                    )
            except Exception as exc:
                stats["failed"] += 1
                if getattr(exc, "editorial_validation_rejected", False):
                    stats["rejected_event_ids"].append(event.event_id)
                _record_editorial_usage(state, run_id, getattr(exc, "editorial_usage_records", []))
                state.record_error(run_id, "editorial", "event", event.event_id, None, exc)
                if progress:
                    progress(
                        f"editorial: {processed}/{len(events)} failed {event.event_id}: {exc}"
                    )
    return stats


def _record_editorial_usage(state: StateDB, run_id: str, records: list[dict[str, Any]]) -> None:
    for record in records:
        usage = record["usage"]
        state.record_llm_usage(
            run_id=run_id, stage="editorial", model=record["model"], prompt_version=record["prompt_version"],
            input_tokens=usage.get("promptTokenCount"), output_tokens=usage.get("candidatesTokenCount"),
        )


def editorial_candidate_rows(
    *,
    state: StateDB,
    force: bool = False,
    event_ids: Sequence[str] | None = None,
    limit: int | None = None,
) -> list[Any]:
    where = ["status IN ('active', 'stale')"]
    params: list[Any] = []
    if not force:
        where.append("(last_editorial_at IS NULL OR updated_at > last_editorial_at)")
    if event_ids:
        clean_ids = sorted({event_id for event_id in event_ids if event_id})
        if not clean_ids:
            return []
        where.append(f"event_id IN ({','.join('?' for _ in clean_ids)})")
        params.extend(clean_ids)
    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT ?"
        params.append(limit)
    return state.conn.execute(
        f"""
        SELECT event_id, title, category, thread, status, created_at, updated_at,
               newsworthiness_global, newsworthiness_category, newsworthiness_json,
               last_editorial_at
        FROM events
        WHERE {' AND '.join(where)}
        ORDER BY COALESCE(newsworthiness_global, 0) DESC, updated_at DESC, event_id
        {limit_clause}
        """,
        params,
    ).fetchall()


def _load_editorial_event(
    state: StateDB,
    row: Any,
    *,
    source_policy: dict[str, dict[str, Any]],
    article_char_limit: int,
    event_char_limit: int,
) -> EditorialEvent:
    article_rows = state.conn.execute(
        """
        SELECT article_id, source_id, source_name, headline, url, published_at, article_path
        FROM articles
        WHERE event_id = ?
          AND is_filtered = 0
        ORDER BY published_at DESC, article_id
        """,
        (row["event_id"],),
    ).fetchall()
    if not article_rows:
        raise ValueError("event has no unfiltered source articles")

    # Give relevant, diverse reports useful passages instead of tiny slices of every source.
    title_words = set(re.findall(r"[a-z]{4,}", str(row["title"]).lower()))
    ranked_rows = sorted(article_rows, key=lambda r: len(title_words & set(
        re.findall(r"[a-z]{4,}", r["headline"].lower()))), reverse=True)
    selected_rows = []
    seen_publishers: set[str] = set()
    for candidate in ranked_rows:
        identity = publisher_id(dict(candidate))
        if identity not in seen_publishers:
            selected_rows.append(candidate)
            seen_publishers.add(identity)
    selected_ids = {r["article_id"] for r in selected_rows}
    selected_rows.extend(r for r in ranked_rows if r["article_id"] not in selected_ids)
    selected_rows = selected_rows[:max(1, min(8, event_char_limit // 3000))]
    per_article_limit = min(article_char_limit, max(1, event_char_limit // len(selected_rows)))
    articles: list[EditorialArticle] = []
    for article_row in selected_rows:
        path = PROJECT_ROOT / article_row["article_path"]
        data = _read_json(path)
        if data is None:
            raise ValueError(f"article artifact missing or invalid: {article_row['article_id']}")
        digest = data.get("llm_digest") if isinstance(data.get("llm_digest"), dict) else {}
        raw_content = (
            data.get("content_text")
            or data.get("content_excerpt")
            or digest.get("summary")
            or data.get("summary")
        )
        content = _bounded_text(str(raw_content or ""), per_article_limit)
        if not content:
            raise ValueError(f"article has no editorial content: {article_row['article_id']}")
        policy = source_policy.get(article_row["source_id"], {})
        facts = digest.get("key_facts") if isinstance(digest.get("key_facts"), list) else []
        articles.append(
            EditorialArticle(
                article_id=article_row["article_id"],
                source_id=article_row["source_id"],
                source_name=article_row["source_name"],
                headline=article_row["headline"],
                url=article_row["url"],
                published_at=article_row["published_at"],
                content=content,
                digest_summary=digest.get("summary") if isinstance(digest.get("summary"), str) else None,
                digest_key_facts=tuple(str(fact) for fact in facts if str(fact).strip()),
                bias_label=str(policy.get("bias_label") or "unknown"),
                reliability=str(policy.get("reliability") or "unknown"),
                publisher_id=publisher_id(dict(article_row)),
                reporting_origin=reporting_origin(str(raw_content or ""), publisher_id(dict(article_row))),
            )
        )
    try:
        newsworthiness = json.loads(row["newsworthiness_json"] or "{}")
    except json.JSONDecodeError:
        newsworthiness = {}
    if not isinstance(newsworthiness, dict):
        newsworthiness = {}
    if "global" not in newsworthiness:
        newsworthiness["global"] = row["newsworthiness_global"]
    if "category" not in newsworthiness:
        newsworthiness["category"] = row["newsworthiness_category"]
    return EditorialEvent(
        event_id=row["event_id"],
        title=row["title"],
        category=row["category"],
        thread=row["thread"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        newsworthiness=newsworthiness,
        articles=tuple(articles),
    )


def generate_story(
    event: EditorialEvent, *, client: JsonGenerator, previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    def record(result: GeminiResult, version: str) -> None:
        records.append({"model": result.model, "prompt_version": version, "usage": result.usage})
    try:
        try:
            ledger, evidence_result = collect_evidence(event, client)
        except ValueError as exc:
            if pending := getattr(exc, "editorial_unrecorded_result", None):
                record(*pending)
            ledger, evidence_result = collect_evidence(event, client, feedback=str(exc))
        record(evidence_result, EVIDENCE_VERSION)
        supported_ids = {e["article_id"] for c in ledger for e in c["evidence"]}
        selected_event = replace(event, articles=tuple(a for a in event.articles if a.article_id in supported_ids))
        framing_eligible = _political_framing_eligible(selected_event)
        compact_retry = False
        feedback = ""
        for attempt in range(2):
            try:
                result = _request_editorial_story(
                    selected_event, client=client, framing_eligible=framing_eligible,
                    ledger=ledger, feedback=feedback,
                )
            except GeminiEmptyResponseError:
                selected_event = _compact_editorial_event(selected_event)
                compact_retry = True
                result = _request_editorial_story(
                    selected_event, client=client, framing_eligible=framing_eligible,
                    compact=True, ledger=ledger, feedback=feedback,
                )
            prompt_version = (
                EDITORIAL_FRAMING_COMPACT_PROMPT_VERSION if framing_eligible else EDITORIAL_COMPACT_PROMPT_VERSION
            ) if compact_retry else (EDITORIAL_FRAMING_PROMPT_VERSION if framing_eligible else EDITORIAL_PROMPT_VERSION)
            record(result, prompt_version)
            try:
                validate_claim_links(result.payload, ledger)
                supported_ids = {e["article_id"] for c in ledger for e in c["evidence"]}
                validation_event = replace(selected_event, articles=tuple(
                    a for a in selected_event.articles if a.article_id in supported_ids))
                validated = validate_editorial_response(result.payload, validation_event)
                review, review_result = verify_story(validated, ledger, previous, client)
                record(review_result, REVIEW_VERSION)
            except ValueError as exc:
                if pending := getattr(exc, "editorial_unrecorded_result", None):
                    record(*pending)
                    del exc.editorial_unrecorded_result
                feedback = str(exc)
                if attempt == 0:
                    continue
                raise
            if review["approved"]:
                break
            feedback = str(review.get("reason") or "Draft is not supported by the evidence")
        else:
            raise ValueError(f"editorial evidence verification failed: {feedback}")
        return {
            "payload": validated, "model": result.model, "prompt_version": prompt_version,
            "usage": {key: sum(int(r["usage"].get(key) or 0) for r in records)
                      for key in ("promptTokenCount", "candidatesTokenCount")},
            "usage_records": records, "evidence": ledger, "review": review,
        }
    except Exception as exc:
        if pending := getattr(exc, "editorial_unrecorded_result", None):
            record(*pending)
        exc.editorial_usage_records = records
        exc.editorial_validation_rejected = isinstance(exc, ValueError)
        raise


def _request_editorial_story(
    event: EditorialEvent,
    *,
    client: JsonGenerator,
    framing_eligible: bool,
    compact: bool = False,
    ledger: list[dict[str, Any]] | None = None,
    feedback: str = "",
) -> GeminiResult:
    system_instruction = (
        "You are the neutral editorial desk for a concise news product. Synthesize only the "
        "provided reporting. Preserve uncertainty, attribute disputed claims, and never invent facts."
    )
    if compact:
        system_instruction += (
            " Some reporting may discuss abuse or other sensitive events; summarize it clinically "
            "without adding graphic detail."
        )
    return client.generate_json(
        system_instruction=system_instruction,
        prompt=_build_editorial_prompt(event) + "\nUse ONLY claims supported by this verified ledger. "
        "Return headline_claim_ids, dek_claim_ids, tldr_claim_ids and briefing_claim_ids linking every "
        "summary assertion to its claim IDs (c1, c2, etc.). In key_facts, uncertainties and framing, "
        "source_article_ids must instead contain the exact article_id strings from supporting evidence, "
        "never claim IDs.\n" + json.dumps(ledger or [], ensure_ascii=False)
        + ("\nCorrect this verifier finding: " + feedback if feedback else ""),
        response_schema=_editorial_response_schema(
            framing_decision=framing_eligible, article_ids=[a.article_id for a in event.articles]),
        max_output_tokens=8192,
        thinking_level="low",
    )


def _compact_editorial_event(event: EditorialEvent) -> EditorialEvent:
    articles = []
    for article in event.articles:
        compact_parts = [article.digest_summary or ""]
        compact_parts.extend(article.digest_key_facts)
        compact_content = "\n".join(part.strip() for part in compact_parts if part.strip())
        if not compact_content:
            compact_content = _bounded_text(article.content, 1_500)
        articles.append(replace(article, content=compact_content))
    return replace(event, articles=tuple(articles))


def _build_editorial_prompt(event: EditorialEvent) -> str:
    framing_allowed = _political_framing_eligible(event)
    article_payloads = [
        {
            "article_id": article.article_id,
            "source_id": article.source_id,
            "source_name": article.source_name,
            "bias_label": article.bias_label,
            "reliability": article.reliability,
            "headline": article.headline,
            "published_at": article.published_at,
            "digest_summary": article.digest_summary,
            "digest_key_facts": list(article.digest_key_facts),
            "article_text": article.content,
        }
        for article in event.articles
    ]
    framing_instruction = (
        "Political framing is eligible because both left/center-left and right/center-right sources are present. "
        "You must explicitly set political_framing_present. Set it true when the two sides meaningfully diverge "
        "in praise versus criticism, causal interpretation, legitimacy, consequences emphasized, or proposed "
        "response; different wording alone is not enough. When true, include political_framing and cite only "
        "left-labeled article IDs under left_perspective and only right-labeled IDs under right_perspective. "
        "When false, omit political_framing. Compare actual article assertions, not presumed outlet ideology. "
        "Never invent symmetry or treat unequal evidence as equally supported."
        if framing_allowed
        else "Omit political_framing; this event does not have the source mix required for a balanced comparison."
    )
    event_payload = {
        "event_id": event.event_id,
        "title": event.title,
        "category": event.category,
        "thread": event.thread,
        "created_at": event.created_at,
        "updated_at": event.updated_at,
        "newsworthiness": event.newsworthiness,
    }
    return (
        "Create one concise story from this event and its source articles.\n"
        "Requirements:\n"
        "- Headline and dek must be neutral, specific, and supported. Aim for 8-14 headline words. "
        "Avoid clickbait and outlet framing. For a rumor, leak or unconfirmed claim, the HEADLINE "
        "must explicitly retain attribution (for example reportedly, report says, or officials allege). "
        "Write the headline in sentence case, preserving normal capitalization for proper names and acronyms.\n"
        "- tldr must contain 2-4 standalone bullets explaining what happened, who is affected, what changed, "
        "and the most important next step or unknown.\n"
        "- briefing must contain exactly two concise standalone bullets (aim for 20-35 words each): "
        "what happened, then the consequence or most material qualification/unknown. Do not repeat the headline. "
        "Include essential uncertainty even when it occurs late in the full TLDR.\n"
        "- key_facts must contain 2-8 factual claims. Every claim must cite one or more provided article_id values.\n"
        "- uncertainties should include disputed, preliminary, unverified, or genuinely unknown points; omit it "
        "when nothing material remains uncertain. Every uncertainty must cite its basis.\n"
        "- Attribute allegations, estimates, opinions, and disputed claims in the text itself. Agreement between "
        "outlets does not convert an unsupported claim into fact.\n"
        "- Use source prose only as factual input; paraphrase and do not copy distinctive wording.\n"
        "- editorial_score is 0.0-1.0 and reflects the story's editorial prominence after considering public "
        "impact, timeliness, breadth, and whether it materially advances an ongoing story.\n"
        "- importance_signals must be short snake_case audit labels, not prose.\n"
        f"- {framing_instruction}\n"
        "Return JSON only. Omit political_framing when it is not warranted.\n\n"
        f"Event:\n{json.dumps(event_payload, ensure_ascii=False, separators=(',', ':'))}\n\n"
        f"Articles:\n{json.dumps(article_payloads, ensure_ascii=False, separators=(',', ':'))}"
    )


def _editorial_response_schema(
    *, framing_decision: bool = False, article_ids: list[str] | None = None,
) -> dict[str, Any]:
    article_id_schema = {"type": "STRING", **({"enum": article_ids} if article_ids else {})}
    cited_item = {
        "type": "OBJECT",
        "properties": {
            "text": {"type": "STRING"},
            "source_article_ids": {"type": "ARRAY", "items": article_id_schema},
        },
        "required": ["text", "source_article_ids"],
    }
    perspective = {
        "type": "OBJECT",
        "properties": {
            "summary": {"type": "STRING"},
            "source_article_ids": {"type": "ARRAY", "items": article_id_schema},
        },
        "required": ["summary", "source_article_ids"],
    }
    schema = {
        "type": "OBJECT",
        "properties": {
            "headline": {"type": "STRING"},
            "dek": {"type": "STRING"},
            "tldr": {"type": "ARRAY", "items": {"type": "STRING"}},
            "briefing": {"type": "ARRAY", "items": {"type": "STRING"}},
            "headline_claim_ids": {"type": "ARRAY", "items": {"type": "STRING"}},
            "dek_claim_ids": {"type": "ARRAY", "items": {"type": "STRING"}},
            "tldr_claim_ids": {"type": "ARRAY", "items": {"type": "ARRAY", "items": {"type": "STRING"}}},
            "briefing_claim_ids": {"type": "ARRAY", "items": {"type": "ARRAY", "items": {"type": "STRING"}}},
            "key_facts": {"type": "ARRAY", "items": cited_item},
            "uncertainties": {"type": "ARRAY", "items": cited_item},
            "political_framing": {
                "type": "OBJECT",
                "properties": {
                    "summary": {"type": "STRING"},
                    "left_perspective": perspective,
                    "right_perspective": perspective,
                },
                "required": ["summary", "left_perspective", "right_perspective"],
            },
            "editorial_score": {"type": "NUMBER"},
            "importance_signals": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": [
            "headline",
            "dek",
            "tldr", "briefing", "headline_claim_ids", "dek_claim_ids", "tldr_claim_ids", "briefing_claim_ids",
            "key_facts",
            "uncertainties",
            "editorial_score",
            "importance_signals",
        ],
    }
    if framing_decision:
        schema["properties"]["political_framing_present"] = {"type": "BOOLEAN"}
        schema["required"].append("political_framing_present")
    return schema


def validate_editorial_response(payload: Any, event: EditorialEvent) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("editorial response must be an object")
    headline = _required_text(payload.get("headline"), "headline", 180)
    dek = _required_text(payload.get("dek"), "dek", 320)
    tldr = _text_list(payload.get("tldr"), "tldr", minimum=2, maximum=4, max_chars=500)
    valid_ids = {article.article_id for article in event.articles}
    key_facts = _cited_items(
        payload.get("key_facts"), "key_facts", valid_ids=valid_ids, minimum=1, maximum=8
    )
    uncertainties = _cited_items(
        payload.get("uncertainties"), "uncertainties", valid_ids=valid_ids, minimum=0, maximum=8
    )
    try:
        editorial_score = float(payload.get("editorial_score"))
    except (TypeError, ValueError) as exc:
        raise ValueError("editorial_score must be numeric") from exc
    if not math.isfinite(editorial_score):
        raise ValueError("editorial_score must be finite")
    editorial_score = max(0.0, min(1.0, editorial_score))
    importance_signals = _signal_list(payload.get("importance_signals"))

    if _political_framing_eligible(event):
        framing_present = payload.get("political_framing_present")
        if not isinstance(framing_present, bool):
            raise ValueError("political_framing_present must be a boolean")
        framing = (
            _validate_political_framing(payload.get("political_framing"), event)
            if framing_present
            else None
        )
        if framing_present and framing is None:
            raise ValueError("political_framing is required when political_framing_present is true")
    else:
        framing = None
    return {
        "headline": headline,
        "dek": dek,
        "tldr": tldr,
        "briefing": _text_list(payload.get("briefing", tldr[:2]), "briefing",
                               minimum=2, maximum=2, max_chars=350),
        "claim_links": {key: payload.get(key, []) for key in
                        ("headline_claim_ids", "dek_claim_ids", "tldr_claim_ids", "briefing_claim_ids")},
        "key_facts": key_facts,
        "uncertainties": uncertainties,
        "political_framing": framing,
        "editorial_score": editorial_score,
        "importance_signals": importance_signals,
    }


def _validate_political_framing(value: Any, event: EditorialEvent) -> dict[str, Any] | None:
    if not isinstance(value, dict) or event.category not in POLITICAL_CATEGORIES:
        return None
    left_ids = {a.article_id for a in event.articles if a.bias_label in LEFT_BIAS_LABELS}
    right_ids = {a.article_id for a in event.articles if a.bias_label in RIGHT_BIAS_LABELS}
    if not left_ids or not right_ids:
        return None
    left = _perspective(value.get("left_perspective"), "left_perspective", left_ids)
    right = _perspective(value.get("right_perspective"), "right_perspective", right_ids)
    return {
        "summary": _required_text(value.get("summary"), "political_framing.summary", 500),
        "left_perspective": left,
        "right_perspective": right,
    }


def _perspective(value: Any, field: str, allowed_ids: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    citations = _citation_ids(value.get("source_article_ids"), allowed_ids, field)
    return {
        "summary": _required_text(value.get("summary"), f"{field}.summary", 600),
        "source_article_ids": citations,
    }


def _cited_items(
    value: Any,
    field: str,
    *,
    valid_ids: set[str],
    minimum: int,
    maximum: int,
) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    if not minimum <= len(value) <= maximum:
        raise ValueError(f"{field} must contain {minimum}-{maximum} items")
    result = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{field}[{index}] must be an object")
        result.append(
            {
                "text": _required_text(item.get("text"), f"{field}[{index}].text", 700),
                "source_article_ids": _citation_ids(
                    item.get("source_article_ids"), valid_ids, f"{field}[{index}]"
                ),
            }
        )
    return result


def _citation_ids(value: Any, allowed_ids: set[str], field: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field}.source_article_ids must be a list")
    if any(not isinstance(item, str) or item not in allowed_ids for item in value):
        raise ValueError(f"{field} must cite only valid source article IDs")
    citations = sorted(set(value))
    if not citations:
        raise ValueError(f"{field} must cite at least one valid source article")
    return citations


def _required_text(value: Any, field: str, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        raise ValueError(f"{field} must not be empty")
    if len(text) > max_chars:
        raise ValueError(f"{field} exceeds {max_chars} characters; shorten without losing qualifications")
    return text


def _text_list(value: Any, field: str, *, minimum: int, maximum: int, max_chars: int) -> list[str]:
    if not isinstance(value, list) or not minimum <= len(value) <= maximum:
        raise ValueError(f"{field} must contain {minimum}-{maximum} items")
    return [_required_text(item, f"{field} item", max_chars) for item in value]


def _signal_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        raise ValueError("importance_signals must be a list")
    signals = []
    for item in value:
        signal = re.sub(r"[^a-z0-9]+", "_", str(item).strip().lower()).strip("_")[:64]
        if signal:
            signals.append(signal)
    return sorted(set(signals))[:12]


def build_story_payload(
    event: EditorialEvent,
    generated: dict[str, Any],
    *,
    generated_at: str,
    existing_story: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = generated["payload"]
    importance = _importance(event, payload, now=_parse_datetime(generated_at))
    used_ids = {e["article_id"] for c in generated.get("evidence", []) for e in c["evidence"]}
    sources = [
        {
            "article_id": article.article_id,
            "source_name": article.source_name,
            "source_id": article.source_id,
            "publisher_id": article.publisher_id or publisher_id(article.__dict__),
            "reporting_origin": article.reporting_origin,
            "headline": article.headline,
            "url": article.url,
        }
        for article in event.articles if not used_ids or article.article_id in used_ids
    ]
    previous = existing_story or {}
    claim_sources = {claim["claim_id"]: sorted({e["article_id"] for e in claim["evidence"]})
                     for claim in generated.get("evidence", [])}
    material_update = bool(previous and generated.get("review", {}).get("material_update"))
    revision = int(previous.get("revision") or 1) + int(material_update)
    revision_at = (generated_at if material_update else
                   previous.get("revision_at") or previous.get("created_at") or generated_at)
    return {
        "revision": revision,
        "revision_at": revision_at,
        "change_summary": generated.get("review", {}).get("change_summary", "") if material_update
        else previous.get("change_summary", ""),
        "_evidence": generated.get("evidence", []),
        "evidence_verification": {
            "version": REVIEW_VERSION, "approved": bool(generated.get("review", {}).get("approved")),
        },
        "claim_links": payload.get("claim_links", {}),
        "claim_sources": claim_sources,
        "briefing": payload.get("briefing", payload["tldr"][:2]),
        "story_id": event.event_id,
        "event_id": event.event_id,
        "category": event.category,
        "thread": event.thread,
        "headline": payload["headline"],
        "dek": payload["dek"],
        "tldr": payload["tldr"],
        "key_facts": payload["key_facts"],
        "uncertainties": payload["uncertainties"],
        "political_framing": payload["political_framing"],
        "sources": sources,
        "importance": importance,
        "created_at": (existing_story or {}).get("created_at") or generated_at,
        "updated_at": generated_at,
        "llm_metadata": {
            "model": generated["model"],
            "prompt_version": generated.get("prompt_version", EDITORIAL_PROMPT_VERSION),
            "generated_at": generated_at,
            "event_updated_at": event.updated_at,
        },
    }


def _importance(event: EditorialEvent, payload: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    global_score = _safe_score(event.newsworthiness.get("global"))
    category_score = _safe_score(event.newsworthiness.get("category"))
    editorial_score = _safe_score(payload.get("editorial_score"))
    source_count = len({article.publisher_id or publisher_id(article.__dict__) for article in event.articles})
    source_score = min(1.0, 0.2 + 0.2 * max(0, source_count - 1))
    reliability_score = sum(
        RELIABILITY_SCORES.get(article.reliability, 0.4) for article in event.articles
    ) / len(event.articles)
    age_hours = max(0.0, (now - _parse_datetime(event.updated_at)).total_seconds() / 3600)
    freshness_score = 1.0 if age_hours <= 6 else 0.8 if age_hours <= 24 else 0.5 if age_hours <= 48 else 0.2
    score = (
        global_score * 0.50
        + category_score * 0.15
        + editorial_score * 0.15
        + freshness_score * 0.10
        + reliability_score * 0.05
        + source_score * 0.05
    )
    signals = set(payload.get("importance_signals") or [])
    rationale = event.newsworthiness.get("rationale_codes")
    if isinstance(rationale, list):
        signals.update(_signal_list(rationale))
    if source_count > 1:
        signals.add("multiple_sources")
    if freshness_score >= 0.8:
        signals.add("fresh")
    if reliability_score >= 0.8:
        signals.add("high_reliability")
    return {
        "score": round(max(0.0, min(1.0, score)), 4),
        "signals": sorted(signals),
        "components": {
            "stage2_global": global_score,
            "stage2_category": category_score,
            "editorial": editorial_score,
            "freshness": freshness_score,
            "source_quality": round(reliability_score, 4),
            "source_count": source_score,
        },
    }


def _display_rank_scores(
    importance: dict[str, Any], *, event_updated_at: str, now: datetime
) -> dict[str, float]:
    """Build view-specific ranks from durable editorial signals and current freshness."""
    base_score = _safe_score(importance.get("score"))
    components = (
        importance.get("components") if isinstance(importance.get("components"), dict) else {}
    )
    global_score = _safe_score(components.get("stage2_global", base_score))
    category_score = _safe_score(components.get("stage2_category", base_score))
    editorial_score = _safe_score(components.get("editorial", base_score))
    source_quality = _safe_score(components.get("source_quality", base_score))
    source_count = _safe_score(components.get("source_count", base_score))
    trust_and_coverage = (source_quality + source_count) / 2
    age_hours = max(0.0, (now - _parse_datetime(event_updated_at)).total_seconds() / 3600)
    freshness = max(0.15, 1.0 - (age_hours / 72.0))
    homepage = (
        global_score * 0.42
        + category_score * 0.13
        + editorial_score * 0.15
        + trust_and_coverage * 0.10
        + freshness * 0.20
    )
    category = (
        global_score * 0.12
        + category_score * 0.48
        + editorial_score * 0.10
        + trust_and_coverage * 0.10
        + freshness * 0.20
    )
    return {
        "homepage": round(max(0.0, min(1.0, homepage)), 4),
        "category": round(max(0.0, min(1.0, category)), 4),
        "freshness": round(freshness, 4),
    }


def write_active_stories_index(
    *,
    state: StateDB,
    story_dir: Path = STORY_DIR,
    output_path: Path = ACTIVE_STORIES_PATH,
    curation_client: JsonGenerator | None = None,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(UTC)
    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=UTC)
    generated_at = generated_at.astimezone(UTC)
    rows = state.conn.execute(
        """
        SELECT event_id, status, created_at AS event_created_at, updated_at AS event_updated_at
        FROM events
        WHERE status IN ('active', 'stale')
        ORDER BY event_id
        """
    ).fetchall()
    stories: list[dict[str, Any]] = []
    story_details: dict[str, dict[str, Any]] = {}
    story_source_names: dict[str, set[str]] = {}
    missing = 0
    for row in rows:
        story = _read_json(story_dir / f"{row['event_id']}.json")
        if story is None or story.get("_pending_coherence"):
            missing += 1
            continue
        story_details[row["event_id"]] = story
        sources = story.get("sources") if isinstance(story.get("sources"), list) else []
        source_metrics, source_names = _source_coverage_metrics(sources)
        story_source_names[row["event_id"]] = source_names
        importance = story.get("importance") if isinstance(story.get("importance"), dict) else {}
        display_rank = _display_rank_scores(
            importance,
            event_updated_at=row["event_updated_at"],
            now=generated_at,
        )
        stories.append(
            {
                "story_id": story.get("story_id") or row["event_id"],
                "category": story.get("category"),
                "headline": story.get("headline"),
                "importance_score": _safe_score(importance.get("score")),
                "homepage_rank_score": display_rank["homepage"],
                "category_rank_score": display_rank["category"],
                "freshness_score": display_rank["freshness"],
                **source_metrics,
                "status": row["status"],
                "event_created_at": row["event_created_at"],
                "event_updated_at": row["event_updated_at"],
                "created_at": story.get("created_at"),
                "updated_at": story.get("updated_at"),
            }
        )
    rolling_window_hours = int(
        load_pipeline_config().presentation.get("rolling_window_hours", 72)
    )
    coverage_cutoff = generated_at - timedelta(hours=max(1, rolling_window_hours))
    category_source_pools: dict[str, set[str]] = {}
    for story in stories:
        if _parse_datetime(str(story.get("event_updated_at") or "")) < coverage_cutoff:
            continue
        category = str(story.get("category") or "other")
        category_source_pools.setdefault(category, set()).update(
            story_source_names.get(str(story["story_id"]), set())
        )
    for story in stories:
        category = str(story.get("category") or "other")
        pool_size = len(category_source_pools.get(category, set()))
        source_count = int(story.get("source_count") or 0)
        story["category_source_pool"] = pool_size
        story["source_coverage_ratio"] = round(
            min(1.0, source_count / pool_size) if pool_size else 0.0,
            4,
        )
    stories.sort(key=lambda item: str(item["event_updated_at"] or ""), reverse=True)
    stories.sort(key=lambda item: item["homepage_rank_score"], reverse=True)
    curation_mode = "fallback"
    try:
        curation = generate_homepage_curation(
            stories=stories,
            story_details=story_details,
            client=curation_client,
            generated_at=generated_at,
        )
        curation_mode = "llm" if curation_client is not None else "fallback"
        if curation_client is not None and run_id:
            for usage_record in curation.get("usage_records") or []:
                usage = usage_record.get("usage") or {}
                state.record_llm_usage(
                    run_id=run_id,
                    stage="homepage_curation",
                    model=str(usage_record.get("model") or curation_client.model),
                    prompt_version=HOMEPAGE_CURATION_PROMPT_VERSION,
                    input_tokens=usage.get("promptTokenCount"),
                    output_tokens=usage.get("candidatesTokenCount"),
                )
            for index, error in enumerate(curation.get("errors") or [], start=1):
                state.record_error(
                    run_id,
                    "homepage_curation",
                    "batch",
                    f"curation-{index}",
                    None,
                    RuntimeError(str(error)),
                )
                if progress:
                    progress(f"editorial: homepage curation batch failed: {error}")
    except Exception as exc:
        curation = generate_homepage_curation(
            stories=stories,
            story_details=story_details,
            client=None,
            generated_at=generated_at,
        )
        if run_id:
            state.record_error(run_id, "homepage_curation", "index", "active-stories", None, exc)
        if progress:
            progress(f"editorial: homepage curation fell back to ranked stories: {exc}")
    public_curation = {
        key: value
        for key, value in curation.items()
        if key not in {"errors", "usage", "usage_records"}
    }
    atomic_write_json(
        output_path,
        {
            "generated_at": isoformat_z(generated_at),
            "ranking_version": "display-ranking-v2",
            "curation": public_curation,
            "stories": stories,
        },
    )
    return {
        "active_index_stories": len(stories),
        "active_index_missing": missing,
        "curation_mode": curation_mode,
        "curation_sections": len(public_curation.get("sections") or []),
        "curation_top_news": len(public_curation.get("top_news") or []),
    }


def generate_homepage_curation(
    *,
    stories: Sequence[dict[str, Any]],
    story_details: dict[str, dict[str, Any]],
    client: JsonGenerator | None,
    generated_at: datetime,
    rolling_window_hours: int | None = None,
) -> dict[str, Any]:
    if rolling_window_hours is None:
        config = load_pipeline_config()
        rolling_window_hours = int(config.presentation.get("rolling_window_hours", 72))
    cutoff = generated_at - timedelta(hours=max(1, rolling_window_hours))
    current = [
        story
        for story in stories
        if _parse_datetime(str(story.get("event_updated_at") or "")) >= cutoff
    ]
    fallback_candidates = sorted(
        current,
        key=lambda story: _homepage_coverage_priority(story, generated_at=generated_at),
        reverse=True,
    )
    target_top_count = min(DEFAULT_CURATION_TOP_STORIES, len(current))
    fallback_top = [
        str(story["story_id"]) for story in fallback_candidates[:target_top_count]
    ]
    if client is None or not current:
        return {
            "prompt_version": HOMEPAGE_CURATION_PROMPT_VERSION,
            "model": "deterministic-ranking-fallback",
            "generated_at": isoformat_z(generated_at),
            "input_story_count": len(current),
            "top_news": fallback_top,
            "sections": [],
        }

    context: list[dict[str, Any]] = []
    for story in current:
        story_id = str(story["story_id"])
        detail = story_details.get(story_id) or {}
        age_hours = max(
            0.0,
            (generated_at - _parse_datetime(str(story.get("event_updated_at") or ""))).total_seconds()
            / 3600,
        )
        context.append(
            {
                "id": story_id,
                "category": story.get("category"),
                "headline": story.get("headline"),
                "dek": detail.get("dek"),
                "homepage_rank": story.get("homepage_rank_score"),
                "category_rank": story.get("category_rank_score"),
                "source_count": story.get("source_count"),
                "source_article_count": story.get("source_article_count"),
                "multi_angle_source_count": story.get("multi_angle_source_count"),
                "source_coverage_score": story.get("source_coverage_score"),
                "category_source_pool": story.get("category_source_pool"),
                "source_coverage_ratio": story.get("source_coverage_ratio"),
                "coverage_priority": _homepage_coverage_priority(
                    story,
                    generated_at=generated_at,
                ),
                "hours_old": round(age_hours, 1),
            }
        )
    errors: list[str] = []
    usage_records: list[dict[str, Any]] = []
    models: set[str] = set()
    raw_top_news: list[str] = []
    try:
        top_result = client.generate_json(
            system_instruction=(
                "You are a senior news homepage editor organizing a concise rolling briefing."
            ),
            prompt=_build_top_news_curation_prompt(
                context[:DEFAULT_CURATION_TOP_CANDIDATES],
                target_top_count=target_top_count,
            ),
            response_schema=_top_news_curation_response_schema(),
            max_output_tokens=2_000,
            thinking_level="low",
        )
        models.add(top_result.model)
        usage_records.append({"model": top_result.model, "usage": top_result.usage})
        if isinstance(top_result.payload.get("top_news"), list):
            raw_top_news = top_result.payload["top_news"]
    except Exception as exc:
        errors.append(f"top-news: {exc}")

    contexts_by_category: dict[str, list[dict[str, Any]]] = {}
    for story in context:
        contexts_by_category.setdefault(str(story.get("category") or "other"), []).append(story)

    section_specs: list[tuple[str, int, list[dict[str, Any]]]] = []
    for category in sorted(contexts_by_category):
        category_stories = sorted(
            contexts_by_category[category],
            key=lambda story: (
                _safe_score(story.get("category_rank")),
                float(story.get("coverage_priority") or 0.0),
                -float(story.get("hours_old") or 0.0),
            ),
            reverse=True,
        )[:DEFAULT_CURATION_STORIES_PER_CATEGORY]
        if len(category_stories) >= 2:
            section_specs.append((category, 1, category_stories))
    section_results: list[GeminiResult | None] = [None] * len(section_specs)
    worker_count = min(2, len(section_specs))
    if worker_count <= 1:
        for index, (category, chunk, chunk_stories) in enumerate(section_specs):
            try:
                section_results[index] = _generate_category_sections(
                    client=client,
                    category=category,
                    stories=chunk_stories,
                )
            except Exception as exc:
                errors.append(f"{category}-{chunk}: {exc}")
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    _generate_category_sections,
                    client=client,
                    category=category,
                    stories=chunk_stories,
                ): index
                for index, (category, _chunk, chunk_stories) in enumerate(section_specs)
            }
            for future in as_completed(futures):
                index = futures[future]
                try:
                    section_results[index] = future.result()
                except Exception as exc:
                    category, chunk, _stories = section_specs[index]
                    errors.append(f"{category}-{chunk}: {exc}")

    raw_sections: list[dict[str, Any]] = []
    for result in section_results:
        if result is None:
            continue
        models.add(result.model)
        usage_records.append({"model": result.model, "usage": result.usage})
        sections = result.payload.get("sections")
        if isinstance(sections, list):
            raw_sections.extend(sections[:DEFAULT_CURATION_MAX_SECTIONS_PER_CATEGORY])
    normalized = _validate_homepage_curation(
        {"top_news": raw_top_news, "sections": raw_sections},
        valid_story_ids={str(story["story_id"]) for story in current},
        fallback_top=fallback_top,
        target_top_count=target_top_count,
    )
    return {
        "prompt_version": HOMEPAGE_CURATION_PROMPT_VERSION,
        "model": ",".join(sorted(models)) or "deterministic-ranking-fallback",
        "generated_at": isoformat_z(generated_at),
        "input_story_count": len(current),
        "calls": len(usage_records),
        "partial_failures": len(errors),
        **normalized,
        "errors": errors,
        "usage_records": usage_records,
    }


def _build_top_news_curation_prompt(
    stories: Sequence[dict[str, Any]],
    *,
    target_top_count: int,
) -> str:
    return (
        "Choose the Top News cards for a rolling news homepage.\n\n"
        f"Top News: choose exactly {target_top_count} story IDs representing the most important "
        "distinct underlying subjects. Favor high-impact developments from the last 12-24 hours, "
        "while retaining an older item only when it remains clearly consequential. Do not choose "
        "two cards about the same underlying event. Rank public consequence first: major wars, "
        "diplomacy, national security, public health, governance, disasters, and major cultural "
        "events can outrank routine product, market, campaign, or celebrity-cycle updates. Use "
        "coverage_priority as corroboration and a tie-breaker, not as a substitute for editorial "
        "judgment. It normalizes source breadth for category feed availability and gives only a "
        "capped smaller boost to multiple angles from one publisher. Do not let categories with "
        "larger feed inventories dominate merely because they can accumulate more raw sources. "
        "Order the IDs by editorial importance.\n\n"
        "Return only the requested JSON. Candidate story cards:\n"
        + json.dumps(stories, ensure_ascii=False, separators=(",", ":"))
    )


def _build_category_sections_prompt(
    category: str,
    stories: Sequence[dict[str, Any]],
) -> str:
    return (
        f"Organize these {category} story cards into useful topic sections for a rolling news "
        "homepage. Concentrate on the highest-ranked, best-supported cards in the supplied set. "
        "Group cards only when at least two distinct stories belong under a useful, "
        "specific ongoing subject such as 'Ukraine War', 'Canada/US Relations', or 'New Car "
        "Releases'. Prefer coherent sections with three or more stories. When a narrow heading "
        "would contain only one or two cards, broaden it to a still-meaningful regional or subject "
        "desk such as 'Middle East' rather than 'Saudi Arabia', but never combine unrelated news. "
        "Use concise 2-5 word headings, order sections by current news value, and put each story "
        "in at most one section. Related but distinct developments can share a section; "
        "cards covering the exact same event should still be treated as one subject, not used to "
        "manufacture a grouping. "
        "Do not create generic category headings, force weak relationships, or include singleton "
        "sections. Leave stories that do not fit a meaningful group unassigned; the site will place "
        "them in a category-specific remainder section. Coverage fields are normalized for category feed availability; "
        "use them as supporting evidence when deciding which useful groups to retain. Return at most "
        f"{DEFAULT_CURATION_MAX_SECTIONS_PER_CATEGORY} sections.\n\n"
        "Return only the requested JSON. Story cards:\n"
        + json.dumps(stories, ensure_ascii=False, separators=(",", ":"))
    )


def _top_news_curation_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {"top_news": {"type": "ARRAY", "items": {"type": "STRING"}}},
        "required": ["top_news"],
    }


def _category_sections_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "sections": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "title": {"type": "STRING"},
                        "story_ids": {"type": "ARRAY", "items": {"type": "STRING"}},
                    },
                    "required": ["title", "story_ids"],
                },
            },
        },
        "required": ["sections"],
    }


def _generate_category_sections(
    *,
    client: JsonGenerator,
    category: str,
    stories: Sequence[dict[str, Any]],
) -> GeminiResult:
    return client.generate_json(
        system_instruction=(
            "You are a senior news homepage editor grouping related but distinct story cards."
        ),
        prompt=_build_category_sections_prompt(category, stories),
        response_schema=_category_sections_response_schema(),
        max_output_tokens=8_000,
        thinking_level="low",
    )


def _validate_homepage_curation(
    payload: dict[str, Any],
    *,
    valid_story_ids: set[str],
    fallback_top: Sequence[str],
    target_top_count: int,
) -> dict[str, Any]:
    raw_top = payload.get("top_news") if isinstance(payload, dict) else []
    top_news: list[str] = []
    for story_id in raw_top if isinstance(raw_top, list) else []:
        if isinstance(story_id, str) and story_id in valid_story_ids and story_id not in top_news:
            top_news.append(story_id)
        if len(top_news) >= target_top_count:
            break
    for story_id in fallback_top:
        if len(top_news) >= target_top_count:
            break
        if story_id not in top_news:
            top_news.append(story_id)

    sections: list[dict[str, Any]] = []
    assigned: set[str] = set()
    raw_sections = payload.get("sections") if isinstance(payload, dict) else []
    for raw_section in raw_sections if isinstance(raw_sections, list) else []:
        if len(sections) >= DEFAULT_CURATION_MAX_SECTIONS or not isinstance(raw_section, dict):
            break
        title = " ".join(str(raw_section.get("title") or "").split()).strip(" .:-")[:60]
        if not title:
            continue
        raw_ids = raw_section.get("story_ids")
        story_ids: list[str] = []
        for story_id in raw_ids if isinstance(raw_ids, list) else []:
            if (
                isinstance(story_id, str)
                and story_id in valid_story_ids
                and story_id not in assigned
                and story_id not in story_ids
            ):
                story_ids.append(story_id)
        if len(story_ids) < 2:
            continue
        assigned.update(story_ids)
        sections.append({"title": title, "story_ids": story_ids})
    return {"top_news": top_news, "sections": sections}


def _source_coverage_metrics(
    sources: Sequence[dict[str, Any]],
) -> tuple[dict[str, Any], set[str]]:
    articles_by_source: dict[str, set[str]] = {}
    for index, source in enumerate(sources):
        if not isinstance(source, dict) or not source.get("source_name"):
            continue
        source_name = publisher_id(source)
        article_key = str(
            source.get("article_id") or source.get("url") or f"source-entry-{index}"
        )
        articles_by_source.setdefault(source_name, set()).add(article_key)
    angle_count = sum(
        min(max(len(article_ids) - 1, 0), 2)
        for article_ids in articles_by_source.values()
    )
    source_names = set(articles_by_source)
    return (
        {
            "source_count": len(source_names),
            "source_article_count": sum(len(value) for value in articles_by_source.values()),
            "multi_angle_source_count": sum(
                1 for value in articles_by_source.values() if len(value) > 1
            ),
            "source_coverage_score": round(len(source_names) + 0.5 * angle_count, 2),
            "known_reporting_origins": sorted({str(s["reporting_origin"]) for s in sources
                                                if isinstance(s, dict) and s.get("reporting_origin")}),
            "reporting_provenance_complete": bool(sources) and all(
                isinstance(s, dict) and s.get("reporting_origin") for s in sources),
        },
        source_names,
    )


def _homepage_coverage_priority(
    story: dict[str, Any],
    *,
    generated_at: datetime,
) -> float:
    event_updated_at = _parse_datetime(str(story.get("event_updated_at") or ""))
    age_hours = max(0.0, (generated_at - event_updated_at).total_seconds() / 3600)
    editorial_rank = _safe_score(
        story.get("homepage_rank_score", story.get("importance_score"))
    )
    editorial = CURATION_EDITORIAL_WEIGHT * editorial_rank**3
    if age_hours > CURATION_COVERAGE_WINDOW_HOURS:
        return round(editorial, 4)
    coverage = max(0.0, float(story.get("source_coverage_score") or 0.0))
    coverage_ratio = max(
        0.0,
        min(1.0, float(story.get("source_coverage_ratio") or 0.0)),
    )
    absolute_coverage = 2.0 * math.log2(1.0 + coverage)
    normalized_coverage = 4.0 * math.sqrt(coverage_ratio)
    return round(absolute_coverage + normalized_coverage + editorial, 4)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _bounded_text(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    clipped = text[:limit]
    return clipped.rsplit(" ", 1)[0] or clipped


def _safe_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(score):
        return 0.0
    return max(0.0, min(1.0, score))


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _political_framing_eligible(event: EditorialEvent) -> bool:
    if event.category not in POLITICAL_CATEGORIES:
        return False
    labels = {article.bias_label for article in event.articles}
    return bool(labels & LEFT_BIAS_LABELS and labels & RIGHT_BIAS_LABELS)
