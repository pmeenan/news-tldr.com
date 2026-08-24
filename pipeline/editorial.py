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
from pipeline.llm import GeminiEmptyResponseError, GeminiResult, create_gemini_client
from pipeline.lock import PipelineLock
from pipeline.paths import ACTIVE_STORIES_PATH, LOCK_PATH, PROJECT_ROOT, STORY_DIR
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z

EDITORIAL_PROMPT_VERSION = "editorial-v2"
EDITORIAL_FRAMING_PROMPT_VERSION = "editorial-framing-v1"
EDITORIAL_COMPACT_PROMPT_VERSION = "editorial-v2-compact"
EDITORIAL_FRAMING_COMPACT_PROMPT_VERSION = "editorial-framing-v1-compact"
DEFAULT_ARTICLE_CHAR_LIMIT = 12_000
DEFAULT_EVENT_CHAR_LIMIT = 60_000
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
            executor.submit(generate_story, event, client=client): event for event in events
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
                state.record_llm_usage(
                    run_id=run_id,
                    stage="editorial",
                    model=generated["model"],
                    prompt_version=generated["prompt_version"],
                    input_tokens=usage.get("promptTokenCount"),
                    output_tokens=usage.get("candidatesTokenCount"),
                )
                for key in stats["usage"]:
                    stats["usage"][key] += int(usage.get(key) or 0)
                stats["completed"] += 1
                if progress:
                    progress(
                        f"editorial: {processed}/{len(events)} completed {event.event_id}"
                    )
            except Exception as exc:
                stats["failed"] += 1
                state.record_error(run_id, "editorial", "event", event.event_id, None, exc)
                if progress:
                    progress(
                        f"editorial: {processed}/{len(events)} failed {event.event_id}: {exc}"
                    )
    return stats


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

    per_article_limit = min(article_char_limit, max(1, event_char_limit // len(article_rows)))
    articles: list[EditorialArticle] = []
    for article_row in article_rows:
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


def generate_story(event: EditorialEvent, *, client: JsonGenerator) -> dict[str, Any]:
    framing_eligible = _political_framing_eligible(event)
    selected_event = event
    compact_retry = False
    try:
        result = _request_editorial_story(
            event,
            client=client,
            framing_eligible=framing_eligible,
        )
    except GeminiEmptyResponseError:
        selected_event = _compact_editorial_event(event)
        compact_retry = True
        result = _request_editorial_story(
            selected_event,
            client=client,
            framing_eligible=framing_eligible,
            compact=True,
        )
    validated = validate_editorial_response(result.payload, selected_event)
    if compact_retry:
        prompt_version = (
            EDITORIAL_FRAMING_COMPACT_PROMPT_VERSION
            if framing_eligible
            else EDITORIAL_COMPACT_PROMPT_VERSION
        )
    else:
        prompt_version = (
            EDITORIAL_FRAMING_PROMPT_VERSION if framing_eligible else EDITORIAL_PROMPT_VERSION
        )
    return {
        "payload": validated,
        "model": result.model,
        "prompt_version": prompt_version,
        "usage": result.usage,
    }


def _request_editorial_story(
    event: EditorialEvent,
    *,
    client: JsonGenerator,
    framing_eligible: bool,
    compact: bool = False,
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
        prompt=_build_editorial_prompt(event),
        response_schema=_editorial_response_schema(framing_decision=framing_eligible),
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
        "When false, omit political_framing."
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
        "- Headline and dek must be neutral, specific, and supported. Avoid clickbait and outlet framing. "
        "Write the headline in sentence case, preserving normal capitalization for proper names and acronyms.\n"
        "- tldr must contain 2-4 standalone bullets explaining what happened, who is affected, what changed, "
        "and the most important next step or unknown.\n"
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


def _editorial_response_schema(*, framing_decision: bool = False) -> dict[str, Any]:
    cited_item = {
        "type": "OBJECT",
        "properties": {
            "text": {"type": "STRING"},
            "source_article_ids": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["text", "source_article_ids"],
    }
    perspective = {
        "type": "OBJECT",
        "properties": {
            "summary": {"type": "STRING"},
            "source_article_ids": {"type": "ARRAY", "items": {"type": "STRING"}},
        },
        "required": ["summary", "source_article_ids"],
    }
    schema = {
        "type": "OBJECT",
        "properties": {
            "headline": {"type": "STRING"},
            "dek": {"type": "STRING"},
            "tldr": {"type": "ARRAY", "items": {"type": "STRING"}},
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
            "tldr",
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
    citations = sorted({str(item) for item in value if str(item) in allowed_ids})
    if not citations:
        raise ValueError(f"{field} must cite at least one valid source article")
    return citations


def _required_text(value: Any, field: str, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        raise ValueError(f"{field} must not be empty")
    return text[:max_chars]


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
    sources = [
        {
            "article_id": article.article_id,
            "source_name": article.source_name,
            "headline": article.headline,
            "url": article.url,
        }
        for article in event.articles
    ]
    return {
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
    source_count = len({article.source_id for article in event.articles})
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
) -> dict[str, Any]:
    generated_at = datetime.now(UTC)
    rows = state.conn.execute(
        """
        SELECT event_id, status, created_at AS event_created_at, updated_at AS event_updated_at
        FROM events
        WHERE status IN ('active', 'stale')
        ORDER BY event_id
        """
    ).fetchall()
    stories: list[dict[str, Any]] = []
    missing = 0
    for row in rows:
        story = _read_json(story_dir / f"{row['event_id']}.json")
        if story is None:
            missing += 1
            continue
        sources = story.get("sources") if isinstance(story.get("sources"), list) else []
        distinct_source_names = {
            str(source.get("source_name"))
            for source in sources
            if isinstance(source, dict) and source.get("source_name")
        }
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
                "source_count": len(distinct_source_names),
                "status": row["status"],
                "event_created_at": row["event_created_at"],
                "event_updated_at": row["event_updated_at"],
                "created_at": story.get("created_at"),
                "updated_at": story.get("updated_at"),
            }
        )
    stories.sort(key=lambda item: str(item["event_updated_at"] or ""), reverse=True)
    stories.sort(key=lambda item: item["homepage_rank_score"], reverse=True)
    atomic_write_json(
        output_path,
        {
            "generated_at": isoformat_z(generated_at),
            "ranking_version": "display-ranking-v1",
            "stories": stories,
        },
    )
    return {"active_index_stories": len(stories), "active_index_missing": missing}


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
