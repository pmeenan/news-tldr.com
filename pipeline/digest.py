from __future__ import annotations

import json
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from pipeline.config import load_pipeline_config
from pipeline.llm import GeminiClient, GeminiResult
from pipeline.paths import PROJECT_ROOT
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z

ARTICLE_DIGEST_PROMPT_VERSION = "article-digest-v2"
ARTICLE_DIGEST_EXPERIMENT_PROMPT_VERSION = "article-digest-experiment-v1"
DEFAULT_CONTENT_CHAR_LIMIT = 12000


class JsonGenerator(Protocol):
    model: str

    def generate_json(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_schema: dict[str, Any],
        temperature: float = 0,
    ) -> GeminiResult: ...


@dataclass(frozen=True)
class ArticleForDigest:
    article_id: str
    source_id: str
    source_name: str
    headline: str
    summary: str | None
    published_at: str | None
    article_path: str
    content_text: str
    selection_reason: str


def run_article_digest_experiment(
    *,
    limit: int = 40,
    control_count: int = 8,
    published_date: str | None = None,
    published_after: str | None = None,
    published_before: str | None = None,
    client: JsonGenerator | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if control_count < 0:
        raise ValueError("control_count cannot be negative")
    articles = select_digest_experiment_articles(
        limit=limit,
        control_count=control_count,
        published_date=published_date,
        published_after=published_after,
        published_before=published_before,
    )
    generator = client or GeminiClient()
    results: list[dict[str, Any]] = []
    usage_totals = {"promptTokenCount": 0, "candidatesTokenCount": 0}
    for index, article in enumerate(articles, start=1):
        if progress:
            progress(
                "article digest experiment: "
                f"{index}/{len(articles)} {article.source_id} {article.article_id[:12]}"
            )
        digest_result = generate_article_digest(article, client=generator)
        for key in usage_totals:
            usage_totals[key] += int(digest_result["usage"].get(key) or 0)
        results.append(digest_result)

    return {
        "prompt_version": ARTICLE_DIGEST_EXPERIMENT_PROMPT_VERSION,
        "model": generator.model,
        "article_count": len(results),
        "limit": limit,
        "control_count": control_count,
        "published_date": published_date,
        "published_after": published_after,
        "published_before": published_before,
        "usage": usage_totals,
        "articles": results,
    }


def digest_articles_for_aggregation(
    *,
    state: StateDB,
    run_id: str,
    published_after: str | None = None,
    published_before: str | None = None,
    limit: int | None = None,
    concurrency: int = 8,
    content_char_limit: int = DEFAULT_CONTENT_CHAR_LIMIT,
    client: JsonGenerator | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    generator = client or GeminiClient()
    max_retries = int(load_pipeline_config().pipeline.get("max_item_retries", 3))
    stats: dict[str, Any] = {
        "candidates": 0,
        "completed": 0,
        "skipped": 0,
        "failed": 0,
        "existing_digest": 0,
        "reprints_copied_persisted": 0,
        "reprints_copied_in_batch": 0,
        "usage": {"promptTokenCount": 0, "candidatesTokenCount": 0},
    }
    candidates: list[ArticleForDigest] = []
    digest_lookup_cache: dict[str, dict[str, Any] | None] = {}

    # Map from fingerprint key to canonical ArticleForDigest candidate
    fingerprint_to_canonical: dict[str, ArticleForDigest] = {}
    # Map from canonical article_id to list of duplicate rows (article_id, article_path) in the current batch
    canonical_to_duplicates: dict[str, list[tuple[str, str]]] = {}

    rows = _digest_candidate_rows(
        state=state,
        published_after=published_after,
        published_before=published_before,
        limit=limit,
    )
    retry_counts = _digest_retry_counts(state, [row["article_id"] for row in rows])

    for row in rows:
        fp_key = _fingerprint_key(row)
        existing_digest = None
        if fp_key:
            existing_digest = _find_completed_digest_by_fingerprint(
                state,
                content_hash=row["content_hash"],
                canonical_url_hash=row["canonical_url_hash"],
                cache=digest_lookup_cache,
            )

        if existing_digest:
            # Reprint detected with an already completed digest. Copy it.
            _copy_digest_to_article(
                target_article_id=row["article_id"],
                target_article_path=row["article_path"],
                digest=existing_digest,
                state=state,
                run_id=run_id,
            )
            stats["reprints_copied_persisted"] += 1
            if progress:
                progress(
                    "article digest: reprint detected and digest copied for "
                    f"{row['source_id']} {row['article_id'][:12]}"
                )
            continue

        if fp_key and fp_key in fingerprint_to_canonical:
            # Reprint of another candidate in the current batch. Defer copying.
            canonical_article = fingerprint_to_canonical[fp_key]
            canonical_to_duplicates.setdefault(canonical_article.article_id, []).append(
                (row["article_id"], row["article_path"])
            )
            continue

        # Load candidate
        article, skip_reason = _load_article_for_pipeline_digest(
            row,
            state=state,
            model=generator.model,
            retry_count=retry_counts.get(row["article_id"], 0),
            max_retries=max_retries,
        )
        if article is not None:
            if fp_key:
                fingerprint_to_canonical[fp_key] = article
            candidates.append(article)
        elif skip_reason == "already_completed":
            stats["existing_digest"] += 1
        elif skip_reason:
            stats["skipped"] += 1

    stats["candidates"] = len(candidates)
    if progress:
        progress(
            "article digest: "
            f"{len(candidates)} article(s) need digests, concurrency={concurrency}"
        )
    if not candidates:
        return stats

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_article = {
            executor.submit(
                generate_article_digest,
                article,
                client=generator,
                content_char_limit=content_char_limit,
            ): article
            for article in candidates
        }
        completed_count = 0
        for future in as_completed(future_to_article):
            article = future_to_article[future]
            completed_count += 1
            try:
                result = future.result()
                _persist_pipeline_digest_result(
                    result,
                    state=state,
                    run_id=run_id,
                    model=generator.model,
                )

                # Propagate digest to any deferred reprints in this batch
                duplicates = canonical_to_duplicates.get(article.article_id)
                if duplicates:
                    generated_digest = {
                        "summary": result["digest"]["summary"],
                        "key_facts": result["digest"]["key_facts"],
                        "content_quality": result["digest"]["content_quality"],
                        "impact": result["digest"]["impact"],
                        "generated_at": isoformat_z(),
                        "model": generator.model,
                        "prompt_version": ARTICLE_DIGEST_PROMPT_VERSION,
                        "content_chars_used": result["content_chars_used"],
                    }
                    for dup_id, dup_path in duplicates:
                        _copy_digest_to_article(
                            target_article_id=dup_id,
                            target_article_path=dup_path,
                            digest=generated_digest,
                            state=state,
                            run_id=run_id,
                        )
                        stats["reprints_copied_in_batch"] += 1
                        if progress:
                            progress(
                                "article digest: copied generated digest to reprint "
                                f"{dup_id[:12]}"
                            )

                for key in stats["usage"]:
                    stats["usage"][key] += int(result["usage"].get(key) or 0)
                stats["completed"] += 1
                if progress:
                    progress(
                        "article digest: "
                        f"{completed_count}/{len(candidates)} completed {article.source_id} "
                        f"{article.article_id[:12]}"
                    )
            except Exception as exc:
                stats["failed"] += 1
                state.update_article_digest_status(
                    article.article_id,
                    status="failed",
                    error=str(exc)[:1000],
                )
                state.record_error(
                    run_id,
                    "article_digest",
                    "article",
                    article.article_id,
                    article.source_id,
                    exc,
                )

                # Mark any duplicate reprints as failed as well
                duplicates = canonical_to_duplicates.get(article.article_id)
                if duplicates:
                    for dup_id, dup_path in duplicates:
                        state.update_article_digest_status(
                            dup_id,
                            status="failed",
                            error=f"canonical digest failed: {str(exc)[:900]}",
                        )

                if progress:
                    progress(
                        "article digest: "
                        f"{completed_count}/{len(candidates)} failed {article.source_id} "
                        f"{article.article_id[:12]}: {exc}"
                    )
    return stats


def select_digest_experiment_articles(
    *,
    limit: int,
    control_count: int,
    published_date: str | None = None,
    published_after: str | None = None,
    published_before: str | None = None,
    db: StateDB | None = None,
) -> list[ArticleForDigest]:
    if published_date is not None:
        datetime.strptime(published_date, "%Y-%m-%d")
    close_db = db is None
    state = db or StateDB()
    try:
        rows = _candidate_rows(
            state=state,
            published_date=published_date,
            published_after=published_after,
            published_before=published_before,
        )
        candidates: list[ArticleForDigest] = []
        for row in rows:
            article = _load_article_for_digest(row)
            if article is not None:
                candidates.append(article)
        low_signal = [article for article in candidates if article.selection_reason != "control"]
        controls = [article for article in candidates if article.selection_reason == "control"]
        selected = low_signal[: max(0, limit - control_count)]
        selected_ids = {article.article_id for article in selected}
        for article in controls:
            if len(selected) >= limit:
                break
            if article.article_id not in selected_ids:
                selected.append(article)
                selected_ids.add(article.article_id)
        for article in low_signal:
            if len(selected) >= limit:
                break
            if article.article_id not in selected_ids:
                selected.append(article)
                selected_ids.add(article.article_id)
        return selected
    finally:
        if close_db:
            state.close()


def generate_article_digest(
    article: ArticleForDigest,
    *,
    client: JsonGenerator,
    content_char_limit: int = DEFAULT_CONTENT_CHAR_LIMIT,
) -> dict[str, Any]:
    prompt = _build_digest_prompt(article, content_char_limit=content_char_limit)
    result = client.generate_json(
        system_instruction=(
            "You write accurate, neutral article digests for a news aggregation pipeline. "
            "Use only the supplied article text. Do not infer, guess, or add facts that "
            "are not directly stated in that text. If a detail is unclear, omit it rather "
            "than speculate."
        ),
        prompt=prompt,
        response_schema=_digest_response_schema(),
        temperature=0,
    )
    digest = validate_digest_response(result.payload)
    return {
        "article_id": article.article_id,
        "source_id": article.source_id,
        "source_name": article.source_name,
        "published_at": article.published_at,
        "headline": article.headline,
        "article_path": article.article_path,
        "selection_reason": article.selection_reason,
        "original_summary": _normalize_text(article.summary),
        "content_chars_used": min(len(article.content_text), content_char_limit),
        "digest": digest,
        "elapsed_ms": result.elapsed_ms,
        "usage": result.usage,
    }


def validate_digest_response(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("digest response must be an object")
    summary = payload.get("summary")
    if not isinstance(summary, str) or not summary.strip():
        raise ValueError("digest summary must be a non-empty string")
    key_facts = payload.get("key_facts")
    if not isinstance(key_facts, list) or not key_facts:
        raise ValueError("digest key_facts must be a non-empty list")
    clean_facts: list[str] = []
    for fact in key_facts:
        if not isinstance(fact, str) or not fact.strip():
            raise ValueError("digest key_facts must contain non-empty strings")
        clean_facts.append(_normalize_text(fact))
    quality = payload.get("content_quality")
    valid_quality = {"ok", "thin", "extraction_noise", "paywalled", "non_news"}
    if quality not in valid_quality:
        raise ValueError(f"digest content_quality must be one of {sorted(valid_quality)}")
    impact = _validate_impact(payload.get("impact"))
    return {
        "summary": _normalize_text(summary),
        "key_facts": clean_facts,
        "content_quality": quality,
        "impact": impact,
    }


def write_digest_experiment_result(result: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def default_digest_experiment_output_path() -> Path:
    return PROJECT_ROOT / "data" / "staging" / "article-digest-experiments" / "latest.json"


def _candidate_rows(
    *,
    state: StateDB,
    published_date: str | None,
    published_after: str | None,
    published_before: str | None,
) -> list[Any]:
    params: list[Any] = []
    where = "1=1"
    if published_date is not None:
        where += " AND substr(published_at, 1, 10) = ?"
        params.append(published_date)
    if published_after is not None:
        where += " AND published_at >= ?"
        params.append(published_after)
    if published_before is not None:
        where += " AND published_at < ?"
        params.append(published_before)
    return state.conn.execute(
        f"""
        SELECT article_id, source_id, source_name, headline, summary, published_at, article_path
        FROM articles
        WHERE {where}
        ORDER BY published_at DESC, fetched_at DESC
        """,
        params,
    ).fetchall()


def _digest_candidate_rows(
    *,
    state: StateDB,
    published_after: str | None,
    published_before: str | None,
    limit: int | None,
) -> list[Any]:
    # Select rows that either (a) are not yet completed/skipped, or
    # (b) were processed at an older prompt version and need refreshing.
    # `digest_prompt_version != ?` evaluates to NULL when the column is NULL
    # (which SQLite treats as false), so the explicit `IS NULL` branch is
    # required to pick up rows that have never been digested.
    params: list[Any] = [ARTICLE_DIGEST_PROMPT_VERSION]
    where = (
        "(a.digest_status NOT IN ('completed', 'skipped') "
        "OR a.digest_prompt_version IS NULL "
        "OR a.digest_prompt_version != ?)"
    )
    if published_after is not None:
        where += " AND a.published_at >= ?"
        params.append(published_after)
    if published_before is not None:
        where += " AND a.published_at < ?"
        params.append(published_before)
    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT ?"
        params.append(limit)
    return state.conn.execute(
        f"""
        SELECT a.article_id, a.source_id, a.source_name, a.headline, a.summary, a.published_at, a.article_path,
               a.digest_status, a.digest_prompt_version,
               f.canonical_url_hash, f.headline_hash, f.summary_hash, f.content_hash
        FROM articles a
        LEFT JOIN article_fingerprints f ON a.article_id = f.article_id
        WHERE {where}
        ORDER BY a.published_at DESC, a.fetched_at DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def _load_article_for_digest(row: Any) -> ArticleForDigest | None:
    article_path = row["article_path"]
    path = PROJECT_ROOT / article_path
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    content_text = _normalize_text(data.get("content_text"))
    if len(content_text) < 500:
        return None
    summary = _normalize_text(row["summary"])
    reason = _summary_quality_reason(row["headline"], summary, content_text)
    return ArticleForDigest(
        article_id=row["article_id"],
        source_id=row["source_id"],
        source_name=row["source_name"],
        headline=row["headline"],
        summary=summary,
        published_at=row["published_at"],
        article_path=article_path,
        content_text=content_text,
        selection_reason=reason,
    )


def _digest_retry_counts(state: StateDB, article_ids: list[str]) -> dict[str, int]:
    if not article_ids:
        return {}
    counts: dict[str, int] = {}
    chunk_size = 500
    for start in range(0, len(article_ids), chunk_size):
        chunk = article_ids[start : start + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = state.conn.execute(
            f"""
            SELECT item_id, COUNT(*) AS error_count
            FROM item_errors
            WHERE item_type = 'article'
              AND stage = 'article_digest'
              AND item_id IN ({placeholders})
            GROUP BY item_id
            """,
            chunk,
        ).fetchall()
        for row in rows:
            counts[row["item_id"]] = int(row["error_count"])
    return counts


def _load_article_for_pipeline_digest(
    row: Any,
    *,
    state: StateDB,
    model: str,
    retry_count: int,
    max_retries: int,
) -> tuple[ArticleForDigest | None, str | None]:
    article_path = row["article_path"]
    path = PROJECT_ROOT / article_path
    if not path.exists():
        # Pin the prompt version so the candidate query stops re-selecting
        # this row on every subsequent pass. If the JSON later reappears,
        # an external reset of digest_status would be required (same as for
        # max_retries_exceeded skips).
        state.update_article_digest_status(
            row["article_id"],
            status="skipped",
            prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
            error=f"article JSON not found: {article_path}",
        )
        return None, "missing_article_json"
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if retry_count >= max_retries:
        state.update_article_digest_status(
            row["article_id"],
            status="skipped",
            prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
            error=f"skipped: failed {retry_count} times",
        )
        return None, "max_retries_exceeded"

    existing_digest = data.get("llm_digest")
    if (
        isinstance(existing_digest, dict)
        and existing_digest.get("prompt_version") == ARTICLE_DIGEST_PROMPT_VERSION
        and existing_digest.get("summary")
        and existing_digest.get("key_facts")
    ):
        state.update_article_digest_status(
            row["article_id"],
            status="completed",
            generated_at=existing_digest.get("generated_at"),
            model=existing_digest.get("model") or model,
            prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
        )
        return None, "already_completed"
    content_text = _normalize_text(data.get("content_text"))
    if len(content_text) < 500:
        state.update_article_digest_status(
            row["article_id"],
            status="skipped",
            prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
            error="content_text shorter than 500 chars",
        )
        return None, "thin_content"
    summary = _normalize_text(row["summary"])
    return (
        ArticleForDigest(
            article_id=row["article_id"],
            source_id=row["source_id"],
            source_name=row["source_name"],
            headline=row["headline"],
            summary=summary,
            published_at=row["published_at"],
            article_path=article_path,
            content_text=content_text,
            selection_reason=_summary_quality_reason(row["headline"], summary, content_text),
        ),
        None,
    )


def _persist_pipeline_digest_result(
    result: dict[str, Any],
    *,
    state: StateDB,
    run_id: str,
    model: str,
) -> None:
    article_id = result["article_id"]
    generated_at = isoformat_z()
    article_path = PROJECT_ROOT / result["article_path"]
    with article_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    data["llm_digest"] = {
        "summary": result["digest"]["summary"],
        "key_facts": result["digest"]["key_facts"],
        "content_quality": result["digest"]["content_quality"],
        "impact": result["digest"]["impact"],
        "generated_at": generated_at,
        "model": model,
        "prompt_version": ARTICLE_DIGEST_PROMPT_VERSION,
        "content_chars_used": result["content_chars_used"],
    }
    atomic_write_json(article_path, data)
    state.update_article_digest_status(
        article_id,
        status="completed",
        generated_at=generated_at,
        model=model,
        prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
    )
    state.record_llm_usage(
        run_id=run_id,
        stage="article_digest",
        model=model,
        prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
        input_tokens=result["usage"].get("promptTokenCount"),
        output_tokens=result["usage"].get("candidatesTokenCount"),
    )


def _summary_quality_reason(headline: str | None, summary: str, content_text: str) -> str:
    headline_text = _normalize_text(headline).lower()
    summary_text = _normalize_text(summary)
    summary_lower = summary_text.lower()
    if not summary_text:
        return "missing_summary"
    if re.search(r"(more…|more\.\.\.|continue reading|read more)", summary_text, re.I):
        return "feed_boilerplate"
    if len(summary_text) < 80 and len(content_text) > 1000:
        return "very_short_summary"
    if headline_text and (summary_lower == headline_text or summary_lower in headline_text):
        return "headline_only_summary"
    if _looks_generic_teaser(summary_text):
        return "generic_teaser"
    if len(summary_text) < 140 and len(content_text) > 3000:
        return "short_summary"
    return "control"


def _looks_generic_teaser(summary: str) -> bool:
    return bool(
        re.match(
            r"^(comments|what we know so far|this week'?s .*|add these players|"
            r"can .*\?|turns out,|here are \w+ ways|the latest|watch:|listen:)$",
            summary.strip(),
            re.I,
        )
    )


def _build_digest_prompt(article: ArticleForDigest, *, content_char_limit: int) -> str:
    content = article.content_text[:content_char_limit]
    rows = {
        "source": article.source_name,
        "published_at": article.published_at,
        "headline": article.headline,
        "site_summary": _normalize_text(article.summary),
        "article_text": content,
    }
    return (
        "Create a factual digest of this article for later story grouping and editorial summarization.\n"
        "The digest should be as short as the article allows, but include the important factual details needed "
        "to understand what happened, who is involved, where/when it happened, key numbers, stated causes, "
        "consequences, uncertainty, and next steps. Do not recreate the full article. If a complex article "
        "needs more than two sentences to preserve the facts, use more sentences. If the article is thin, noisy, "
        "paywalled, promotional, or not a news article, say so through content_quality and summarize only what "
        "can be supported by the text.\n"
        "Do not include quotes unless the quote itself is the news. Do not copy boilerplate, newsletter text, "
        "ad copy, navigation, captions repeated as story text, or unrelated page chrome. Use neutral wording.\n"
        "Also score this article's impact from the article text: global impact for a broad general-news homepage, "
        "category impact within its strongest vertical, public-interest scope, and whether it is breaking news, "
        "an update/follow-up, analysis, evergreen, or low-signal. Use compact rationale codes.\n"
        "Return JSON only.\n\n"
        f"Article:\n{json.dumps(rows, ensure_ascii=False, separators=(',', ':'))}"
    )


def _digest_response_schema() -> dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "summary": {"type": "STRING"},
            "key_facts": {
                "type": "ARRAY",
                "items": {"type": "STRING"},
            },
            "content_quality": {
                "type": "STRING",
                "enum": ["ok", "thin", "extraction_noise", "paywalled", "non_news"],
            },
            "impact": {
                "type": "OBJECT",
                "properties": {
                    "global": {"type": "NUMBER"},
                    "category": {"type": "NUMBER"},
                    "scope": {
                        "type": "STRING",
                        "enum": ["local", "regional", "national", "international", "niche"],
                    },
                    "novelty": {
                        "type": "STRING",
                        "enum": ["breaking", "update", "analysis", "evergreen", "low_signal"],
                    },
                    "rationale_codes": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                    },
                },
                "required": ["global", "category", "scope", "novelty", "rationale_codes"],
            },
        },
        "required": ["summary", "key_facts", "content_quality", "impact"],
    }


def _validate_impact(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("digest impact must be an object")
    scope = value.get("scope")
    valid_scopes = {"local", "regional", "national", "international", "niche"}
    if scope not in valid_scopes:
        raise ValueError(f"digest impact scope must be one of {sorted(valid_scopes)}")
    novelty = value.get("novelty")
    valid_novelty = {"breaking", "update", "analysis", "evergreen", "low_signal"}
    if novelty not in valid_novelty:
        raise ValueError(f"digest impact novelty must be one of {sorted(valid_novelty)}")
    rationale_codes = value.get("rationale_codes")
    if not isinstance(rationale_codes, list):
        raise ValueError("digest impact rationale_codes must be a list")
    clean_codes = []
    for code in rationale_codes:
        clean = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(code).strip().lower()).strip("_")
        if clean:
            clean_codes.append(clean[:64])
    return {
        "global": _normalize_score(value.get("global"), "impact.global"),
        "category": _normalize_score(value.get("category"), "impact.category"),
        "scope": scope,
        "novelty": novelty,
        "rationale_codes": sorted(set(clean_codes)),
    }


def _normalize_score(value: Any, field: str) -> float:
    if not isinstance(value, int | float):
        raise ValueError(f"digest {field} must be numeric")
    score = float(value)
    if score < 0 or score > 1:
        raise ValueError(f"digest {field} must be between 0 and 1")
    return round(score, 3)


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


# Only content_hash and canonical_url_hash are safe digest-copy keys.
# headline_hash matches two articles whenever they share a (possibly generic)
# headline like "Morning briefing" or "[City] weather forecast", which would
# silently propagate one story's digest onto an unrelated story.
def _fingerprint_key(row: Any) -> str | None:
    if row["content_hash"]:
        return f"content:{row['content_hash']}"
    if row["canonical_url_hash"]:
        return f"url:{row['canonical_url_hash']}"
    return None


def _find_completed_digest_by_fingerprint(
    state: StateDB,
    *,
    content_hash: str | None,
    canonical_url_hash: str | None,
    cache: dict[str, dict[str, Any] | None] | None = None,
) -> dict[str, Any] | None:
    for key, value, column in (
        ("content", content_hash, "content_hash"),
        ("url", canonical_url_hash, "canonical_url_hash"),
    ):
        if not value:
            continue
        cache_key = f"{key}:{value}"
        if cache is not None and cache_key in cache:
            cached = cache[cache_key]
            if cached:
                return cached
            continue
        row = state.conn.execute(
            f"""
            SELECT a.article_path
            FROM articles a
            JOIN article_fingerprints f ON a.article_id = f.article_id
            WHERE f.{column} = ? AND a.digest_status = 'completed'
            LIMIT 1
            """,
            (value,),
        ).fetchone()
        digest = _read_llm_digest_from_file(row["article_path"]) if row else None
        if cache is not None:
            cache[cache_key] = digest
        if digest:
            return digest
    return None


def _read_llm_digest_from_file(article_path: str) -> dict[str, Any] | None:
    path = PROJECT_ROOT / article_path
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        digest = data.get("llm_digest")
        if isinstance(digest, dict) and digest.get("summary") and digest.get("key_facts"):
            return digest
    except Exception:
        pass
    return None


# The article JSON receives the digest dict unchanged, including its real
# generating model. The SQLite digest_model column gets "copied" as a
# provenance marker so downstream queries can tell generated digests from
# reprint copies without re-reading the file.
def _copy_digest_to_article(
    target_article_id: str,
    target_article_path: str,
    digest: dict[str, Any],
    state: StateDB,
    run_id: str,
) -> None:
    article_path = PROJECT_ROOT / target_article_path
    if not article_path.exists():
        return
    with article_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    data["llm_digest"] = digest
    atomic_write_json(article_path, data)
    state.update_article_digest_status(
        target_article_id,
        status="completed",
        generated_at=digest.get("generated_at"),
        model="copied",
        prompt_version=digest.get("prompt_version"),
    )

