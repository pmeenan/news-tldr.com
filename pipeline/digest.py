from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import urlparse

from pipeline.config import load_pipeline_config
from pipeline.llm import GeminiResult, create_gemini_client
from pipeline.lock import PipelineLock
from pipeline.paths import LOCK_PATH, PROJECT_ROOT
from pipeline.state import StateDB
from pipeline.util import atomic_write_json, isoformat_z

ARTICLE_DIGEST_PROMPT_VERSION = "article-digest-v6"
ARTICLE_FILTER_REVIEW_PROMPT_VERSION = "article-filter-review-v1"
DEFAULT_CONTENT_CHAR_LIMIT = 12000
NON_NEWS_IMPACT_CAP = 0.10
PROMOTIONAL_IMPACT_CAP = 0.15
NOISY_CONTENT_IMPACT_CAP = 0.65
MULTI_TOPIC_IMPACT_CAP = 0.30
VENDOR_ANNOUNCEMENT_GLOBAL_CAP = 0.55
VENDOR_ANNOUNCEMENT_CATEGORY_CAP = 0.75
UNCONFIRMED_INJURY_GLOBAL_CAP = 0.60
LOW_IMPACT_RATIONALE_CODES = {
    "affiliate_content",
    "affiliate_deals",
    "archival_index",
    "gambling_advice",
    "gambling_content",
    "low_public_interest",
    "gallery_page",
    "product_advice",
    "product_deal",
    "product_deals",
    "product_recommendation",
    "consumer_review",
    "media_transcript",
    "promotional",
    "promotional_content",
    "promotional_material",
    "puzzle_guide",
    "recycled_content",
    "profile_or_background",
    "video_page",
}
MULTI_TOPIC_RATIONALE_CODES = {
    "live_blog",
    "newsletter_roundup",
}
VENDOR_ANNOUNCEMENT_RATIONALE_CODES = {
    "vendor_announcement",
}
UNCONFIRMED_INJURY_RATIONALE_CODES = {
    "unconfirmed_injury",
}
HIGH_IMPACT_RATIONALE_CODES = {
    "critical_infrastructure",
    "economic_impact",
    "emergency_response",
    "geopolitical_escalation",
    "geopolitical_tension",
    "government_security",
    "major_policy",
    "national_security",
    "public_health",
    "public_safety",
    "security_incident",
}
MEDIA_PAGE_PATH_SEGMENTS = {
    "gallery",
    "galleries",
    "video",
    "videos",
}
MEDIA_PAGE_SLUG_SUFFIXES = (
    "-gallery",
    "-galleries",
    "-video",
    "-videos",
)
LIVE_PAGE_PATH_SEGMENTS = {
    "live",
    "live-news",
    "live-updates",
}
STALE_ESTIMATED_PAGE_DAYS = 30
MONTH_NUMBERS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
URL_NUMERIC_DATE_RE = re.compile(r"(?:^|/)(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)")
URL_MONTH_DATE_RE = re.compile(
    r"(?:^|/)(20\d{2})/(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)/(\d{1,2})(?:/|$)",
    re.I,
)
URL_SHORT_DATE_RE = re.compile(r"(?<!\d)(0?[1-9]|1[0-2])[-_/](0?[1-9]|[12]\d|3[01])[-_/](\d{2})(?!\d)")
TEXT_MONTH_DATE_RE = re.compile(
    r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+([0-3]?\d),\s+(20\d{2})\b",
    re.I,
)
BIOMEDICAL_STUDY_TERMS = (
    "animal model",
    "bacteria",
    "biological",
    "biology",
    "biomedical",
    "biotech",
    "blood pressure",
    "brain",
    "cancer",
    "cardiovascular",
    "clinical trial",
    "dementia",
    "diabetes",
    "disease",
    "dna",
    "drug",
    "early human",
    "epidemiological",
    "epidemiology",
    "fda",
    "gene",
    "genetic",
    "health care",
    "health outcomes",
    "health records",
    "healthcare",
    "heart",
    "human trial",
    "infection",
    "medical",
    "medicine",
    "mental health",
    "mice",
    "mortality",
    "mouse",
    "neurological",
    "neurologic",
    "neurology",
    "obesity",
    "patient",
    "pharmaceutical",
    "preclinical",
    "protein",
    "public health",
    "rna",
    "side effect",
    "therapy",
    "trial phase",
    "tumor",
    "vaccine",
    "virus",
)
MATERIALS_STUDY_TERMS = (
    "alloy",
    "battery material",
    "catalyst",
    "chemical",
    "chemistry",
    "compound",
    "crystal",
    "graphene",
    "lithium",
    "lunar material",
    "lunar sample",
    "material science",
    "materials research",
    "materials science",
    "mineral",
    "molecule",
    "moon rock",
    "nanomaterial",
    "nanoparticle",
    "oxide",
    "perovskite",
    "polymer",
    "regolith",
    "semiconductor",
    "superconductor",
    "wafer",
)
STUDY_STAGE_EXCLUDED_DOMAIN_TERMS = (
    "aerodynamic",
    "aeronautical",
    "astronomy",
    "astrophysics",
    "backend code",
    "black hole",
    "climate",
    "code generation",
    "cosmic",
    "earth science",
    "fossil",
    "general engineering",
    "geology",
    "geoscience",
    "llm agent",
    "marine heatwave",
    "neutrino",
    "ocean warming",
    "paleontology",
    "sea level",
    "software",
    "space mission",
    "wormhole",
)
STUDY_STAGE_EXCLUSION_OVERRIDE_TERMS = (
    "cancer",
    "clinical trial",
    "disease",
    "drug",
    "human trial",
    "infection",
    "medical",
    "medicine",
    "patient",
    "therapy",
    "treatment",
    "vaccine",
    "virus",
)


class JsonGenerator(Protocol):
    model: str

    def generate_json(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_schema: dict[str, Any],
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
    publish_date_estimated: bool = False
    url: str | None = None
    canonical_url: str | None = None


def digest_once(
    *,
    range_start: str | None = None,
    range_end: str | None = None,
    limit: int | None = None,
    concurrency: int | None = None,
    force: bool = False,
    client: JsonGenerator | None = None,
    review_client: JsonGenerator | None = None,
    progress: Callable[[str], None] | None = None,
    acquire_lock: bool = True,
    max_article_rowid: int | None = None,
) -> dict[str, Any]:
    if (range_start is None) != (range_end is None):
        raise ValueError("range_start and range_end must be provided together or both omitted")
    config = load_pipeline_config()
    if range_start is None:
        from datetime import time

        from pipeline.util import utc_now

        ref = utc_now()
        today_start = datetime.combine(ref.date(), time.min, tzinfo=ref.tzinfo)
        lookback_days = max(1, int(config.retention.get("staging_article_days", 1)))
        range_start = isoformat_z(today_start - timedelta(days=lookback_days))
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    selected_concurrency = int(concurrency or config.digest.get("concurrency", 10))
    content_char_limit = int(config.digest.get("content_char_limit", DEFAULT_CONTENT_CHAR_LIMIT))
    review_enabled = bool(config.digest.get("filter_review_enabled", True))
    review_margin = float(config.digest.get("filter_review_margin", 0.10))
    min_category_impact = float(config.aggregation.get("min_category_impact", 0.25))
    lock_timeout = timedelta(minutes=int(config.pipeline.get("watchdog_timeout_minutes", 30)))
    run_id = f"article-digest-{uuid.uuid4().hex}"
    owns_generator = client is None
    generator = client or create_gemini_client("bulk")
    owns_review_generator = review_client is None and client is None and review_enabled
    reviewer = review_client
    if reviewer is None and client is None and review_enabled:
        reviewer = create_gemini_client("review")
    state = StateDB()
    stats: dict[str, Any] = {"run_id": run_id}
    try:
        lock_context = PipelineLock(LOCK_PATH, lock_timeout, run_id=run_id) if acquire_lock else nullcontext()
        with lock_context:
            state.start_run(run_id, "article_digest")
            status = "success"
            try:
                stats.update(
                    digest_articles_for_aggregation(
                        state=state,
                        run_id=run_id,
                        published_after=range_start,
                        published_before=range_end,
                        limit=limit,
                        concurrency=selected_concurrency,
                        content_char_limit=content_char_limit,
                        force=force,
                        client=generator,
                        review_client=reviewer,
                        min_category_impact=min_category_impact,
                        review_margin=review_margin,
                        progress=progress,
                        max_article_rowid=max_article_rowid,
                    )
                )
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
        if owns_review_generator and reviewer is not None:
            close = getattr(reviewer, "close", None)
            if callable(close):
                close()
        if owns_generator:
            close = getattr(generator, "close", None)
            if callable(close):
                close()


def digest_articles_for_aggregation(
    *,
    state: StateDB,
    run_id: str,
    published_after: str | None = None,
    published_before: str | None = None,
    limit: int | None = None,
    concurrency: int = 8,
    content_char_limit: int = DEFAULT_CONTENT_CHAR_LIMIT,
    force: bool = False,
    client: JsonGenerator | None = None,
    review_client: JsonGenerator | None = None,
    min_category_impact: float = 0.25,
    review_margin: float = 0.10,
    progress: Callable[[str], None] | None = None,
    max_article_rowid: int | None = None,
) -> dict[str, Any]:
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    generator = client or create_gemini_client("bulk")
    max_retries = int(load_pipeline_config().pipeline.get("max_item_retries", 3))
    stats: dict[str, Any] = {
        "candidates": 0,
        "completed": 0,
        "skipped": 0,
        "failed": 0,
        "existing_digest": 0,
        "forced": force,
        "reprints_copied_persisted": 0,
        "reprints_copied_in_batch": 0,
        "reviewed": 0,
        "review_rescued": 0,
        "review_dropped": 0,
        "bulk_model": generator.model,
        "review_model": review_client.model if review_client is not None else None,
        "max_article_rowid": max_article_rowid,
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
        force=force,
        max_article_rowid=max_article_rowid,
    )
    retry_counts = _digest_retry_counts(state, [row["article_id"] for row in rows])

    for row in rows:
        fp_key = _fingerprint_key(row)
        existing_digest = None
        if fp_key and not force:
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
            force=force,
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
        progress(f"article digest: {len(candidates)} article(s) need digests, concurrency={concurrency}")
    if not candidates:
        return stats

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_article = {
            executor.submit(
                generate_article_digest_with_review,
                article,
                client=generator,
                review_client=review_client,
                content_char_limit=content_char_limit,
                min_category_impact=min_category_impact,
                review_margin=review_margin,
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
                )

                if result.get("review"):
                    stats["reviewed"] += 1
                    first_score = float(result["review"]["first_pass_category_impact"])
                    final_score = float(result["digest"]["impact"]["category"])
                    if first_score < min_category_impact <= final_score:
                        stats["review_rescued"] += 1
                    elif final_score < min_category_impact <= first_score:
                        stats["review_dropped"] += 1

                # Propagate digest to any deferred reprints in this batch
                duplicates = canonical_to_duplicates.get(article.article_id)
                if duplicates:
                    generated_digest = {
                        "summary": result["digest"]["summary"],
                        "key_facts": result["digest"]["key_facts"],
                        "content_quality": result["digest"]["content_quality"],
                        "impact": result["digest"]["impact"],
                        "generated_at": isoformat_z(),
                        "model": result["model"],
                        "prompt_version": ARTICLE_DIGEST_PROMPT_VERSION,
                        "content_chars_used": result["content_chars_used"],
                    }
                    if "study_stage" in result["digest"]:
                        generated_digest["study_stage"] = result["digest"]["study_stage"]
                    if result.get("review"):
                        generated_digest["review"] = result["review"]
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
                            progress(f"article digest: copied generated digest to reprint {dup_id[:12]}")

                for usage_record in result.get("usage_records", []):
                    usage = usage_record.get("usage") or {}
                    for key in stats["usage"]:
                        stats["usage"][key] += int(usage.get(key) or 0)
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
    )
    digest = _drop_irrelevant_study_stage(validate_digest_response(result.payload), article)
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
        "model": client.model,
        "usage_records": [
            {
                "stage": "article_digest",
                "model": client.model,
                "prompt_version": ARTICLE_DIGEST_PROMPT_VERSION,
                "usage": result.usage,
            }
        ],
    }


def generate_article_digest_with_review(
    article: ArticleForDigest,
    *,
    client: JsonGenerator,
    review_client: JsonGenerator | None,
    content_char_limit: int = DEFAULT_CONTENT_CHAR_LIMIT,
    min_category_impact: float = 0.25,
    review_margin: float = 0.10,
) -> dict[str, Any]:
    first_pass = generate_article_digest(
        article,
        client=client,
        content_char_limit=content_char_limit,
    )
    if review_client is None:
        return first_pass
    review_reason = article_filter_review_reason(
        first_pass["digest"],
        min_category_impact=min_category_impact,
        review_margin=review_margin,
    )
    if review_reason is None:
        return first_pass

    result = review_client.generate_json(
        system_instruction=(
            "You are the senior review editor for a news aggregation pipeline. "
            "Independently verify the article digest and especially whether the article "
            "should survive a category-impact filter. Use only the supplied article text."
        ),
        prompt=(
            _build_digest_prompt(article, content_char_limit=content_char_limit)
            + "\n\nA lower-cost model produced this first-pass digest:\n"
            + json.dumps(first_pass["digest"], ensure_ascii=False, indent=2)
            + f"\n\nReview trigger: {review_reason}. Return a corrected digest using the same schema. "
            "Do not preserve the first-pass scores merely for consistency. For this final filtering decision, "
            "category impact means importance within one of the site's configured categories: world, US, "
            "politics, business, technology, science, health, environment, automotive, or entertainment. "
            "There is no sports category: routine games, standings, tournament live blogs, athlete profiles, "
            "and ordinary sports results must remain below the 0.25 category threshold unless the article has "
            "unusually broad non-sports public impact. Local human-interest stories should also remain below "
            "the threshold unless they reveal a broader consequential development."
        ),
        response_schema=_digest_response_schema(),
    )
    reviewed_digest = _drop_irrelevant_study_stage(
        validate_digest_response(result.payload), article
    )
    return {
        **first_pass,
        "digest": reviewed_digest,
        "elapsed_ms": int(first_pass["elapsed_ms"]) + int(result.elapsed_ms),
        "usage": result.usage,
        "model": result.model,
        "usage_records": [
            *first_pass["usage_records"],
            {
                "stage": "article_filter_review",
                "model": result.model,
                "prompt_version": ARTICLE_FILTER_REVIEW_PROMPT_VERSION,
                "usage": result.usage,
            },
        ],
        "review": {
            "reason": review_reason,
            "first_pass_model": client.model,
            "review_model": result.model,
            "prompt_version": ARTICLE_FILTER_REVIEW_PROMPT_VERSION,
            "first_pass_content_quality": first_pass["digest"]["content_quality"],
            "first_pass_category_impact": first_pass["digest"]["impact"]["category"],
            "first_pass_global_impact": first_pass["digest"]["impact"]["global"],
        },
    }


def article_filter_review_reason(
    digest: dict[str, Any],
    *,
    min_category_impact: float,
    review_margin: float,
) -> str | None:
    impact = digest.get("impact")
    if not isinstance(impact, dict):
        return "missing impact metadata"
    category_score = impact.get("category")
    if isinstance(category_score, int | float) and not isinstance(category_score, bool):
        lower = min_category_impact - max(0.0, review_margin)
        upper = min_category_impact + max(0.0, review_margin)
        if lower <= float(category_score) <= upper:
            return (
                f"category impact {float(category_score):.3f} is within "
                f"{review_margin:.3f} of the {min_category_impact:.3f} filter threshold"
            )
    rationale_codes = {
        str(code) for code in impact.get("rationale_codes", []) if str(code).strip()
    }
    quality = digest.get("content_quality")
    if quality != "ok" and rationale_codes & HIGH_IMPACT_RATIONALE_CODES:
        return (
            f"content quality {quality!r} conflicts with high-impact rationale codes "
            f"{sorted(rationale_codes & HIGH_IMPACT_RATIONALE_CODES)}"
        )
    return None


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
    impact = _apply_impact_caps(impact, content_quality=quality)
    study_stage = _normalize_study_stage(payload.get("study_stage"))
    result: dict[str, Any] = {
        "summary": _normalize_text(summary),
        "key_facts": clean_facts,
        "content_quality": quality,
        "impact": impact,
    }
    if study_stage is not None:
        result["study_stage"] = study_stage
    return result


def _digest_candidate_rows(
    *,
    state: StateDB,
    published_after: str | None,
    published_before: str | None,
    limit: int | None,
    force: bool = False,
    max_article_rowid: int | None = None,
) -> list[Any]:
    # Select rows that either (a) are not yet completed/skipped, or
    # (b) were processed at an older prompt version and need refreshing.
    # `digest_prompt_version != ?` evaluates to NULL when the column is NULL
    # (which SQLite treats as false), so the explicit `IS NULL` branch is
    # required to pick up rows that have never been digested.
    params: list[Any] = []
    if force:
        # Force is an operator override: include filtered rows so a recovered
        # article can be regenerated and unfiltered if the new digest is usable.
        where = "1=1"
    else:
        params.append(ARTICLE_DIGEST_PROMPT_VERSION)
        where = (
            "a.is_filtered = 0 AND (a.digest_status NOT IN ('completed', 'skipped') "
            "OR a.digest_prompt_version IS NULL "
            "OR a.digest_prompt_version != ?)"
        )
    if published_after is not None:
        where += " AND a.published_at >= ?"
        params.append(published_after)
    if published_before is not None:
        where += " AND a.published_at < ?"
        params.append(published_before)
    if max_article_rowid is not None:
        where += " AND a.rowid <= ?"
        params.append(max_article_rowid)
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
    force: bool = False,
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
        state.update_article_aggregation_status(
            row["article_id"],
            status="filtered_missing_article_json",
            reason=f"article JSON not found: {article_path}",
        )
        return None, "missing_article_json"
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    url = data.get("url")
    canonical_url = data.get("canonical_url")
    media_page_reason = _media_page_url_reason(url, canonical_url)
    if media_page_reason:
        state.update_article_digest_status(
            row["article_id"],
            status="skipped",
            prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
            error=f"skipped: {media_page_reason}",
        )
        state.update_article_aggregation_status(
            row["article_id"],
            status="filtered_video_or_carousel",
            reason=f"{media_page_reason} detected by URL pattern",
        )
        return None, "media_page_url"

    content_text = _normalize_text(data.get("content_text"))
    stale_page_reason = _stale_estimated_page_reason(data, content_text)
    if stale_page_reason:
        state.update_article_digest_status(
            row["article_id"],
            status="skipped",
            prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
            error=f"skipped: {stale_page_reason}",
        )
        state.update_article_aggregation_status(
            row["article_id"],
            status="filtered_recycled_content",
            reason=f"{stale_page_reason} detected before digest generation",
        )
        return None, "stale_estimated_date_page"

    if not force and retry_count >= max_retries:
        state.update_article_digest_status(
            row["article_id"],
            status="skipped",
            prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
            error=f"skipped: failed {retry_count} times",
        )
        state.update_article_aggregation_status(
            row["article_id"],
            status="filtered_max_retries_exceeded",
            reason=f"skipped: failed {retry_count} times",
        )
        return None, "max_retries_exceeded"

    existing_digest = data.get("llm_digest")
    if (
        not force
        and isinstance(existing_digest, dict)
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
        state.set_article_aggregation_pending_if_unassigned(row["article_id"])
        return None, "already_completed"
    if len(content_text) < 500:
        state.update_article_digest_status(
            row["article_id"],
            status="skipped",
            prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
            error="content_text shorter than 500 chars",
        )
        state.update_article_aggregation_status(
            row["article_id"],
            status="filtered_thin_content",
            reason="content_text shorter than 500 chars",
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
            publish_date_estimated=bool(data.get("publish_date_estimated")),
            url=url if isinstance(url, str) else None,
            canonical_url=canonical_url if isinstance(canonical_url, str) else None,
            article_path=article_path,
            content_text=content_text,
            selection_reason=_summary_quality_reason(row["headline"], summary, content_text),
        ),
        None,
    )


def _media_page_url_reason(*urls: Any) -> str | None:
    for raw_url in urls:
        if not isinstance(raw_url, str) or not raw_url.strip():
            continue
        path = urlparse(raw_url).path.lower()
        segments = [segment for segment in path.split("/") if segment]
        for segment in segments:
            if segment in MEDIA_PAGE_PATH_SEGMENTS:
                return f"media page URL path contains /{segment}/"
        if segments and segments[-1].endswith(MEDIA_PAGE_SLUG_SUFFIXES):
            return "media page URL slug indicates video/gallery content"
    return None


def _stale_estimated_page_reason(data: dict[str, Any], content_text: str) -> str | None:
    if not data.get("publish_date_estimated"):
        return None
    reference_date = _parse_iso_date(data.get("published_at")) or _parse_iso_date(data.get("fetched_at"))
    if reference_date is None:
        return None
    urls = (data.get("url"), data.get("canonical_url"))
    stale_url_date = _first_stale_date(
        [parsed_date for raw_url in urls for parsed_date in _url_path_dates(raw_url)],
        reference_date,
    )
    if stale_url_date is not None:
        return f"stale estimated-date page dated {stale_url_date.isoformat()} in URL path"
    if _has_live_page_path(*urls):
        stale_text_date = _first_stale_date(
            _text_month_dates(content_text[:3000]),
            reference_date,
        )
        if stale_text_date is not None:
            return f"stale estimated-date live page dated {stale_text_date.isoformat()} in article text"
    return None


def _parse_iso_date(value: Any) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None


def _first_stale_date(dates: list[date], reference_date: date) -> date | None:
    stale_dates = [
        parsed_date for parsed_date in dates if (reference_date - parsed_date).days > STALE_ESTIMATED_PAGE_DAYS
    ]
    return min(stale_dates) if stale_dates else None


def _url_path_dates(raw_url: Any) -> list[date]:
    if not isinstance(raw_url, str) or not raw_url.strip():
        return []
    path = urlparse(raw_url).path.lower()
    parsed_dates: list[date] = []
    for match in URL_NUMERIC_DATE_RE.finditer(path):
        parsed_date = _safe_date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        if parsed_date is not None:
            parsed_dates.append(parsed_date)
    for match in URL_MONTH_DATE_RE.finditer(path):
        month = MONTH_NUMBERS[match.group(2).lower()]
        parsed_date = _safe_date(int(match.group(1)), month, int(match.group(3)))
        if parsed_date is not None:
            parsed_dates.append(parsed_date)
    for match in URL_SHORT_DATE_RE.finditer(path):
        parsed_date = _safe_date(2000 + int(match.group(3)), int(match.group(1)), int(match.group(2)))
        if parsed_date is not None:
            parsed_dates.append(parsed_date)
    return parsed_dates


def _text_month_dates(text: str) -> list[date]:
    parsed_dates: list[date] = []
    for match in TEXT_MONTH_DATE_RE.finditer(text):
        month = MONTH_NUMBERS[match.group(1).lower()]
        parsed_date = _safe_date(int(match.group(3)), month, int(match.group(2)))
        if parsed_date is not None:
            parsed_dates.append(parsed_date)
    return parsed_dates


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _has_live_page_path(*urls: Any) -> bool:
    for raw_url in urls:
        if not isinstance(raw_url, str) or not raw_url.strip():
            continue
        segments = [segment for segment in urlparse(raw_url).path.lower().split("/") if segment]
        if any(segment in LIVE_PAGE_PATH_SEGMENTS for segment in segments):
            return True
    return False


def _persist_pipeline_digest_result(
    result: dict[str, Any],
    *,
    state: StateDB,
    run_id: str,
) -> None:
    article_id = result["article_id"]
    generated_at = isoformat_z()
    article_path = PROJECT_ROOT / result["article_path"]
    with article_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    digest_payload = {
        "summary": result["digest"]["summary"],
        "key_facts": result["digest"]["key_facts"],
        "content_quality": result["digest"]["content_quality"],
        "impact": result["digest"]["impact"],
        "generated_at": generated_at,
        "model": result["model"],
        "prompt_version": ARTICLE_DIGEST_PROMPT_VERSION,
        "content_chars_used": result["content_chars_used"],
    }
    if "study_stage" in result["digest"]:
        digest_payload["study_stage"] = result["digest"]["study_stage"]
    if result.get("review"):
        digest_payload["review"] = result["review"]
    data["llm_digest"] = digest_payload
    atomic_write_json(article_path, data)
    state.update_article_digest_status(
        article_id,
        status="completed",
        generated_at=generated_at,
        model=result["model"],
        prompt_version=ARTICLE_DIGEST_PROMPT_VERSION,
    )
    state.set_article_aggregation_pending_if_unassigned(article_id)
    for usage_record in result.get("usage_records", []):
        usage = usage_record.get("usage") or {}
        state.record_llm_usage(
            run_id=run_id,
            stage=usage_record["stage"],
            model=usage_record["model"],
            prompt_version=usage_record["prompt_version"],
            input_tokens=usage.get("promptTokenCount"),
            output_tokens=usage.get("candidatesTokenCount"),
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
        "url": article.url,
        "canonical_url": article.canonical_url,
        "published_at": article.published_at,
        "publish_date_estimated": article.publish_date_estimated,
        "headline": article.headline,
        "site_summary": _normalize_text(article.summary),
        "article_text": content,
    }
    return (
        "Create a factual digest of this article for later story grouping and editorial summarization.\n"
        "The digest should be as short as the article allows, but include the important factual details needed "
        "to understand what happened, who is involved, where/when it happened, key numbers, stated causes, "
        "consequences, uncertainty, and next steps. Preserve uncertainty: do not imply causation, confirmed dates, "
        "confirmed injuries, or confirmed outcomes unless the article states them. Do not recreate the full article. "
        "If a complex article needs more than two sentences to preserve the facts, use more sentences.\n"
        "Grounding rules: do not add names, titles, dates, causes, or context from outside the supplied article_text, "
        "even if widely known. If an article uses an unnamed role, keep it unnamed. Do not strengthen the source's "
        "language; for example, if the source says 'doctors received the wounded' do not write 'hospitals declared "
        "a medical emergency.' Do not assert formal designations (e.g. 'PHEIC', 'state of emergency', 'pandemic "
        "declaration', 'recession') unless those exact terms appear in the article text. The published_at value "
        "supplied above is metadata only; do not insert its year, month, or day into the summary or key_facts unless "
        "the article text itself states that date. The url, canonical_url, and publish_date_estimated values are "
        "metadata for freshness and quality checks, not facts to summarize.\n"
        "Write key_facts as standalone, source-grounded editorial notes: each fact should be specific enough to "
        "support a later user-visible summary without re-reading the full article. Prefer facts that name the main "
        "actors, action, place, timing, numbers, consequences, uncertainty, official responses, and next steps. "
        "For noisy or thin pages, key_facts should say what is actually supported and avoid padding with boilerplate.\n"
        "Do not include quotes unless the quote itself is the news. Do not copy boilerplate, newsletter text, "
        "ad copy, navigation, captions repeated as story text, or unrelated page chrome. Use neutral wording.\n"
        "content_quality: choose 'ok' only when the extracted text reads as a clean article body. Choose 'paywalled' "
        "when the body cuts off after a lede with subscription/login chrome (e.g. 'Read full article', 'Subscribe to "
        "continue', 'Comments', 'Democracy Dies in Darkness'). Choose 'thin' only when the original article itself "
        "is genuinely short (under ~1000 chars of narrative after stripping boilerplate). Choose 'extraction_noise' "
        "when sidebars, transcripts, navigation, or unrelated material dominate the extracted text. Choose 'non_news' "
        "for promotional, affiliate, advertorial, archival/index, puzzle/game help, gambling picks, product "
        "recommendation, or non-journalistic content (e.g. personal blog posts). When content_quality is non_news for "
        "a promotional or advertorial reason, the summary's first clause must explicitly label it (e.g. 'This is a "
        "promotional credit-card review' or 'This is an advertorial...'), not describe it as a guide or review. "
        "Video pages, media transcripts, photo galleries, profile/background pages, and pages whose article_text is "
        "mostly captions, navigation, or teaser links should be 'extraction_noise' unless they are clearly a clean "
        "current news article body. Use 'non_news' for pure shopping/deals pages and non-journalistic pages.\n"
        "Score this article's impact from the article text. global and category MUST be decimal numbers from 0.0 "
        "through 1.0 only, never percentages and never a 1-10 scale. Example: use 0.82, not 82 or 8.2. global is "
        "impact for a broad general-news homepage; category is impact within the article's strongest vertical. "
        "Impact must reflect both the importance of the topic and whether the extracted article is usable news "
        "content. If content_quality is non_news or the article is promotional/affiliate/deals/archive-index/"
        "puzzle/gambling/product-recommendation content, both impact scores must be low. Local human-interest, "
        "community-aid, conservation/restoration completions, charity drives, and similar feel-good stories must be "
        "novelty='low_signal' and scope at most 'regional' unless they involve fatalities, ongoing emergencies, or "
        "policy change. Category impact is impact within the article's own vertical: a legitimate automotive, "
        "technology, science, health, business, entertainment, or local-news article may have low global impact but "
        "medium or high category impact if it is important to that audience. Do not mark independent vehicle, device, "
        "film, music, game, or restaurant reviews as product_recommendation unless the article is shopping advice, "
        "affiliate/deals content, or advertorial. For vendor product launches, keynote announcements, and rewrites of "
        "corporate press releases, "
        "emit rationale code 'vendor_announcement'; this caps global impact unless the launch has concrete macro "
        "consequences (regulation, multi-billion market shift, security-critical, public-safety). For injuries "
        "described with hedged language ('apparent', 'appeared to grab', 'seemed to'), emit rationale code "
        "'unconfirmed_injury'. If the article is a live blog, news roundup, or daily-briefing newsletter covering "
        "multiple unrelated topics, emit rationale code 'live_blog' or 'newsletter_roundup' and set content_quality "
        "to 'extraction_noise'. If the article_text contains explicit datelines that conflict with published_at "
        "(e.g. text dated 'January 31, 2024' delivered with a 2026 published_at), emit rationale code "
        "'recycled_content'. If publish_date_estimated is true and the url/canonical_url path or article_text points "
        "to an old gallery, profile, live page, archive, backgrounder, or otherwise stale page rather than a fresh "
        "development, emit 'archival_index', 'profile_or_background', or 'recycled_content', set "
        "novelty='low_signal', and keep both impact scores low. Treat stale estimated-date pages as article-quality "
        "problems even if the topic itself was historically important.\n"
        "scope: reflects the geographic reach of the actor/phenomenon, not the broader topic it touches. A local "
        "official commenting on a national trend is 'local' or 'regional', not 'national'. Stories with named "
        "foreign actors, transnational groups, or cross-border physical effects are 'international'.\n"
        "novelty: use 'breaking' only for time-sensitive events occurring within the last ~48 hours with concrete "
        "consequences (incidents, casualties, outbreaks, official announcements). Use 'update' for new factual "
        "developments on an ongoing story. Use 'analysis' for interview/commentary/feature pieces whose value is "
        "interpretation rather than new facts. A newly published research paper is 'analysis' or 'evergreen', never "
        "'breaking', even if it reports a landmark or first finding. Use 'low_signal' for thin extractions, "
        "video-only pages, media transcripts, galleries, navigation/index pages, profile/background pages, and "
        "feel-good local stories without time-sensitive public impact.\n"
        "rationale_codes: pick all that apply from this controlled vocabulary, written exactly as shown: "
        "affiliate_content, affiliate_deals, archival_index, gambling_content, low_public_interest, "
        "product_recommendation, promotional_content, puzzle_guide, vendor_announcement, live_blog, "
        "newsletter_roundup, recycled_content, unconfirmed_injury, video_page, gallery_page, media_transcript, "
        "profile_or_background, consumer_review, election, critical_infrastructure, "
        "economic_impact, emergency_response, geopolitical_escalation, geopolitical_tension, government_security, "
        "major_policy, national_security, public_health, public_safety, security_incident. You may add at most one "
        "descriptive code outside this list only if none of the above fit. "
        "Use public_health only when the article concerns disease threats, treatments, outbreaks, or health-system "
        "stresses affecting populations; do not use it for personal-health backstory in a profile, "
        "general environment/climate items, or basic biology research with no direct population-health claim. "
        "Use public_safety only for direct physical/health harm to people; do not use it for investor risk, market "
        "stress, or infrastructure-availability narratives.\n"
        "study_stage: if the article reports on medical, biological, pharmaceutical, or materials/lab research, "
        "set study_stage to one of: preclinical, animal, early_human, trial_phase, approved, observational, "
        "lab_bench, unknown. Use observational for retrospective, social-media, or chart-review studies; lab_bench "
        "for in-vitro or pure materials work. Omit study_stage (or use not_applicable) for non-research articles and "
        "for climate, astronomy, space, aeronautics, software, paleontology, engineering, and general earth-science "
        "research. Do not infer stage; if a covered research article does not say, use unknown.\n"
        "Return JSON only.\n\n"
        f"Article:\n{json.dumps(rows, ensure_ascii=False, separators=(',', ':'))}"
    )


VALID_STUDY_STAGES = (
    "preclinical",
    "animal",
    "early_human",
    "trial_phase",
    "approved",
    "observational",
    "lab_bench",
    "not_applicable",
    "unknown",
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
            "study_stage": {
                "type": "STRING",
                "enum": list(VALID_STUDY_STAGES),
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


def _apply_impact_caps(impact: dict[str, Any], *, content_quality: str) -> dict[str, Any]:
    rationale_codes = set(impact.get("rationale_codes", []))
    has_high_rationale = bool(rationale_codes & HIGH_IMPACT_RATIONALE_CODES)
    global_caps: list[float] = []
    category_caps: list[float] = []

    if content_quality == "non_news":
        global_caps.append(NON_NEWS_IMPACT_CAP)
        category_caps.append(NON_NEWS_IMPACT_CAP)
    if rationale_codes & LOW_IMPACT_RATIONALE_CODES:
        global_caps.append(PROMOTIONAL_IMPACT_CAP)
        category_caps.append(PROMOTIONAL_IMPACT_CAP)
    if rationale_codes & MULTI_TOPIC_RATIONALE_CODES:
        global_caps.append(MULTI_TOPIC_IMPACT_CAP)
        category_caps.append(MULTI_TOPIC_IMPACT_CAP)
    if rationale_codes & VENDOR_ANNOUNCEMENT_RATIONALE_CODES and not has_high_rationale:
        global_caps.append(VENDOR_ANNOUNCEMENT_GLOBAL_CAP)
        category_caps.append(VENDOR_ANNOUNCEMENT_CATEGORY_CAP)
    if rationale_codes & UNCONFIRMED_INJURY_RATIONALE_CODES:
        global_caps.append(UNCONFIRMED_INJURY_GLOBAL_CAP)
    if content_quality in {"thin", "extraction_noise", "paywalled"} and not has_high_rationale:
        global_caps.append(NOISY_CONTENT_IMPACT_CAP)
        category_caps.append(NOISY_CONTENT_IMPACT_CAP)

    if not global_caps and not category_caps:
        return impact

    new_global = float(impact["global"])
    new_category = float(impact["category"])
    if global_caps:
        new_global = min(new_global, min(global_caps))
    if category_caps:
        new_category = min(new_category, min(category_caps))

    capped = new_global < float(impact["global"]) - 1e-9 or new_category < float(impact["category"]) - 1e-9
    new_codes = sorted(rationale_codes | {"impact_capped"}) if capped else sorted(rationale_codes)
    return {
        **impact,
        "global": round(new_global, 3),
        "category": round(new_category, 3),
        "rationale_codes": new_codes,
    }


def _normalize_score(value: Any, field: str) -> float:
    if not isinstance(value, int | float):
        raise ValueError(f"digest {field} must be numeric")
    score = float(value)
    if score < 0:
        raise ValueError(f"digest {field} must be non-negative")
    if score > 1:
        # Gemini occasionally returns an otherwise sensible 1-10 or percentage
        # score despite the prompt/schema requesting 0.0-1.0. Normalize those
        # common scales so one malformed score does not waste the whole call.
        if score <= 10:
            score /= 10
        elif score <= 100:
            score /= 100
        else:
            raise ValueError(f"digest {field} must be between 0 and 1")
    return round(score, 3)


def _normalize_study_stage(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    cleaned = value.strip().lower().replace("-", "_").replace(" ", "_")
    if not cleaned or cleaned == "not_applicable":
        return None
    if cleaned not in VALID_STUDY_STAGES:
        return None
    return cleaned


def _drop_irrelevant_study_stage(
    digest: dict[str, Any],
    article: ArticleForDigest,
) -> dict[str, Any]:
    if "study_stage" not in digest:
        return digest
    context = _study_stage_context(article)
    has_biomedical_context = _context_contains_any(context, BIOMEDICAL_STUDY_TERMS)
    has_materials_context = _context_contains_any(context, MATERIALS_STUDY_TERMS)
    if has_materials_context:
        return digest
    if has_biomedical_context and (
        not _context_contains_any(context, STUDY_STAGE_EXCLUDED_DOMAIN_TERMS)
        or _context_contains_any(context, STUDY_STAGE_EXCLUSION_OVERRIDE_TERMS)
    ):
        return digest
    cleaned = dict(digest)
    cleaned.pop("study_stage", None)
    return cleaned


def _study_stage_context(article: ArticleForDigest) -> str:
    text = _normalize_text(
        " ".join(
            str(value)
            for value in (
                article.source_id,
                article.source_name,
                article.headline,
                article.summary,
                article.url,
                article.canonical_url,
                article.content_text[:4000],
            )
            if value
        )
    ).lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _context_contains_any(context: str, terms: tuple[str, ...]) -> bool:
    for term in terms:
        clean_term = re.sub(r"[^a-z0-9]+", " ", term.lower()).strip()
        if not clean_term:
            continue
        escaped_term = re.escape(clean_term).replace(r"\ ", r"\s+")
        pattern = rf"(?<![a-z0-9]){escaped_term}(?![a-z0-9])"
        if re.search(pattern, context):
            return True
    return False


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
            WHERE f.{column} = ?
              AND a.digest_status = 'completed'
              AND a.digest_prompt_version = ?
              AND a.is_filtered = 0
            LIMIT 1
            """,
            (value, ARTICLE_DIGEST_PROMPT_VERSION),
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
        if (
            isinstance(digest, dict)
            and digest.get("prompt_version") == ARTICLE_DIGEST_PROMPT_VERSION
            and digest.get("summary")
            and digest.get("key_facts")
        ):
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
    state.set_article_aggregation_pending_if_unassigned(target_article_id)
