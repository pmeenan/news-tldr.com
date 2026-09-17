"""Generate a private evaluation report for source-grounding and neutrality review."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from contextlib import ExitStack
from pathlib import Path
from typing import Any

# Permit the documented direct invocation from any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.editorial import EditorialArticle, EditorialEvent, build_story_payload, generate_story  # noqa: E402
from pipeline.llm import (  # noqa: E402
    GeminiResult,
    create_llm_client,
    estimate_cost_usd,
    reported_cost_usd,
    usage_fields,
)
from pipeline.util import atomic_write_json, isoformat_z  # noqa: E402


class RecordingClient:
    """Capture every returned response, even when subsequent validation fails."""

    def __init__(self, client: Any, role: str, records: list[dict[str, Any]]) -> None:
        self.client = client
        self.model = client.model
        self.role = role
        self.records = records

    def generate_json(self, **kwargs: Any) -> Any:
        try:
            result = self.client.generate_json(**kwargs)
        except Exception as exc:
            if result := getattr(exc, "llm_result", None):
                self.record(result, kwargs, error=str(exc))
            raise
        self.record(result, kwargs)
        return result

    def record(self, result: Any, request: dict[str, Any], *, error: str | None = None) -> None:
        for attempt in result.usage.get("routingAttempts") or []:
            self.record(GeminiResult({}, attempt["model"], 0, attempt["usage"]), request,
                        error="discarded free-routing response")
        fields = usage_fields(result.usage)
        cost = reported_cost_usd(result.usage)
        if cost is None:
            cost = estimate_cost_usd(model=result.model, **fields)
        self.records.append({
            "role": self.role, "model": result.model, "elapsed_ms": result.elapsed_ms,
            **fields, "cost_usd": cost, "error": error,
            "raw_usage": {k: v for k, v in result.usage.items() if k != "routingAttempts"},
            "response": result.payload,
            "request_sha256": hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest(),
        })


def evaluate_case(case: dict[str, Any], clients: dict[str, Any]) -> dict[str, Any]:
    now = isoformat_z()
    articles = tuple(EditorialArticle(
        article_id=f"fixture-{case['id']}-{i}", source_id=f"fixture-{i}", source_name=f"Fixture publisher {i}",
        headline=case["title"], url=f"https://example.test/{case['id']}/{i}", published_at=now,
        content=text, digest_summary=None, digest_key_facts=(), bias_label="unknown", reliability="unknown",
    ) for i, text in enumerate(case["reports"]))
    event = EditorialEvent(event_id=case["id"], title=case["title"], category=case["category"], thread=None,
                           status="active", created_at=now, updated_at=now,
                           newsworthiness={"global": 0.5, "category": 0.5}, articles=articles)
    records: list[dict[str, Any]] = []
    row = {"id": case["id"], "expected": case["expected"], "input": case["reports"],
           "human_review": {"unsupported_claims": None, "missing_qualifications": None,
                            "misleading_headline": None, "notes": ""}, "usage_records": records}
    provenance = []
    try:
        generated = generate_story(
            event, client=RecordingClient(clients["draft"], "draft", records),
            evidence_client=RecordingClient(clients["evidence"], "evidence", records),
            verification_client=RecordingClient(clients["verification"], "verification", records),
        )
        row["story"] = build_story_payload(event, generated, generated_at=now)
        row["automatic_validation"] = "passed"
        row["usage"] = generated["usage"]
        provenance = generated.get("usage_records") or []
    except Exception as exc:
        row["automatic_validation"] = "failed"
        row["error"] = str(exc)
        provenance = getattr(exc, "editorial_usage_records", [])
    for native in provenance:
        for record in records:
            if ("prompt_version" not in record and record["model"] == native["model"]
                    and record["raw_usage"] == {k: v for k, v in native["usage"].items() if k != "routingAttempts"}):
                record["prompt_version"] = native["prompt_version"]
                break
    row["recorded_cost_usd"] = sum(r["cost_usd"] for r in records if r["cost_usd"] is not None)
    row["responses_without_cost"] = sum(r["cost_usd"] is None for r in records)
    return row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="List fixtures without network calls")
    parser.add_argument("--output", type=Path, default=Path("data/evaluations/editorial.json"))
    parser.add_argument("--backend", choices=("gemini", "openrouter", "free-first"),
                        help="Draft backend; default: configured")
    parser.add_argument("--model", action="append", help="Pin a draft model; repeat to compare on the same fixtures")
    parser.add_argument("--evidence-backend", choices=("gemini", "openrouter", "free-first"))
    parser.add_argument("--evidence-model", help="Pin the evidence extractor; otherwise use configured bulk routing")
    parser.add_argument("--verification-backend", choices=("gemini", "openrouter", "free-first"))
    parser.add_argument("--verification-model", help="Pin the verifier; otherwise use configured review routing")
    parser.add_argument("--limit", type=int, help="Maximum fixtures per candidate model")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    fixture = Path(__file__).resolve().parents[1] / "tests/fixtures/editorial-evaluation.json"
    data = json.loads(fixture.read_text())
    cases = data["stories"][:args.limit]
    candidates = args.model or [None]
    selection = {
        "draft_backend": args.backend, "draft_models": candidates,
        "evidence_backend": args.evidence_backend, "evidence_model": args.evidence_model,
        "verification_backend": args.verification_backend, "verification_model": args.verification_model,
    }
    if args.dry_run:
        print(json.dumps({"stories": [case["id"] for case in cases], "selection": selection,
                          "partitions": data["partitions"], "network_calls": 0,
                          "case_count": len(cases) * len(candidates)}))
        return 0
    rows = []
    report = {"generated_at": isoformat_z(), "selection": selection, "cases": rows,
              "partition_review_cases": data["partitions"], "human_review_complete": False, "complete": False,
              "recorded_cost_usd": 0.0, "responses_without_cost": 0}
    atomic_write_json(args.output, report)
    progress = (lambda message: print(message, file=sys.stderr, flush=True)) if args.verbose else None
    for model in candidates:
        with ExitStack() as stack:
            clients = {
                "draft": stack.enter_context(create_llm_client(
                    "review", backend=args.backend, model=model, purpose="editorial", progress=progress,
                )),
                "evidence": stack.enter_context(create_llm_client(
                    "bulk", backend=args.evidence_backend, model=args.evidence_model, purpose="evidence",
                    progress=progress,
                )),
                "verification": stack.enter_context(create_llm_client(
                    "review", backend=args.verification_backend, model=args.verification_model,
                    purpose="editorial", last_resort=True, progress=progress,
                )),
            }
            for index, case in enumerate(cases, 1):
                if progress:
                    progress(f"evaluation: {clients['draft'].model} {index}/{len(cases)} {case['id']}")
                row = evaluate_case(case, clients)
                row["requested_models"] = {role: client.model for role, client in clients.items()}
                row["client_settings"] = {
                    role: {key: getattr(client, key, None) for key in (
                        "models", "labels", "reasoning_effort", "thinking_level", "max_output_tokens",
                        "response_format", "provider_preferences",
                    )}
                    for role, client in clients.items()
                }
                rows.append(row)
                report["recorded_cost_usd"] = sum(item["recorded_cost_usd"] for item in rows)
                report["responses_without_cost"] = sum(item["responses_without_cost"] for item in rows)
                atomic_write_json(args.output, report)
    report["complete"] = True
    atomic_write_json(args.output, report)
    failed = sum(row["automatic_validation"] != "passed" for row in rows)
    print(json.dumps({"output": str(args.output), "cases": len(rows), "failed": failed,
                      "recorded_cost_usd": report["recorded_cost_usd"],
                      "responses_without_cost": report["responses_without_cost"],
                      "human_review_complete": False}))
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
