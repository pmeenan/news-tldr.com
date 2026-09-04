"""Generate a private evaluation report for source-grounding and neutrality review."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Permit the documented direct invocation from any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.editorial import EditorialArticle, EditorialEvent, build_story_payload, generate_story  # noqa: E402
from pipeline.llm import create_gemini_client  # noqa: E402
from pipeline.util import isoformat_z  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="List fixtures without network calls")
    parser.add_argument("--output", type=Path, default=Path("data/evaluations/editorial.json"))
    args = parser.parse_args()
    fixture = Path(__file__).resolve().parents[1] / "tests/fixtures/editorial-evaluation.json"
    data = json.loads(fixture.read_text())
    if args.dry_run:
        print(json.dumps({"stories": [case["id"] for case in data["stories"]],
                          "partitions": data["partitions"], "network_calls": 0}))
        return 0
    client = create_gemini_client("review")
    rows = []
    try:
        for index, case in enumerate(data["stories"], 1):
            if args.verbose:
                print(f"evaluation: {index}/{len(data['stories'])} {case['id']}", file=sys.stderr, flush=True)
            now = isoformat_z()
            articles = tuple(EditorialArticle(
                article_id=f"fixture-{case['id']}-{i}", source_id=f"fixture-{i}", source_name=f"Fixture publisher {i}",
                headline=case["title"], url=f"https://example.test/{case['id']}/{i}", published_at=now,
                content=text, digest_summary=None, digest_key_facts=(), bias_label="unknown", reliability="unknown",
            ) for i, text in enumerate(case["reports"]))
            event = EditorialEvent(event_id=case["id"], title=case["title"], category=case["category"], thread=None,
                                   status="active", created_at=now, updated_at=now,
                                   newsworthiness={"global": 0.5, "category": 0.5}, articles=articles)
            row = {"id": case["id"], "expected": case["expected"], "input": case["reports"],
                   "human_review": {"unsupported_claims": None, "missing_qualifications": None,
                                    "misleading_headline": None, "notes": ""}}
            try:
                generated = generate_story(event, client=client)
                row["story"] = build_story_payload(event, generated, generated_at=now)
                row["automatic_validation"] = "passed"
                row["usage"] = generated["usage"]
            except Exception as exc:
                row["automatic_validation"] = "failed"
                row["error"] = str(exc)
            rows.append(row)
    finally:
        client.close()
    report = {"generated_at": isoformat_z(), "cases": rows, "partition_review_cases": data["partitions"],
              "human_review_complete": False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    failed = sum(row["automatic_validation"] != "passed" for row in rows)
    print(json.dumps({"output": str(args.output), "cases": len(rows), "failed": failed,
                      "human_review_complete": False}))
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
