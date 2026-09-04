from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipeline.evidence import validate_claim_links, validate_evidence
from pipeline.sources import publisher_id, reporting_origin


def test_evidence_rejects_fabricated_passages_and_unknown_sources() -> None:
    event = SimpleNamespace(
        articles=[SimpleNamespace(article_id="a1", content="Officials reported 12 cases. Investigation continues.")]
    )
    claim = {
        "text": "Officials reported 12 cases.",
        "status": "attributed",
        "evidence": [{"article_id": "a1", "quote": "Officials reported 12 cases."}],
    }
    ledger = validate_evidence({"claims": [claim]}, event)
    assert ledger[0]["claim_id"] == "c1"
    claim["evidence"][0]["quote"] = "Officials confirmed 120 cases."
    with pytest.raises(ValueError, match="not present"):
        validate_evidence({"claims": [claim]}, event)
    claim["evidence"][0] = {"article_id": "invented", "quote": "Officials reported 12 cases."}
    with pytest.raises(ValueError, match="source or quote"):
        validate_evidence({"claims": [claim]}, event)


def test_every_summary_bullet_must_link_to_evidence() -> None:
    payload = {
        "headline_claim_ids": ["c1"],
        "dek_claim_ids": ["c1"],
        "tldr": ["First", "Second"],
        "briefing": ["First", "Qualification"],
        "tldr_claim_ids": [["c1"], ["c1"]],
        "briefing_claim_ids": [["c1"], ["c1"]],
    }
    validate_claim_links(payload, [{"claim_id": "c1"}])
    payload["briefing_claim_ids"][1] = ["fabricated"]
    with pytest.raises(ValueError, match="valid evidence claims"):
        validate_claim_links(payload, [{"claim_id": "c1"}])


def test_publisher_identity_does_not_count_feeds_as_corroboration() -> None:
    assert publisher_id({"source_name": "Fox News"}) == publisher_id({"source_name": "Fox News - Politics"})
    assert publisher_id({"source_name": "ABC News - Business"}) == publisher_id(
        {"source_name": "ABC News - Top Stories"}
    )
    assert reporting_origin("WASHINGTON (AP) — A report follows.", "abc-news") == "associated-press"
    assert reporting_origin("An outlet cited another outlet without a byline.", "abc-news") is None
