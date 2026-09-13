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


def test_change_summary_must_read_as_news_and_gets_one_verifier_retry() -> None:
    from pipeline.evidence import change_summary_problem, verify_story

    payload = {"briefing": ["Rescuers found two workers alive.", "Repairs continue."], "tldr": []}
    assert change_summary_problem("", payload) is None
    assert "describes the edit" in change_summary_problem("Added sector breakdown details and reactions.", payload)
    assert "describes the edit" in change_summary_problem("Updated to report that a second victim died.", payload)
    assert "repeat" in change_summary_problem("Rescuers found two workers alive", payload)
    assert change_summary_problem("A second victim was identified as a six-week-old infant.", payload) is None

    class Client:
        model = "test"

        def __init__(self) -> None:
            self.calls: list[dict] = []

        def generate_json(self, **kwargs):
            self.calls.append(kwargs)
            summary = (
                "Added details on the second victim." if len(self.calls) == 1
                else "A second victim died in hospital on Thursday."
            )
            return SimpleNamespace(
                payload={"approved": True, "reason": "", "material_update": True, "change_summary": summary},
                model="test", elapsed_ms=1, usage={},
            )

    client = Client()
    review, _result = verify_story(payload, [], {"headline": "Old story"}, client)
    assert review["change_summary"] == "A second victim died in hospital on Thursday."
    assert len(review["retried_results"]) == 1
    assert len(client.calls) == 2
    assert "Correct this problem with your previous review" in client.calls[1]["prompt"]
    assert "ONE reader-facing sentence" in client.calls[0]["prompt"]

    class NonMaterial(Client):
        def generate_json(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                payload={"approved": True, "reason": "", "material_update": False,
                         "change_summary": "Added stray text that must be dropped."},
                model="test", elapsed_ms=1, usage={},
            )

    review, _result = verify_story(payload, [], {"headline": "Old story"}, NonMaterial())
    assert review["change_summary"] == "" and review["retried_results"] == []


def test_evidence_caps_passage_count_and_length() -> None:
    body = ("x" * 400) + " Officials reported 12 cases."
    event = SimpleNamespace(articles=[SimpleNamespace(article_id="a", content=body)])
    claim = {"text": "Twelve cases.", "status": "reported", "evidence": [{"article_id": "a", "quote": "x" * 330}]}
    with pytest.raises(ValueError, match="exceeds 320"):
        validate_evidence({"claims": [claim]}, event)
    claim["evidence"] = [{"article_id": "a", "quote": "Officials reported 12 cases."}] * 4
    with pytest.raises(ValueError, match="1-3 supporting"):
        validate_evidence({"claims": [claim]}, event)
    claim["evidence"] = claim["evidence"][:3]
    assert validate_evidence({"claims": [claim]}, event)[0]["claim_id"] == "c1"


def test_source_passages_preserve_abbreviations_numbers_and_quote_attribution() -> None:
    from pipeline.evidence import source_passages

    text = ('U.S. officials met Dr. Jones and Lt. Smith on Jan. 9 at 3.5 percent. '
            '“No,” she said. A second report followed.')
    passages = source_passages(text)
    assert passages == [
        'U.S. officials met Dr. Jones and Lt. Smith on Jan. 9 at 3.5 percent.',
        '“No,” she said.', 'A second report followed.',
    ]
    assert all(p in text for p in passages)


def test_long_source_passages_cover_text_with_overlap_and_keep_short_tail() -> None:
    from pipeline.evidence import source_passages

    text = ' '.join(f'word{i}' for i in range(180)) + '. End.'
    passages = source_passages(text)
    assert all(8 <= len(p) <= 320 and p in text for p in passages)
    assert set(text.split()) == {word for p in passages for word in p.split()}
    assert 'End.' in passages[-1]
    for first, second in zip(passages, passages[1:]):
        assert set(first.split()) & set(second.split())


def test_passage_ids_resolve_to_exact_sources_not_model_supplied_quotes() -> None:
    from pipeline.evidence import evidence_passages, resolve_evidence_passages

    event = SimpleNamespace(articles=[
        SimpleNamespace(article_id='original-a', source_name='Outlet A', headline='Headline only',
                        published_at='2026-09-13', content='Officials reported 12 cases.\nTesting continues.'),
        SimpleNamespace(article_id='original-b', source_name='Outlet B', headline='Another headline',
                        published_at='2026-09-13', content='Another outlet reported 15 cases.'),
    ])
    articles, lookup = evidence_passages(event)
    assert list(articles[0]['passages']) == ['a0p0', 'a0p1']
    payload = {'claims': [{'text': 'Reports disagree on the count.', 'status': 'disputed',
                           'passage_ids': ['a0p0', 'a1p0'],
                           'evidence': [{'article_id': 'invented', 'quote': 'Invented quotation.'}]}]}
    ledger = resolve_evidence_passages(payload, lookup, event)
    assert ledger[0]['evidence'] == [
        {'article_id': 'original-a', 'quote': 'Officials reported 12 cases.'},
        {'article_id': 'original-b', 'quote': 'Another outlet reported 15 cases.'},
    ]
    assert 'passage_ids' not in ledger[0]
    assert 'Headline only' not in str(lookup)


@pytest.mark.parametrize('ids', [['missing'], ['a0p0', 'a0p0'], [], [True], [{}], ['a0p0'] * 4])
def test_passage_resolution_rejects_invalid_ids(ids) -> None:
    from pipeline.evidence import resolve_evidence_passages

    event = SimpleNamespace(articles=[SimpleNamespace(article_id='a', content='A supported passage.')])
    lookup = {'a0p0': {'article_id': 'a', 'quote': 'A supported passage.'}}
    with pytest.raises(ValueError):
        resolve_evidence_passages({'claims': [{'text': 'Claim.', 'status': 'reported', 'passage_ids': ids}]},
                                  lookup, event)


def test_passage_validation_failure_retains_usage_for_accounting() -> None:
    from pipeline.evidence import EVIDENCE_VERSION, collect_evidence

    result = SimpleNamespace(payload={'claims': [{'text': 'Claim.', 'status': 'reported',
                                                 'passage_ids': ['missing']}]}, usage={'promptTokenCount': 100})
    client = SimpleNamespace(generate_json=lambda **kwargs: result)
    event = SimpleNamespace(title='Event', articles=[SimpleNamespace(
        article_id='a', source_name='Outlet', headline='Headline', published_at=None, content='A supported passage.',
    )])
    with pytest.raises(ValueError) as error:
        collect_evidence(event, client)
    assert error.value.editorial_unrecorded_result == (result, EVIDENCE_VERSION)


def test_related_short_sentences_retain_names_and_custody_context() -> None:
    from pipeline.evidence import source_passages

    text = 'Alex Smith appeared in court charged with violent disorder. He was remanded in custody.'
    assert source_passages(text) == [text]
