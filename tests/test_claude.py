from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import httpx
import pytest

from pipeline.claude import _CIRCUITS, ClaudeRoutingClient
from pipeline.free_routing import NEMOTRON, FreeFirstClient
from pipeline.llm import GeminiResult, create_llm_client, generate_validated_json, llm_client_for_role

SETTINGS = {
    "enabled": True, "model": "claude-haiku-5-5",
    "roles": {"digest": "high", "filter": "medium", "draft": "high", "top_news": "medium"},
    "prices": {"input": 0.1, "output": 0.5, "long_context_multiplier": 5},
}
REQUEST = {
    "system_instruction": "test", "prompt": "test",
    "response_schema": {"type": "OBJECT", "properties": {"n": {"type": "INTEGER", "minimum": 1}},
                        "required": ["n"]},
    "max_output_tokens": 2000, "thinking_level": "low",
}


class Fallback:
    model = "gemini-existing"

    def __init__(self, result=None, error=None):
        self.calls = 0
        self.result = result or GeminiResult({"n": 2}, self.model, 1, {"costUsd": 0.01})
        self.error = error

    def generate_json(self, **kwargs):
        self.calls += 1
        if self.error:
            raise self.error
        return self.result

    def close(self):
        pass


@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    _CIRCUITS.clear()
    monkeypatch.setenv("CLAUDE_API_KEY", "test-key")
    monkeypatch.delenv("CLAUDE_DISABLED", raising=False)
    monkeypatch.setattr("pipeline.llm.load_dotenv", lambda: None)
    yield
    _CIRCUITS.clear()


def response(**changes):
    return {"model": SETTINGS["model"], "stop_reason": "end_turn",
            "content": [{"type": "text", "text": '{"n": 1}'}],
            "usage": {"input_tokens": 100, "output_tokens": 40,
                      "output_tokens_details": {"thinking_tokens": 30}}, **changes}


def client(handler, fallback=None):
    return ClaudeRoutingClient(fallback or Fallback(), copy.deepcopy(SETTINGS),
                               transport=httpx.MockTransport(handler))


@pytest.mark.parametrize(('role', 'effort'), [('digest', 'high'), ('filter', 'medium'), ('draft', 'high')])
def test_request_and_thinking_accounting(role, effort):
    def handle(req):
        body = json.loads(req.content)
        assert body['output_config']['effort'] == effort
        assert body['max_tokens'] == 16384
        assert 'thinking_level' not in body
        assert 'minimum' not in body['output_config']['format']['schema']['properties']['n']
        return httpx.Response(200, json=response())
    with client(handle) as route:
        result = llm_client_for_role(route, role).generate_json(**REQUEST)
        assert result.model == SETTINGS['model']
        assert result.usage['candidatesTokenCount'] == 10
        assert result.usage['thoughtsTokenCount'] == 30
        assert result.usage['costUsd'] == pytest.approx(0.00003)
        assert route.fallback.calls == 0


@pytest.mark.parametrize('role', ['framing', 'verification', 'evidence', 'grouping', 'coherence', 'prescreen'])
def test_unselected_roles_keep_original_client(role):
    with client(lambda _: pytest.fail('Unexpected Claude call')) as route:
        assert llm_client_for_role(route, role) is route.fallback
        assert route.generate_json(**REQUEST).model == 'gemini-existing'


@pytest.mark.parametrize('status', [400, 401, 402, 403, 429, 500, 529])
def test_provider_failure_and_shared_cooldown(status):
    seen = []
    def handle(req):
        seen.append(req)
        return httpx.Response(status, json={'error': {'message': 'Your credit balance is too low'}},
                              headers={'Retry-After': '7200'})
    with client(handle) as first, client(handle) as second:
        assert first.for_role('digest').generate_json(**REQUEST).model == 'gemini-existing'
        assert second.for_role('filter').generate_json(**REQUEST).model == 'gemini-existing'
        assert len(seen) == 1
        first.circuit.until = 0
        second.for_role('filter').generate_json(**REQUEST)
        assert len(seen) == 2


def test_missing_key(monkeypatch):
    monkeypatch.delenv('CLAUDE_API_KEY')
    with client(lambda _: pytest.fail('Unexpected Claude call')) as route:
        assert route.for_role('digest').generate_json(**REQUEST).model == 'gemini-existing'


@pytest.mark.parametrize('changes', [
    {'stop_reason': 'max_tokens'}, {'model': 'unexpected-model'},
    {'content': [{'type': 'text', 'text': 'not json'}]},
    {'content': [{'type': 'text', 'text': '{"n": 0}'}]},
    {'content': [{'type': 'text', 'text': '{"n": true}'}]},
])
def test_invalid_paid_response_preserves_usage(changes):
    with client(lambda _: httpx.Response(200, json=response(**changes))) as route:
        result = route.for_role('digest').generate_json(**REQUEST)
        assert result.model == 'gemini-existing'
        assert result.usage['routingAttempts'][0]['usage']['costUsd'] == pytest.approx(0.00003)


def test_timeout_falls_back():
    def handle(req):
        raise httpx.ReadTimeout('timed out', request=req)
    with client(handle) as route:
        assert route.for_role('digest').generate_json(**REQUEST).model == 'gemini-existing'


def test_long_context_price_and_cache():
    usage = {'input_tokens': 100001, 'cache_creation_input_tokens': 100, 'cache_read_input_tokens': 100,
             'output_tokens': 100}
    with client(lambda _: httpx.Response(200, json=response(usage=usage))) as route:
        result = route.for_role('digest').generate_json(**REQUEST)
        assert result.usage['costUsd'] == pytest.approx(5 * (10000.1 + 12.5 + 1 + 50) / 1e6)
        assert result.usage['promptTokenCount'] == 100201


def only_two(payload):
    if payload['n'] != 2:
        raise ValueError('domain failure')


def test_domain_failure_through_free_route():
    with client(lambda _: httpx.Response(200, json=response())) as paid:
        route = FreeFirstClient(stage='review', fallback=paid, settings={})
        result = generate_validated_json(route.for_role('digest'), only_two, **REQUEST)
        assert result.payload == {'n': 2}
        assert result.usage['routingAttempts'][0]['model'] == SETTINGS['model']


def test_terminal_failure_preserves_discarded_usage():
    with client(lambda _: httpx.Response(200, json=response(stop_reason='max_tokens')),
                Fallback(error=RuntimeError('Gemini failed'))) as paid:
        route = FreeFirstClient(stage='review', fallback=paid, settings={})
        with pytest.raises(RuntimeError) as caught:
            route.for_role('digest').generate_json(**REQUEST)
        assert caught.value.routing_attempts[0]['model'] == SETTINGS['model']


def test_free_success_precedes_claude():
    free = Fallback(GeminiResult({'n': 2}, f'openrouter/{NEMOTRON}', 1, {'costUsd': 0}))
    policy = SimpleNamespace(_clock=lambda: 0, reserve_attempt=lambda *a, **k: None, release=lambda m: None)
    with client(lambda _: pytest.fail('Unexpected Claude call')) as paid:
        route = FreeFirstClient(stage='bulk', fallback=paid, settings={}, policy=policy, candidates={NEMOTRON: free})
        result = route.for_role('digest').generate_json(**REQUEST)
        assert result.model == f'openrouter/{NEMOTRON}'
        assert free.calls == 1


def test_free_domain_rejection_then_claude_then_gemini():
    free = Fallback(GeminiResult({'n': 1}, f'openrouter/{NEMOTRON}', 1, {'costUsd': 0}))
    policy = SimpleNamespace(_clock=lambda: 0, reserve_attempt=lambda *a, **k: None, release=lambda m: None)
    with client(lambda _: httpx.Response(200, json=response())) as paid:
        route = FreeFirstClient(stage='bulk', fallback=paid, settings={}, policy=policy, candidates={NEMOTRON: free})
        result = generate_validated_json(route.for_role('digest'), only_two, **REQUEST)
        assert result.model == 'gemini-existing'
        attempts = result.usage['routingAttempts']
        assert [a['model'] for a in attempts] == [f'openrouter/{NEMOTRON}', SETTINGS['model']]


@pytest.mark.parametrize('disabled', [False, True])
def test_factory_pins_and_disable(monkeypatch, disabled):
    monkeypatch.setattr('pipeline.config.load_pipeline_config', lambda: SimpleNamespace(llm={'claude': SETTINGS}))
    monkeypatch.setattr('pipeline.llm.create_gemini_client', lambda *a, **k: Fallback())
    monkeypatch.setenv('LLM_BACKEND', 'gemini')
    monkeypatch.delenv('LLM_REVIEW_BACKEND', raising=False)
    if disabled:
        monkeypatch.setenv('CLAUDE_DISABLED', '1')
    implicit = create_llm_client('review')
    assert isinstance(implicit, ClaudeRoutingClient) is not disabled
    implicit.close()
    assert isinstance(create_llm_client('review', backend='gemini'), Fallback)



@pytest.mark.parametrize('raw', [[], {'usage': None}, response(usage={}), response(content=[None])])
def test_malformed_response_falls_back(raw):
    with client(lambda _: httpx.Response(200, json=raw)) as route:
        assert route.for_role('digest').generate_json(**REQUEST).model == 'gemini-existing'


def test_config_disabled(monkeypatch):
    monkeypatch.setattr('pipeline.config.load_pipeline_config',
                        lambda: SimpleNamespace(llm={'claude': {**SETTINGS, 'enabled': False}}))
    monkeypatch.setattr('pipeline.llm.create_gemini_client', lambda *a, **k: Fallback())
    monkeypatch.setenv('LLM_BACKEND', 'gemini')
    monkeypatch.delenv('LLM_REVIEW_BACKEND', raising=False)
    assert isinstance(create_llm_client('review'), Fallback)
