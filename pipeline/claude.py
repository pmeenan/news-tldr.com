"""Role-scoped Claude requests with the existing paid route as fallback."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx

from pipeline.llm import GeminiResult, generate_validated_json
from pipeline.openrouter import _restore_optional_fields, json_schema, retry_after_seconds


def claude_schema(schema: dict) -> dict:
    value = json_schema(schema)

    def walk(node: object) -> None:
        if isinstance(node, dict):
            constraints = {
                k: node.pop(k)
                for k in ["maxItems", "minItems", "minimum", "maximum", "minLength", "maxLength"]
                if k in node
            }
            if constraints:
                node["description"] = node.get("description", "") + " Constraints: " + json.dumps(constraints)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(value)
    return value


def validate_constraints(value: object, schema: dict) -> None:
    if isinstance(value, list):
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            raise ValueError("Too many array items")
        if "minItems" in schema and len(value) < schema["minItems"]:
            raise ValueError("Too few array items")
        for v in value:
            validate_constraints(v, schema.get("items", {}))
    elif isinstance(value, dict):
        for k, v in value.items():
            validate_constraints(v, schema.get("properties", {}).get(k, {}))
    elif isinstance(value, str):
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            raise ValueError("String too long")
        if "minLength" in schema and len(value) < schema["minLength"]:
            raise ValueError("String too short")
    elif type(value) in [int, float]:
        if "minimum" in schema and value < schema["minimum"]:
            raise ValueError("Below minimum")
        if "maximum" in schema and value > schema["maximum"]:
            raise ValueError("Above maximum")


class Circuit:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.until = 0.0

    def available(self) -> bool:
        with self.lock:
            return time.monotonic() >= self.until

    def fail(self, seconds: float) -> None:
        with self.lock:
            self.until = max(self.until, time.monotonic() + seconds)


_CIRCUITS: dict[str, Circuit] = {}
_CIRCUITS_LOCK = threading.Lock()


def shared_circuit(key: str) -> Circuit:
    fingerprint = hashlib.sha256(key.encode()).hexdigest()
    with _CIRCUITS_LOCK:
        return _CIRCUITS.setdefault(fingerprint, Circuit())


class ClaudeRoutingClient:
    """Unselected roles delegate unchanged; role views share transport and circuit.

    One Claude attempt per request. The circuit is process-wide, so each new
    scheduled process can probe again after credits are replenished.
    """

    def __init__(
        self, fallback: Any, settings: dict[str, Any], *,
        progress: Callable[[str], None] | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.fallback = fallback
        self.settings = settings
        self.model = fallback.model
        self.models = getattr(fallback, "models", (fallback.model,))
        self.role: str | None = None
        self.effort: str | None = None
        self.key = os.environ.get("CLAUDE_API_KEY", "")
        self.circuit = shared_circuit(self.key)
        self.progress = progress
        self.http = httpx.Client(transport=transport, timeout=httpx.Timeout(
            settings.get("timeout_seconds", 120), connect=15), trust_env=False)

    def for_role(self, role: str) -> Any:
        effort = self.settings.get("roles", {}).get(role)
        if effort not in {"medium", "high"}:
            return self.fallback
        view = copy.copy(self)
        view.role, view.effort = role, effort
        return view

    def close(self) -> None:
        self.http.close()
        self.fallback.close()

    def __enter__(self) -> ClaudeRoutingClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        return self._generate(None, kwargs)

    def generate_validated_json(self, validator: Callable, **kwargs: Any) -> GeminiResult:
        return self._generate(validator, kwargs)

    def _generate(self, validator: Callable | None, kwargs: dict[str, Any]) -> GeminiResult:
        attempts: list[dict[str, Any]] = []
        if self.role and self.key and self.circuit.available():
            result = None
            try:
                result = self._request(kwargs)
                if validator:
                    validator(result.payload)
                return result
            except (httpx.HTTPError, ValueError, RuntimeError, KeyError, TypeError, AttributeError) as exc:
                result = result or getattr(exc, "llm_result", None)
                if result is not None:
                    attempts.append({"model": result.model, "usage": result.usage})
                status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
                # Invalid requests, authentication and billing faults cannot be
                # repaired by repeating another job with the same configuration.
                delay = self.settings.get("cooldown_seconds", 60)
                if status in {400, 401, 402, 403, 404}:
                    delay = self.settings.get("account_cooldown_seconds", 3600)
                if isinstance(exc, httpx.HTTPStatusError):
                    delay = max(delay, retry_after_seconds(exc.response))
                self.circuit.fail(delay)
                if self.progress:
                    self.progress(f"Claude {self.role}: {type(exc).__name__}"
                                  f"{f' HTTP {status}' if status else ''}; using existing route")
        try:
            result = (generate_validated_json(self.fallback, validator, **kwargs) if validator
                      else self.fallback.generate_json(**kwargs))
        except Exception as exc:
            if rejected := getattr(exc, "llm_result", None):
                exc.llm_result = self._account(rejected, attempts)
            else:
                exc.routing_attempts = [*attempts, *getattr(exc, "routing_attempts", [])]
            raise
        return self._account(result, attempts)

    @staticmethod
    def _account(result: GeminiResult, attempts: list[dict[str, Any]]) -> GeminiResult:
        if not attempts:
            return result
        return GeminiResult(result.payload, result.model, result.elapsed_ms, {
            **result.usage, "routingAttempts": [*attempts, *result.usage.get("routingAttempts", [])],
        })

    def _request(self, kwargs: dict[str, Any]) -> GeminiResult:
        from pipeline.free_routing import validate_shape

        started = time.monotonic()
        model = self.settings["model"]
        schema = kwargs["response_schema"]
        response = self.http.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": self.key, "anthropic-version": "2023-06-01"},
            json={"model": model, "max_tokens": self.settings.get("max_output_tokens", 16384),
                  "system": kwargs["system_instruction"],
                  "messages": [{"role": "user", "content": kwargs["prompt"]}],
                  "thinking": {"type": "adaptive"},
                  "output_config": {"effort": self.effort,
                                    "format": {"type": "json_schema", "schema": claude_schema(schema)}}},
            timeout=kwargs.get("timeout_seconds") or self.settings.get("timeout_seconds", 120),
        )
        response.raise_for_status()
        raw = response.json()
        if not isinstance(raw, dict) or not isinstance(raw.get("usage"), dict):
            raise ValueError("Claude response is missing usage")
        usage = raw["usage"]
        for field in ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"):
            value = usage.get(field, 0)
            if type(value) is not int or value < 0:
                raise ValueError("Claude returned invalid token usage")
        if not {"input_tokens", "output_tokens"} <= usage.keys():
            raise ValueError("Claude response is missing token usage")
        incoming = usage.get("input_tokens", 0)
        writes = usage.get("cache_creation_input_tokens", 0)
        reads = usage.get("cache_read_input_tokens", 0)
        outgoing = usage.get("output_tokens", 0)
        thinking = min(outgoing, (usage.get("output_tokens_details") or {}).get("thinking_tokens", 0))
        prices = self.settings["prices"]
        factor = prices["long_context_multiplier"] if incoming + writes + reads > 100000 else 1
        normalized = {
            "model": raw.get("model", model),
            "promptTokenCount": incoming + writes + reads,
            "candidatesTokenCount": outgoing - thinking, "thoughtsTokenCount": thinking,
            "cachedContentTokenCount": reads, "serviceTier": "standard",
            "costUsd": factor * (incoming * prices["input"] + writes * prices["input"] * 1.25
                                 + reads * prices["input"] * 0.1 + outgoing * prices["output"]) / 1e6,
        }
        result = GeminiResult({}, raw.get("model", model), round((time.monotonic() - started) * 1000), normalized)
        try:
            if raw.get("model") != model or raw.get("stop_reason") != "end_turn":
                raise ValueError("Claude returned an unexpected model or incomplete response")
            payload = json.loads("".join(b["text"] for b in raw.get("content", []) if b.get("type") == "text"))
            payload = _restore_optional_fields(payload, schema)
            result = GeminiResult(payload, result.model, result.elapsed_ms, normalized)
            validate_shape(payload, schema)
            validate_constraints(payload, schema)
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            exc.llm_result = result
            raise
        return result
