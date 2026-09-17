"""OpenRouter adapter for the pipeline's structured JSON client contract."""
from __future__ import annotations

import copy
import json
import math
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from pipeline.llm import (
    DEFAULT_MAX_OUTPUT_TOKENS,
    RETRYABLE_STATUS_CODES,
    GeminiEmptyResponseError,
    GeminiResult,
    GeminiRetryableError,
    GeminiTruncatedError,
    load_dotenv,
)

OPENROUTER_API_BASE = "https://openrouter.ai/api/v1"
REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}


def retry_after_seconds(response: httpx.Response | None) -> float:
    value = response.headers.get("Retry-After", "") if response is not None else ""
    try:
        delay = float(value)
        return max(0.0, delay) if math.isfinite(delay) else 0.0
    except ValueError:
        try:
            return max(0.0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 0.0


def json_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Translate Gemini schemas to strict JSON Schema without changing the input.

    Strict endpoints require all properties. Optional fields become nullable;
    the response adapter removes their null placeholders before stage validation.
    """
    result = copy.deepcopy(schema)
    result.pop("propertyOrdering", None)
    nullable = result.pop("nullable", False)
    if isinstance(result.get("type"), str):
        result["type"] = result["type"].lower()
    if "properties" in result:
        required = set(result.get("required", []))
        result["properties"] = {
            key: json_schema(value) if key in required else {
                "anyOf": [json_schema(value), {"type": "null"}],
            }
            for key, value in result["properties"].items()
        }
        result["required"] = list(result["properties"])
        result["additionalProperties"] = False
    if isinstance(result.get("items"), dict):
        result["items"] = json_schema(result["items"])
    for key in ("anyOf", "oneOf", "allOf"):
        if key in result:
            result[key] = [json_schema(value) for value in result[key]]
    if nullable:
        result = {"anyOf": [result, {"type": "null"}]}
    return result


def _restore_optional_fields(value: Any, schema: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        return {
            key: _restore_optional_fields(item, properties.get(key, {}))
            for key, item in value.items()
            if not (key in properties and key not in required and item is None)
        }
    if isinstance(value, list):
        return [_restore_optional_fields(item, schema.get("items", {})) for item in value]
    return value


def normalized_usage(response: dict[str, Any], requested_model: str) -> dict[str, Any]:
    """Keep the legacy token names used by stage accounting, plus raw provenance."""
    usage = response.get("usage") or {}
    completion = usage.get("completion_tokens")
    reasoning = (usage.get("completion_tokens_details") or {}).get("reasoning_tokens")
    result = {
        "promptTokenCount": usage.get("prompt_tokens"),
        # OpenRouter includes reasoning in completion_tokens; Gemini does not.
        "candidatesTokenCount": max(0, completion - (reasoning or 0)) if completion is not None else None,
        "thoughtsTokenCount": reasoning,
        "cachedContentTokenCount": (usage.get("prompt_tokens_details") or {}).get("cached_tokens"),
        "serviceTier": "openrouter",
        "backend": "openrouter",
        "model": f"openrouter/{response.get('model') or requested_model}",
        "requestedModel": requested_model,
        "provider": response.get("provider"),
        "generationId": response.get("id"),
        "openrouterUsage": usage,
    }
    cost = usage.get("cost")
    if isinstance(cost, int | float) and not isinstance(cost, bool) and math.isfinite(cost) and cost >= 0:
        result["costUsd"] = float(cost)
    return result


class OpenRouterClient:
    def __init__(
        self, *, model: str, api_key: str | None = None,
        api_base: str = OPENROUTER_API_BASE, timeout_seconds: float = 300,
        max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS, max_attempts: int = 3,
        response_format: str | None = None, reasoning_effort: str | None = None,
        provider_preferences: dict[str, Any] | None = None,
        sleep: Callable[[float], None] | None = None,
        http_client: httpx.Client | None = None, transport: httpx.BaseTransport | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> None:
        load_dotenv()
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")
        if not self.api_key:
            raise RuntimeError("OPENROUTER_API_KEY is not configured")
        if not model.strip():
            raise ValueError("An explicit OpenRouter model ID is required")
        self.requested_model = model.strip()
        self.model = f"openrouter/{self.requested_model}"
        self.response_format = response_format or os.environ.get("OPENROUTER_RESPONSE_FORMAT", "json_schema")
        if self.response_format not in {"json_schema", "json_object"}:
            raise ValueError("OPENROUTER_RESPONSE_FORMAT must be json_schema or json_object")
        self.reasoning_effort = (
            os.environ.get("OPENROUTER_REASONING_EFFORT") if reasoning_effort is None else reasoning_effort
        ) or None
        if self.reasoning_effort is not None and self.reasoning_effort not in REASONING_EFFORTS:
            raise ValueError(f"reasoning effort must be one of {sorted(REASONING_EFFORTS)}")
        preferences = provider_preferences
        if preferences is None:
            preferences = json.loads(os.environ.get("OPENROUTER_PROVIDER_PREFERENCES") or "{}")
        if not isinstance(preferences, dict):
            raise ValueError("OPENROUTER_PROVIDER_PREFERENCES must be a JSON object")
        self.provider_preferences = {**preferences, "require_parameters": True}
        self.api_base = api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_output_tokens = max_output_tokens
        self.max_attempts = max(1, int(max_attempts))
        self._sleep = sleep or time.sleep
        self._progress = progress
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client(
            timeout=timeout_seconds, transport=transport,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def __enter__(self) -> OpenRouterClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def generate_json(
        self, *, system_instruction: str, prompt: str, response_schema: dict[str, Any],
        max_output_tokens: int | None = None, thinking_level: str | None = None,
        timeout_seconds: float | None = None,
    ) -> GeminiResult:
        schema = json_schema(response_schema)
        format_config: dict[str, Any] = {"type": self.response_format}
        if self.response_format == "json_schema":
            format_config["json_schema"] = {"name": "pipeline_response", "strict": True, "schema": schema}
        else:
            system_instruction += "\nReturn only a JSON object conforming to this schema:\n" + json.dumps(schema)
        payload: dict[str, Any] = {
            "model": self.requested_model,
            "messages": [{"role": "system", "content": system_instruction}, {"role": "user", "content": prompt}],
            "response_format": format_config,
            "max_tokens": max_output_tokens or self.max_output_tokens,
            "provider": self.provider_preferences,
            "stream": False,
        }
        # Stage-level thinking hints belong to Gemini. OpenRouter models have
        # different capabilities; only forward an explicit OpenRouter setting.
        effort = self.reasoning_effort
        if effort is not None:
            if effort not in REASONING_EFFORTS:
                raise ValueError(f"reasoning effort must be one of {sorted(REASONING_EFFORTS)}")
            payload["reasoning"] = {"effort": effort, "exclude": True}
        started = time.monotonic()
        for attempt in range(self.max_attempts):
            response = None
            try:
                response = self._http_client.post(
                    f"{self.api_base}/chat/completions", json=payload,
                    headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                    timeout=self.timeout_seconds if timeout_seconds is None else timeout_seconds,
                )
                if response.status_code >= 400:
                    message = f"OpenRouter HTTP {response.status_code}: {response.text[:1000]}"
                    if response.status_code in RETRYABLE_STATUS_CODES:
                        raise GeminiRetryableError(message, status_code=response.status_code)
                    error = RuntimeError(message)
                    error.status_code = response.status_code
                    raise error
                data = response.json()
                if not isinstance(data, dict):
                    raise RuntimeError("OpenRouter response must be a JSON object")
                if error := data.get("error"):
                    code = error.get("code")
                    message = f"OpenRouter error {code}: {error.get('message', '')}"
                    if code in RETRYABLE_STATUS_CODES:
                        raise GeminiRetryableError(message, status_code=code)
                    raise RuntimeError(message)
            except (httpx.TransportError, GeminiRetryableError) as exc:
                if attempt + 1 >= self.max_attempts:
                    error = GeminiRetryableError(
                        f"OpenRouter request failed: {exc}", status_code=getattr(exc, "status_code", None),
                    )
                    error.retry_after_seconds = retry_after_seconds(response)
                    raise error from exc
                delay = max(min(2 ** attempt, 30), retry_after_seconds(response))
                # Leave long/daily quota waits to the next run, rather than retrying early.
                if delay > 60:
                    raise GeminiRetryableError(f"OpenRouter retry deferred for {delay:g}s", status_code=429) from exc
                if self._progress:
                    self._progress(f"OpenRouter {self.requested_model}: retry {attempt + 2} in {delay:g}s")
                self._sleep(delay)
                continue
            result = GeminiResult(
                payload={}, model=f"openrouter/{data.get('model') or self.requested_model}",
                elapsed_ms=round((time.monotonic() - started) * 1000),
                usage=normalized_usage(data, self.requested_model),
            )
            try:
                choices = data.get("choices") or []
                if not choices:
                    raise GeminiEmptyResponseError("OpenRouter returned no choices")
                choice = choices[0]
                reason = choice.get("finish_reason")
                if reason == "length":
                    raise GeminiTruncatedError("OpenRouter response reached its output token limit")
                if reason not in {None, "stop"}:
                    raise RuntimeError(f"OpenRouter response stopped with finish_reason={reason}")
                message = choice.get("message") or {}
                if message.get("refusal"):
                    raise GeminiEmptyResponseError("OpenRouter model refused the request")
                content = message.get("content")
                if not isinstance(content, str) or not content.strip():
                    raise GeminiEmptyResponseError("OpenRouter returned no answer content")
                parsed = json.loads(content)
                if not isinstance(parsed, dict):
                    raise ValueError("OpenRouter answer must be a JSON object")
            except (ValueError, RuntimeError) as exc:
                # Evaluation reports can account for billed malformed/truncated replies.
                exc.llm_result = result
                raise
            return GeminiResult(
                payload=_restore_optional_fields(parsed, response_schema), model=result.model,
                elapsed_ms=result.elapsed_ms, usage=result.usage,
            )
        raise AssertionError("unreachable")
