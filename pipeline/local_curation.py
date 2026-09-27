"""Local Top News ranking with the existing API route as a failure fallback."""
from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

import httpx

from pipeline.config import load_pipeline_config
from pipeline.llm import GeminiResult, create_llm_client
from pipeline.openrouter import json_schema


def create_top_news_client(*, progress: Callable[[str], None] | None = None) -> Any:
    settings = load_pipeline_config().llm.get("local_top_news") or {}
    def fallback() -> Any:
        return create_llm_client("review", purpose="curation", progress=progress)
    if not settings.get("enabled", False):
        return fallback()
    return LocalTopNewsClient(settings=settings, fallback_factory=fallback, progress=progress)


class LocalTopNewsClient:
    """Only the explicit ranking method uses LocalAI; other calls retain API routing."""

    def __init__(
        self, *, settings: dict[str, Any], fallback_factory: Callable[[], Any],
        progress: Callable[[str], None] | None = None, transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.requested_model = str(settings.get("model", "gemma4-26b-a4b-iq3s"))
        self.model = f"localai/{self.requested_model}"
        self._base = str(settings.get("api_base", "http://127.0.0.1:11434/v1")).rstrip("/")
        self._http = httpx.Client(
            timeout=float(settings.get("timeout_seconds", 90)), transport=transport, trust_env=False,
        )
        self._fallback_factory = fallback_factory
        self._fallback: Any = None
        self._progress = progress

    def _api_client(self) -> Any:
        if self._fallback is None:
            self._fallback = self._fallback_factory()
        return self._fallback

    def close(self) -> None:
        self._http.close()
        if self._fallback is not None:
            self._fallback.close()

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        # Protect category sections if a caller does not supply a separate client.
        return self._api_client().generate_json(**kwargs)

    def generate_top_news(
        self, *, valid_story_ids: set[str], target_top_count: int, **kwargs: Any,
    ) -> GeminiResult:
        started = time.monotonic()
        attempts: list[dict[str, Any]] = []
        try:
            schema = json_schema(kwargs["response_schema"])
            body = {
                "model": self.requested_model,
                "messages": [
                    {"role": "system", "content": kwargs["system_instruction"]
                     + "\nReturn only a JSON object conforming to this schema:\n" + json.dumps(schema)},
                    {"role": "user", "content": kwargs["prompt"]},
                ],
                "response_format": {"type": "json_schema", "json_schema": {
                    "name": "pipeline_response", "strict": True, "schema": schema,
                }},
                "reasoning_effort": "none", "temperature": 0.7, "top_p": 0.8, "seed": 42,
                "max_tokens": kwargs.get("max_output_tokens", 2000), "stream": False,
            }
            if self._progress:
                self._progress(f"top-news: ranking locally with {self.requested_model}")
            response = self._http.post(self._base + "/chat/completions", json=body)
            response.raise_for_status()
            raw = response.json()
            if not isinstance(raw, dict):
                raise ValueError("LocalAI response must be an object")
            usage = raw.get("usage") or {}
            if not isinstance(usage, dict):
                raise ValueError("LocalAI usage must be an object")
            local_usage = {
                "model": self.model, "backend": "localai", "serviceTier": "localai", "costUsd": 0.0,
                "promptTokenCount": usage.get("prompt_tokens"),
                "candidatesTokenCount": usage.get("completion_tokens"),
            }
            attempts.append({"model": self.model, "usage": local_usage})
            if raw.get("model") != self.requested_model:
                raise ValueError("LocalAI returned a different model")
            choice = raw["choices"][0]
            if not isinstance(choice, dict):
                raise ValueError("LocalAI choice must be an object")
            if choice.get("finish_reason") != "stop":
                raise ValueError("LocalAI ranking was incomplete")
            payload = json.loads(choice["message"]["content"])
            ids = payload.get("top_news") if isinstance(payload, dict) else None
            if (not isinstance(ids, list) or len(ids) != target_top_count
                    or not all(isinstance(sid, str) for sid in ids)
                    or len(set(ids)) != len(ids) or not set(ids) <= valid_story_ids):
                raise ValueError("LocalAI Top News count/identity/uniqueness violation")
            if self._progress:
                self._progress(f"top-news: local ranking validated ({len(ids)} stories)")
            return GeminiResult(payload, self.model, round((time.monotonic() - started) * 1000), local_usage)
        except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError) as exc:
            reason = f"{type(exc).__name__}: {exc}"
            if self._progress:
                self._progress(f"top-news: local ranking failed; using API fallback: {reason}")
            try:
                result = self._api_client().generate_json(**kwargs)
            except Exception as fallback_error:
                fallback_error.routing_attempts = [
                    *attempts, *getattr(fallback_error, "routing_attempts", []),
                ]
                raise
            return GeminiResult(result.payload, result.model, round((time.monotonic() - started) * 1000), {
                **result.usage,
                "routingAttempts": [*attempts, *(result.usage.get("routingAttempts") or [])],
                "routingDecisions": [
                    {"model": self.model, "reason": reason}, *(result.usage.get("routingDecisions") or []),
                ],
            })
