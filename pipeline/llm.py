from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from pipeline.paths import PROJECT_ROOT

DEFAULT_GEMINI_MODEL = "gemini-3.5-flash-lite"
DEFAULT_GEMINI_REVIEW_MODEL = "gemini-3.7-flash"
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_MAX_OUTPUT_TOKENS = 32768
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 30.0
DEFAULT_MAX_CONNECTIONS = 100
DEFAULT_MAX_KEEPALIVE_CONNECTIONS = 20
DEFAULT_HTTP2 = False
VALID_THINKING_LEVELS = frozenset({"minimal", "low", "medium", "high"})
DEFAULT_REVIEW_FALLBACK_MODELS = ("gemini-3.6-flash", "gemini-3.5-flash")
DEFAULT_REVIEW_LITE_FALLBACK_MODEL = "gemini-3.5-flash-lite"
DEFAULT_CAPACITY_COOLDOWN_SECONDS = 300.0


class GeminiTruncatedError(RuntimeError):
    pass


class GeminiEmptyResponseError(RuntimeError):
    pass


class GeminiRetryableError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class GeminiResult:
    payload: dict[str, Any]
    model: str
    elapsed_ms: int
    usage: dict[str, Any]


def parse_dotenv(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.lstrip()
        if value and value[0] in ("'", '"'):
            quote = value[0]
            end = value.find(quote, 1)
            if end != -1:
                value = value[1:end]
            else:
                value = value[1:]
        else:
            hash_index = value.find(" #")
            if hash_index != -1:
                value = value[:hash_index]
            value = value.rstrip()
        parsed[key] = value
    return parsed


def load_dotenv(path: Path | None = None) -> dict[str, str]:
    env_path = path or PROJECT_ROOT / ".env"
    if not env_path.exists():
        return {}
    parsed = parse_dotenv(env_path.read_text(encoding="utf-8"))
    for key, value in parsed.items():
        if key not in os.environ:
            os.environ[key] = value
    return parsed


class GeminiClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str | None = None,
        timeout_seconds: float = 300,
        api_base: str = GEMINI_API_BASE,
        max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS,
        backoff_max_seconds: float = DEFAULT_BACKOFF_MAX_SECONDS,
        sleep: Callable[[float], None] | None = None,
        http_client: httpx.Client | None = None,
        transport: httpx.BaseTransport | None = None,
        http2: bool = DEFAULT_HTTP2,
        thinking_level: str | None = None,
    ) -> None:
        load_dotenv()
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY", "")
        self.model = model or os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)
        self.timeout_seconds = timeout_seconds
        self.api_base = api_base.rstrip("/")
        self.max_output_tokens = max_output_tokens
        self.max_attempts = max(1, int(max_attempts))
        if thinking_level is not None and thinking_level not in VALID_THINKING_LEVELS:
            raise ValueError(
                f"thinking_level must be one of {sorted(VALID_THINKING_LEVELS)}"
            )
        self.thinking_level = thinking_level
        self.backoff_base_seconds = max(0.0, float(backoff_base_seconds))
        self.backoff_max_seconds = max(0.0, float(backoff_max_seconds))
        self._sleep = sleep or time.sleep
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.Client(
            http2=http2,
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(
                max_connections=DEFAULT_MAX_CONNECTIONS,
                max_keepalive_connections=DEFAULT_MAX_KEEPALIVE_CONNECTIONS,
            ),
            transport=transport,
        )
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")

    def close(self) -> None:
        if self._owns_http_client:
            self._http_client.close()

    def __enter__(self) -> GeminiClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def generate_json(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_schema: dict[str, Any],
        max_output_tokens: int | None = None,
        thinking_level: str | None = None,
    ) -> GeminiResult:
        selected_thinking_level = thinking_level or self.thinking_level
        if (
            selected_thinking_level is not None
            and selected_thinking_level not in VALID_THINKING_LEVELS
        ):
            raise ValueError(
                f"thinking_level must be one of {sorted(VALID_THINKING_LEVELS)}"
            )
        generation_config: dict[str, Any] = {
            "maxOutputTokens": int(max_output_tokens or self.max_output_tokens),
            "responseMimeType": "application/json",
            "responseSchema": response_schema,
        }
        if selected_thinking_level is not None:
            generation_config["thinkingConfig"] = {
                "thinkingLevel": selected_thinking_level,
            }
        request_payload = {
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": generation_config,
        }
        url = f"{self.api_base}/models/{self.model}:generateContent"
        body = json.dumps(request_payload).encode("utf-8")

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            started = time.monotonic()
            try:
                response = self._http_client.post(
                    url,
                    content=body,
                    headers={
                        "Content-Type": "application/json",
                        "x-goog-api-key": self.api_key,
                    },
                )
            except httpx.TransportError as exc:
                if attempt < self.max_attempts:
                    last_error = GeminiRetryableError(f"transport error: {exc}")
                    self._sleep(self._backoff_delay(attempt))
                    continue
                raise GeminiRetryableError(f"Gemini API transport error: {exc}") from exc

            if response.status_code >= 400:
                error_body = response.text
                if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.max_attempts:
                    last_error = GeminiRetryableError(
                        f"HTTP {response.status_code}: {error_body[:500]}",
                        status_code=response.status_code,
                    )
                    self._sleep(self._backoff_delay(attempt))
                    continue
                message = (
                    f"Gemini API request failed with HTTP {response.status_code}: "
                    f"{error_body[:1000]}"
                )
                if response.status_code in RETRYABLE_STATUS_CODES:
                    raise GeminiRetryableError(
                        message,
                        status_code=response.status_code,
                    )
                raise RuntimeError(message)

            elapsed_ms = round((time.monotonic() - started) * 1000)
            try:
                response_payload = response.json()
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Gemini API response was not valid JSON: {response.text[:1000]}") from exc
            try:
                text = _extract_text(response_payload)
            except GeminiTruncatedError:
                raise
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Gemini response was not valid JSON: {text[:1000]}") from exc
            usage = dict(response_payload.get("usageMetadata") or {})
            return GeminiResult(payload=parsed, model=self.model, elapsed_ms=elapsed_ms, usage=usage)

        raise RuntimeError(f"Gemini API request failed after {self.max_attempts} attempts: {last_error}")

    def _backoff_delay(self, attempt: int) -> float:
        import random

        delay = self.backoff_base_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, 0.5)
        return min(delay + jitter, self.backoff_max_seconds)


class FallbackGeminiClient:
    """Try stable Gemini models in order and temporarily bypass capacity-limited tiers."""

    def __init__(
        self,
        clients: list[GeminiClient],
        *,
        cooldown_seconds: float = DEFAULT_CAPACITY_COOLDOWN_SECONDS,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        if not clients:
            raise ValueError("fallback client requires at least one Gemini client")
        self._clients = tuple(clients)
        self.models = tuple(client.model for client in clients)
        self.model = self.models[0]
        self.cooldown_seconds = max(0.0, float(cooldown_seconds))
        self._monotonic = monotonic or time.monotonic
        self._unavailable_until: dict[str, float] = {}
        self._lock = threading.Lock()

    def close(self) -> None:
        for client in self._clients:
            client.close()

    def __enter__(self) -> FallbackGeminiClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def generate_json(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_schema: dict[str, Any],
        max_output_tokens: int | None = None,
        thinking_level: str | None = None,
    ) -> GeminiResult:
        last_error: GeminiRetryableError | None = None
        empty_response_count = 0
        now = self._monotonic()
        attempted = 0
        for index, client in enumerate(self._clients):
            with self._lock:
                unavailable_until = self._unavailable_until.get(client.model, 0.0)
            if unavailable_until > now and index < len(self._clients) - 1:
                continue
            attempted += 1
            try:
                return client.generate_json(
                    system_instruction=system_instruction,
                    prompt=prompt,
                    response_schema=response_schema,
                    max_output_tokens=max_output_tokens,
                    thinking_level=thinking_level,
                )
            except GeminiRetryableError as exc:
                last_error = exc
                with self._lock:
                    self._unavailable_until[client.model] = (
                        self._monotonic() + self.cooldown_seconds
                    )
            except GeminiEmptyResponseError as exc:
                empty_response_count += 1
                last_error = GeminiRetryableError(str(exc))
        models = ", ".join(self.models)
        if attempted and empty_response_count == attempted:
            raise GeminiEmptyResponseError(
                f"all Gemini fallback models returned empty responses ({models})"
            )
        raise GeminiRetryableError(
            f"all Gemini fallback models unavailable after {attempted} attempt(s) "
            f"({models}): {last_error}",
            status_code=getattr(last_error, "status_code", None),
        )


def gemini_model_for_stage(stage: str) -> str:
    """Resolve stable stage-specific model IDs while preserving GEMINI_MODEL fallback."""
    load_dotenv()
    fallback = os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)
    if stage == "review":
        return os.environ.get("GEMINI_REVIEW_MODEL", DEFAULT_GEMINI_REVIEW_MODEL)
    if stage == "bulk":
        return os.environ.get("GEMINI_BULK_MODEL", fallback)
    raise ValueError("stage must be 'bulk' or 'review'")


def _review_fallback_model_names(*, include_lite: bool) -> tuple[str, ...]:
    configured = os.environ.get("GEMINI_REVIEW_FALLBACK_MODELS")
    fallback_models = (
        tuple(model.strip() for model in configured.split(",") if model.strip())
        if configured is not None
        else DEFAULT_REVIEW_FALLBACK_MODELS
    )
    if include_lite:
        lite_model = os.environ.get(
            "GEMINI_REVIEW_LITE_FALLBACK_MODEL",
            DEFAULT_REVIEW_LITE_FALLBACK_MODEL,
        ).strip()
        if lite_model:
            fallback_models = (*fallback_models, lite_model)
    return fallback_models


def create_gemini_client(
    stage: str, *, include_lite: bool = False
) -> GeminiClient | FallbackGeminiClient:
    if stage == "review":
        primary = gemini_model_for_stage(stage)
        models = tuple(
            dict.fromkeys((primary, *_review_fallback_model_names(include_lite=include_lite)))
        )
        attempts = max(1, int(os.environ.get("GEMINI_REVIEW_ATTEMPTS_PER_MODEL", "1")))
        cooldown = float(
            os.environ.get(
                "GEMINI_CAPACITY_COOLDOWN_SECONDS",
                str(DEFAULT_CAPACITY_COOLDOWN_SECONDS),
            )
        )
        return FallbackGeminiClient(
            [
                GeminiClient(
                    model=model,
                    thinking_level="minimal" if model.endswith("-lite") else "low",
                    max_attempts=attempts,
                )
                for model in models
            ],
            cooldown_seconds=cooldown,
        )
    if stage == "bulk":
        return GeminiClient(model=gemini_model_for_stage(stage), thinking_level="minimal")
    raise ValueError("stage must be 'bulk' or 'review'")


def _extract_text(response_payload: dict[str, Any]) -> str:
    candidates = response_payload.get("candidates") or []
    if not candidates:
        raise GeminiEmptyResponseError("Gemini response did not include candidates")
    candidate = candidates[0]
    finish_reason = candidate.get("finishReason")
    if finish_reason and finish_reason not in ("STOP", "FINISH_REASON_UNSPECIFIED", None):
        if finish_reason == "MAX_TOKENS":
            raise GeminiTruncatedError(
                "Gemini response was truncated (finishReason=MAX_TOKENS); "
                "increase max_output_tokens or reduce input size"
            )
        raise RuntimeError(f"Gemini response stopped with finishReason={finish_reason}")
    parts = candidate.get("content", {}).get("parts") or []
    text = "".join(str(part.get("text", "")) for part in parts)
    if not text:
        raise GeminiEmptyResponseError("Gemini response candidate did not include text")
    return text
