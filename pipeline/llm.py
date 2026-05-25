from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.paths import PROJECT_ROOT

DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
DEFAULT_MAX_OUTPUT_TOKENS = 32768
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 30.0


class GeminiTruncatedError(RuntimeError):
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
    ) -> None:
        load_dotenv()
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY", "")
        self.model = model or os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)
        self.timeout_seconds = timeout_seconds
        self.api_base = api_base.rstrip("/")
        self.max_output_tokens = max_output_tokens
        self.max_attempts = max(1, int(max_attempts))
        self.backoff_base_seconds = max(0.0, float(backoff_base_seconds))
        self.backoff_max_seconds = max(0.0, float(backoff_max_seconds))
        self._sleep = sleep or time.sleep
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured")

    def generate_json(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_schema: dict[str, Any],
        temperature: float = 0,
        max_output_tokens: int | None = None,
    ) -> GeminiResult:
        request_payload = {
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "topP": 1,
                "topK": 1,
                "maxOutputTokens": int(max_output_tokens or self.max_output_tokens),
                "responseMimeType": "application/json",
                "responseSchema": response_schema,
            },
        }
        url = f"{self.api_base}/models/{self.model}:generateContent"
        body = json.dumps(request_payload).encode("utf-8")

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            request = urllib.request.Request(
                url,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": self.api_key,
                },
            )
            started = time.monotonic()
            try:
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    response_body = response.read()
            except urllib.error.HTTPError as exc:
                error_body = exc.read().decode("utf-8", errors="replace")
                if exc.code in RETRYABLE_STATUS_CODES and attempt < self.max_attempts:
                    last_error = GeminiRetryableError(f"HTTP {exc.code}: {error_body[:500]}", status_code=exc.code)
                    self._sleep(self._backoff_delay(attempt))
                    continue
                raise RuntimeError(f"Gemini API request failed with HTTP {exc.code}: {error_body[:1000]}") from exc
            except (urllib.error.URLError, TimeoutError) as exc:
                if attempt < self.max_attempts:
                    last_error = GeminiRetryableError(f"transport error: {exc}")
                    self._sleep(self._backoff_delay(attempt))
                    continue
                raise RuntimeError(f"Gemini API transport error: {exc}") from exc

            elapsed_ms = round((time.monotonic() - started) * 1000)
            response_payload = json.loads(response_body.decode("utf-8"))
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


def _extract_text(response_payload: dict[str, Any]) -> str:
    candidates = response_payload.get("candidates") or []
    if not candidates:
        raise RuntimeError("Gemini response did not include candidates")
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
        raise RuntimeError("Gemini response candidate did not include text")
    return text
