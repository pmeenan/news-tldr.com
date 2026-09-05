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
DEFAULT_GEMINI_REVIEW_MODEL = "gemini-3.8-flash"
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
VALID_SERVICE_TIERS = frozenset({"flex", "priority"})
FLEX_SERVICE_TIER = "flex"
# 3.5 Flash costs twice 3.8/3.7 Flash, so it is a last resort reserved for
# verification rather than a general fallback.
DEFAULT_REVIEW_FALLBACK_MODELS = ("gemini-3.7-flash",)
DEFAULT_REVIEW_LAST_RESORT_MODELS = ("gemini-3.5-flash",)
# Flex attempts go to 3.7 Flash: same price as 3.8 on flex, but 3.8's flex pool
# shed every probe during rollout while 3.7 answered in under a second.
DEFAULT_REVIEW_FLEX_MODEL = "gemini-3.7-flash"
DEFAULT_REVIEW_LITE_FALLBACK_MODEL = "gemini-3.5-flash-lite"
DEFAULT_CAPACITY_COOLDOWN_SECONDS = 300.0
# Flex shedding is per request, not a model outage: retry the half-price tier
# again soon instead of abandoning it for the rest of the run.
DEFAULT_FLEX_COOLDOWN_SECONDS = 45.0
# Per-purpose flex budgets (seconds). A flex attempt that exceeds its budget or
# is shed with 429/503 falls back to the standard tier; 0 disables flex.
DEFAULT_FLEX_BUDGET_SECONDS: dict[str, int] = {}


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
        service_tier: str | None = None,
        cooldown_seconds: float | None = None,
    ) -> None:
        load_dotenv()
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY", "")
        self.model = model or os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)
        if service_tier is not None and service_tier not in VALID_SERVICE_TIERS:
            raise ValueError(f"service_tier must be one of {sorted(VALID_SERVICE_TIERS)}")
        self.service_tier = service_tier
        # Per-client capacity cooldown override; None uses the chain's default.
        self.cooldown_seconds = None if cooldown_seconds is None else max(0.0, float(cooldown_seconds))
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

    @property
    def label(self) -> str:
        """Model plus tier, used to key capacity cooldowns and progress output."""
        return f"{self.model}:{self.service_tier or 'standard'}"

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
        request_payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": generation_config,
        }
        if self.service_tier:
            request_payload["serviceTier"] = self.service_tier
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
        self.models = tuple(dict.fromkeys(client.model for client in clients))
        self.labels = tuple(getattr(client, "label", client.model) for client in clients)
        self.model = clients[0].model
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
            label = getattr(client, "label", client.model)
            with self._lock:
                unavailable_until = self._unavailable_until.get(label, 0.0)
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
                client_cooldown = getattr(client, "cooldown_seconds", None)
                cooldown = self.cooldown_seconds if client_cooldown is None else float(client_cooldown)
                with self._lock:
                    self._unavailable_until[label] = self._monotonic() + cooldown
            except GeminiEmptyResponseError as exc:
                empty_response_count += 1
                last_error = GeminiRetryableError(str(exc))
        models = ", ".join(self.labels)
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


def _model_list(env_name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    configured = os.environ.get(env_name)
    if configured is None:
        return default
    return tuple(model.strip() for model in configured.split(",") if model.strip())


def _review_fallback_model_names(
    *, include_lite: bool, last_resort: bool = False
) -> tuple[str, ...]:
    fallback_models = _model_list("GEMINI_REVIEW_FALLBACK_MODELS", DEFAULT_REVIEW_FALLBACK_MODELS)
    if last_resort:
        fallback_models = (
            *fallback_models,
            *_model_list("GEMINI_REVIEW_LAST_RESORT_MODELS", DEFAULT_REVIEW_LAST_RESORT_MODELS),
        )
    if include_lite:
        lite_model = os.environ.get(
            "GEMINI_REVIEW_LITE_FALLBACK_MODEL",
            DEFAULT_REVIEW_LITE_FALLBACK_MODEL,
        ).strip()
        if lite_model:
            fallback_models = (*fallback_models, lite_model)
    return fallback_models


def flex_budget_seconds(purpose: str | None) -> int:
    """Flex attempt budget for a pipeline purpose; 0 means standard tier only."""
    if not purpose or os.environ.get("GEMINI_FLEX_DISABLED", "").strip() in {"1", "true", "yes"}:
        return 0
    from pipeline.config import load_pipeline_config

    budgets = load_pipeline_config().llm.get("flex_budget_seconds") or DEFAULT_FLEX_BUDGET_SECONDS
    try:
        return max(0, int(budgets.get(purpose, 0)))
    except (AttributeError, TypeError, ValueError):
        return 0


def flex_cooldown_seconds() -> float:
    """How long a shed flex attempt bypasses the flex tier (``llm.flex_cooldown_seconds``)."""
    from pipeline.config import load_pipeline_config

    try:
        value = load_pipeline_config().llm.get("flex_cooldown_seconds", DEFAULT_FLEX_COOLDOWN_SECONDS)
        return max(0.0, float(value))
    except (OSError, TypeError, ValueError):
        return DEFAULT_FLEX_COOLDOWN_SECONDS


def _thinking_level_for(model: str) -> str:
    return "minimal" if model.endswith("-lite") else "low"


def _model_chain(
    models: tuple[str, ...],
    *,
    budget_seconds: int,
    standard_attempts: int,
    flex_model: str | None = None,
) -> list[GeminiClient]:
    """One flex attempt (on ``flex_model`` or the primary) when budgeted, then
    every model at the standard tier in order."""
    clients: list[GeminiClient] = []
    if budget_seconds > 0:
        selected = flex_model or models[0]
        clients.append(
            GeminiClient(
                model=selected,
                thinking_level=_thinking_level_for(selected),
                service_tier=FLEX_SERVICE_TIER,
                timeout_seconds=float(budget_seconds),
                max_attempts=1,
                cooldown_seconds=flex_cooldown_seconds(),
            )
        )
    clients.extend(
        GeminiClient(
            model=model,
            thinking_level=_thinking_level_for(model),
            max_attempts=standard_attempts,
        )
        for model in models
    )
    return clients


def create_gemini_client(
    stage: str,
    *,
    include_lite: bool = False,
    purpose: str | None = None,
    last_resort: bool = False,
) -> GeminiClient | FallbackGeminiClient:
    """Build the client chain for a stage.

    ``purpose`` selects the configured flex budget (``llm.flex_budget_seconds``);
    ``last_resort`` appends the expensive last-resort review models, which only
    verification should use."""
    cooldown = float(
        os.environ.get(
            "GEMINI_CAPACITY_COOLDOWN_SECONDS",
            str(DEFAULT_CAPACITY_COOLDOWN_SECONDS),
        )
    )
    budget = flex_budget_seconds(purpose)
    if stage == "review":
        primary = gemini_model_for_stage(stage)
        models = tuple(
            dict.fromkeys((
                primary,
                *_review_fallback_model_names(include_lite=include_lite, last_resort=last_resort),
            ))
        )
        attempts = max(1, int(os.environ.get("GEMINI_REVIEW_ATTEMPTS_PER_MODEL", "1")))
        flex_model = os.environ.get("GEMINI_REVIEW_FLEX_MODEL", DEFAULT_REVIEW_FLEX_MODEL).strip() or None
        return FallbackGeminiClient(
            _model_chain(models, budget_seconds=budget, standard_attempts=attempts, flex_model=flex_model),
            cooldown_seconds=cooldown,
        )
    if stage == "bulk":
        model = gemini_model_for_stage(stage)
        if budget <= 0:
            return GeminiClient(model=model, thinking_level="minimal")
        return FallbackGeminiClient(
            _model_chain((model,), budget_seconds=budget, standard_attempts=DEFAULT_MAX_ATTEMPTS),
            cooldown_seconds=cooldown,
        )
    raise ValueError("stage must be 'bulk' or 'review'")


def llm_price_table() -> dict[str, dict[str, float]]:
    """Per-million-token prices from ``llm.prices`` in the pipeline configuration."""
    from pipeline.config import load_pipeline_config

    prices = load_pipeline_config().llm.get("prices") or {}
    return prices if isinstance(prices, dict) else {}


def estimate_cost_usd(
    *,
    model: str,
    service_tier: str | None,
    input_tokens: int | None,
    output_tokens: int | None,
    thinking_tokens: int | None = None,
    cached_tokens: int | None = None,
    prices: dict[str, dict[str, float]] | None = None,
) -> float | None:
    """Estimate one call's cost. Thinking tokens bill as output; cached prompt
    tokens bill at the cached rate; flex and batch tiers use their own rates."""
    table = prices if prices is not None else llm_price_table()
    entry = table.get(model)
    if not isinstance(entry, dict):
        return None
    tier = (service_tier or "standard").lower()
    prefix = "" if tier == "standard" else f"{tier}_"
    try:
        input_rate = float(entry.get(f"{prefix}input", entry.get("input")))
        output_rate = float(entry.get(f"{prefix}output", entry.get("output")))
    except (TypeError, ValueError):
        return None
    cached = max(0, int(cached_tokens or 0))
    billable_input = max(0, int(input_tokens or 0) - cached)
    cached_rate = entry.get("cached_input")
    cost = billable_input / 1e6 * input_rate
    cost += cached / 1e6 * (float(cached_rate) if cached_rate is not None else input_rate)
    cost += (int(output_tokens or 0) + int(thinking_tokens or 0)) / 1e6 * output_rate
    return round(cost, 8)


def usage_fields(usage: dict[str, Any] | None) -> dict[str, Any]:
    """Token and tier fields worth persisting from a ``usageMetadata`` block."""
    usage = usage or {}

    def count(key: str) -> int | None:
        value = usage.get(key)
        return int(value) if isinstance(value, int | float) and not isinstance(value, bool) else None

    tier = usage.get("serviceTier")
    return {
        "input_tokens": count("promptTokenCount"),
        "output_tokens": count("candidatesTokenCount"),
        "thinking_tokens": count("thoughtsTokenCount"),
        "cached_tokens": count("cachedContentTokenCount"),
        "service_tier": str(tier).lower() if isinstance(tier, str) and tier else None,
    }


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
