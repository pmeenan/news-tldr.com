"""Prefer explicitly selected zero-priced models, then the existing Gemini chain."""
from __future__ import annotations

import hashlib
import math
import os
import threading
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from pipeline.llm import (
    GeminiEmptyResponseError,
    GeminiResult,
    GeminiRetryableError,
    GeminiTruncatedError,
    reported_cost_usd,
)
from pipeline.openrouter import OPENROUTER_API_BASE, OpenRouterClient

UNION = "stealth/union-alpha"
NEMOTRON = "nvidia/nemotron-3-super-120b-a12b:free"
ZERO_PRICE = {"prompt": 0, "completion": 0, "request": 0, "image": 0}


def zero_priced(model: dict[str, Any]) -> bool:
    prices = model.get("pricing")
    if not isinstance(prices, dict) or not {"prompt", "completion"} <= prices.keys():
        return False
    try:
        return all(not isinstance(v, bool) and math.isfinite(float(v)) and float(v) == 0 for v in prices.values())
    except (TypeError, ValueError, OverflowError):
        return False


class FreeModelPolicy:
    """One process-wide quota reservation and capacity circuit per API key.

    Pipeline commands already share an exclusive process lock. Separate evaluation
    processes and other account users are reconciled through the live key counter;
    provider rate limits remain authoritative. Unknown metadata fails closed.
    """

    def __init__(
        self, *, api_key: str, settings: dict[str, Any],
        transport: httpx.BaseTransport | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        utc_day: Callable[[], str] | None = None,
    ) -> None:
        self.api_key = api_key
        self.settings = settings
        self._transport = transport
        self._clock = monotonic
        self._day = utc_day or (lambda: datetime.now(UTC).date().isoformat())
        self._lock = threading.Lock()
        self._catalog: dict[str, dict[str, Any]] = {}
        self._catalog_until = 0.0
        self._quota_until = 0.0
        self._quota_day = ""
        self._remaining: int | None = None
        self._quota_valid = False
        self._active: dict[str, int] = {}
        self._cooldown: dict[str, float] = {}
        self._requests: deque[float] = deque()

    def _get(self, resource: str) -> dict[str, Any]:
        # Short-lived metadata clients avoid a process-global connection pool
        # outliving the stage clients. The lock coalesces simultaneous refreshes.
        with httpx.Client(transport=self._transport, timeout=5) as client:
            response = client.get(
                f"{OPENROUTER_API_BASE}/{resource}",
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
            response.raise_for_status()
            return response.json()

    def _refresh_catalog(self, now: float) -> None:
        if now < self._catalog_until:
            return
        self._catalog = {}
        try:
            data = self._get("models")["data"]
            self._catalog = {m["id"]: m for m in data if isinstance(m, dict) and m.get("id") in {UNION, NEMOTRON}}
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            pass
        self._catalog_until = self._clock() + self.settings.get("metadata_ttl_seconds", 60)

    def _refresh_quota(self, now: float) -> None:
        day = self._day()
        if now < self._quota_until and day == self._quota_day:
            return
        self._quota_valid = False
        if day != self._quota_day:
            self._remaining = None
        try:
            quota = self._get("key")["data"]["free_model_daily_requests"]
            remaining = quota["remaining"]
            if type(remaining) is not int or remaining < 0:
                raise ValueError("unknown free quota")
            # Reserve conservatively for already running requests and do not
            # restore locally consumed slots from a delayed API counter.
            remaining = max(0, remaining - self._active.get(NEMOTRON, 0))
            if day == self._quota_day and self._remaining is not None:
                remaining = min(remaining, self._remaining)
            self._remaining = remaining
            self._quota_valid = True
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            pass
        self._quota_day = day
        self._quota_until = self._clock() + self.settings.get("metadata_ttl_seconds", 60)

    def _unavailable(self, model: str, now: float) -> str | None:
        if self._cooldown.get(model, 0) > now:
            return "capacity cooldown"
        self._refresh_catalog(now)
        metadata = self._catalog.get(model)
        if not metadata or not zero_priced(metadata):
            return "not available at a confirmed zero price"
        required = {"response_format"} | ({"structured_outputs", "reasoning"} if model == NEMOTRON else set())
        if not required <= set(metadata.get("supported_parameters") or []):
            return "required JSON/reasoning support unavailable"
        if model == NEMOTRON:
            self._refresh_quota(now)
            if not self._quota_valid or not self._remaining:
                return "free daily quota exhausted or unknown"
        return None

    def acquire(self, model: str) -> str | None:
        with self._lock:
            reason = self._unavailable(model, self._clock())
            if reason:
                return reason
            active = self._active.get(model, 0)
            if active >= self.settings.get("max_in_flight_per_model", 40):
                return "local concurrency limit"
            self._active[model] = active + 1
            return None

    def reserve_attempt(self, model: str) -> str | None:
        with self._lock:
            now = self._clock()
            reason = self._unavailable(model, now)
            if reason:
                return reason
            if model == NEMOTRON:
                while self._requests and now - self._requests[0] >= 60:
                    self._requests.popleft()
                if len(self._requests) >= self.settings.get("free_requests_per_minute", 20):
                    return "free per-minute request budget"
                self._remaining -= 1
                self._requests.append(now)
            return None

    def release(self, model: str) -> None:
        with self._lock:
            self._active[model] -= 1

    def failed(self, model: str, *, delay: float = 0) -> None:
        with self._lock:
            self._cooldown[model] = max(
                self._cooldown.get(model, 0),
                self._clock() + max(delay, self.settings.get("cooldown_seconds", 120)),
            )
            if model == NEMOTRON:
                self._quota_until = 0  # Reconcile daily/rate-limit failures before trying again.


_POLICIES: dict[str, FreeModelPolicy] = {}
_POLICY_LOCK = threading.Lock()


def shared_policy(api_key: str, settings: dict[str, Any]) -> FreeModelPolicy:
    identity = hashlib.sha256(api_key.encode()).hexdigest()
    with _POLICY_LOCK:
        if identity not in _POLICIES:
            _POLICIES[identity] = FreeModelPolicy(api_key=api_key, settings=settings)
        return _POLICIES[identity]


def validate_shape(value: Any, schema: dict[str, Any], path: str = "response") -> None:
    """Check the pipeline's schema subset before accepting a JSON-mode answer.

    Domain-specific passage, membership, factual and editorial checks still run
    in their original stages. This catches malformed shapes at the routing layer.
    """
    if value is None and schema.get("nullable"):
        return
    kind = str(schema.get("type", "")).lower()
    valid = {
        "object": isinstance(value, dict), "array": isinstance(value, list),
        "string": isinstance(value, str), "boolean": type(value) is bool,
        "integer": type(value) is int,
        "number": type(value) in (int, float) and math.isfinite(value),
    }
    if kind and not valid.get(kind, False):
        raise ValueError(f"{path}: expected {kind}")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(f"{path}: value outside enum")
    if isinstance(value, dict):
        for key in schema.get("required", []):
            if key not in value:
                raise ValueError(f"{path}: missing {key}")
        for key, subschema in schema.get("properties", {}).items():
            if key in value:
                validate_shape(value[key], subschema, f"{path}.{key}")
    if isinstance(value, list):
        for item in value:
            validate_shape(item, schema.get("items", {}), f"{path}[]")
        if len(value) < schema.get("minItems", 0) or len(value) > schema.get("maxItems", math.inf):
            raise ValueError(f"{path}: invalid item count")


class FreeFirstClient:
    def __init__(
        self, *, stage: str, fallback: Any, settings: dict[str, Any],
        policy: FreeModelPolicy | None = None, candidates: dict[str, Any] | None = None,
        progress: Callable[[str], None] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.fallback = fallback
        self.model = f"free-first/{stage}"
        self.settings = settings
        self._progress = progress
        self._sleep = sleep
        self.order = (NEMOTRON, UNION) if stage == "bulk" else (UNION,)
        self.models = (*[f"openrouter/{m}" for m in self.order], *getattr(fallback, "models", (fallback.model,)))
        key = os.environ.get("OPENROUTER_API_KEY", "")
        self.policy = policy or (shared_policy(key, settings) if key else None)
        if candidates is not None:
            self.candidates = candidates
            return
        self.candidates = {
            model: OpenRouterClient(
                model=model, api_key=key, max_attempts=1,
                response_format="json_schema" if model == NEMOTRON else "json_object",
                reasoning_effort="low" if model == NEMOTRON else "",
                provider_preferences={"max_price": ZERO_PRICE},
                max_output_tokens=32768, timeout_seconds=settings.get("timeout_seconds", 90),
            ) for model in self.order
        } if self.policy is not None else {}

    def close(self) -> None:
        for client in self.candidates.values():
            client.close()
        self.fallback.close()

    def __enter__(self) -> FreeFirstClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _log(self, message: str) -> None:
        if self._progress:
            self._progress(f"free routing: {message}")

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        started = time.monotonic()
        attempts: list[dict[str, Any]] = []
        decisions: list[dict[str, str]] = []
        for model in self.order if self.policy else ():
            reason = self.policy.acquire(model)
            if reason:
                decisions.append({"model": model, "reason": reason})
                self._log(f"skip {model}: {reason}")
                continue
            try:
                for attempt in range(self.settings.get("max_attempts", 2)):
                    reason = self.policy.reserve_attempt(model)
                    if reason:
                        decisions.append({"model": model, "reason": reason})
                        self._log(f"skip {model}: {reason}")
                        break
                    result = None
                    try:
                        result = self.candidates[model].generate_json(**kwargs)
                        if reported_cost_usd(result.usage) != 0:
                            raise RuntimeError("zero request cost was not confirmed")
                        validate_shape(result.payload, kwargs["response_schema"])
                        self._log(f"{model} succeeded on attempt {attempt + 1}")
                        return self._with_routing(result, attempts, decisions, started)
                    except (RuntimeError, ValueError, httpx.HTTPError) as exc:
                        result = result or getattr(exc, "llm_result", None)
                        if result is not None:
                            attempts.append({"model": result.model, "usage": result.usage})
                        status = getattr(exc, "status_code", None)
                        reason = f"{type(exc).__name__}" + (f" HTTP {status}" if status else "")
                        decisions.append({"model": model, "reason": reason})
                        self._log(f"{model} attempt {attempt + 1} failed ({reason})")
                        # Rate limits cool immediately. Other transport/format
                        # failures get one bounded retry; configuration failures do not.
                        retry = isinstance(exc, (GeminiRetryableError, GeminiEmptyResponseError,
                                                 GeminiTruncatedError, ValueError, httpx.TransportError))
                        delay = getattr(exc, "retry_after_seconds", 0)
                        exhausted = attempt + 1 >= self.settings.get("max_attempts", 2)
                        if status == 429 or delay > 0 or not retry or exhausted:
                            self.policy.failed(model, delay=delay)
                            break
                        self._sleep(1)
            finally:
                self.policy.release(model)
        self._log(f"fall back to {self.fallback.model}")
        try:
            result = self.fallback.generate_json(**kwargs)
        except Exception as exc:
            exc.routing_attempts = attempts
            raise
        return self._with_routing(result, attempts, decisions, started)

    @staticmethod
    def _with_routing(
        result: GeminiResult, attempts: list[dict[str, Any]], decisions: list[dict[str, str]], started: float,
    ) -> GeminiResult:
        return GeminiResult(result.payload, result.model, round((time.monotonic() - started) * 1000), {
            **result.usage, "model": result.model, "routingAttempts": attempts, "routingDecisions": decisions,
        })
