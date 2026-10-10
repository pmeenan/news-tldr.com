"""Prefer explicitly selected zero-priced models, then the existing Gemini chain."""
from __future__ import annotations

import copy
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
    generate_validated_json,
    llm_client_for_role,
    reported_cost_usd,
)
from pipeline.openrouter import OPENROUTER_API_BASE, OpenRouterClient

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
        self._rate_limit_until: dict[str, float] = {}
        self._next_attempt: dict[str, float] = {}
        self._queues: dict[str, deque[object]] = {}

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
            self._catalog = {m["id"]: m for m in data if isinstance(m, dict) and m.get("id") == NEMOTRON}
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
        required = {"response_format", "structured_outputs", "reasoning"}
        if not required <= set(metadata.get("supported_parameters") or []):
            return "required JSON/reasoning support unavailable"
        if model == NEMOTRON:
            self._refresh_quota(now)
            if not self._quota_valid or not self._remaining:
                return "free daily quota exhausted or unknown"
        return None

    def reserve_attempt(
        self, model: str, *, deadline: float | None = None,
        sleep: Callable[[float], None] = time.sleep,
        progress: Callable[[str], None] | None = None,
    ) -> str | None:
        """Wait fairly, then atomically reserve capacity and quota for one send.

        Waiting workers consume neither daily quota nor in-flight slots. Every
        attempt, including retries, rejoins the shared queue and is spaced out.
        A missing deadline requests immediate admission only.
        """
        deadline = self._clock() if deadline is None else deadline
        ticket = object()
        with self._lock:
            queue = self._queues.setdefault(model, deque())
            queue.append(ticket)
        next_log = 0.0
        waited = False
        try:
            while True:
                with self._lock:
                    reason = self._unavailable(model, self._clock())
                    if reason:
                        return reason
                    # Metadata refreshes can take time; base reservations and
                    # deadlines on the clock after those lookups complete.
                    now = self._clock()
                    if waited and now > deadline:
                        return "request queue exceeds free wait budget"
                    ready_at = max(self._next_attempt.get(model, 0), self._rate_limit_until.get(model, 0))
                    active = self._active.get(model, 0)
                    if queue[0] is ticket and ready_at <= now and active < self.settings.get(
                        "max_in_flight_per_model", 40,
                    ):
                        rpm = min(20, max(1, self.settings.get("free_requests_per_minute", 20)))
                        self._remaining -= 1
                        # Small boundary margin keeps every rolling minute below
                        # the cap even when a provider rounds arrival timestamps.
                        self._next_attempt[model] = now + 60 / rpm + 0.05
                        self._active[model] = active + 1
                        return None
                    if self._rate_limit_until.get(model, 0) > now:
                        reason = "rate-limit cooldown"
                    elif ready_at > now:
                        reason = "request pacing"
                    elif active >= self.settings.get("max_in_flight_per_model", 40):
                        reason = "local concurrency limit"
                    else:
                        reason = "queued for request slot"
                    if now >= deadline or ready_at > deadline:
                        return f"{reason} exceeds free wait budget"
                    wait = min(1.0, deadline - now)
                    if queue[0] is ticket and ready_at > now:
                        wait = min(wait, ready_at - now)
                # Never sleep or emit progress while holding the shared lock.
                if progress and now >= next_log:
                    progress(f"wait for {model}: {reason} ({deadline - now:.0f}s budget remaining)")
                    next_log = now + 30
                waited = True
                sleep(wait)
        finally:
            with self._lock:
                queue.remove(ticket)

    def release(self, model: str) -> None:
        with self._lock:
            self._active[model] -= 1

    def failed(self, model: str, *, delay: float = 0, rate_limited: bool = False) -> None:
        with self._lock:
            circuit = self._rate_limit_until if rate_limited else self._cooldown
            default = self.settings.get("rate_limit_retry_seconds", 60) if rate_limited else self.settings.get(
                "cooldown_seconds", 120,
            )
            circuit[model] = max(
                circuit.get(model, 0), self._clock() + max(delay, default),
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
        self.order = (NEMOTRON,) if stage == "bulk" else ()
        self.models = (*[f"openrouter/{m}" for m in self.order], *getattr(fallback, "models", (fallback.model,)))
        key = os.environ.get("OPENROUTER_API_KEY", "")
        self.policy = policy or (shared_policy(key, settings) if key and self.order else None)
        if candidates is not None:
            self.candidates = candidates
            return
        self.candidates = {
            model: OpenRouterClient(
                model=model, api_key=key, max_attempts=1,
                response_format="json_schema", reasoning_effort="low",
                provider_preferences={"max_price": ZERO_PRICE},
                max_output_tokens=32768, timeout_seconds=settings.get("timeout_seconds", 90),
            ) for model in self.order
        } if self.policy is not None else {}

    def for_role(self, role: str) -> FreeFirstClient:
        view = copy.copy(self)
        view.fallback = llm_client_for_role(self.fallback, role)
        return view

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

    def validation_session(self) -> ValidationSession:
        return ValidationSession(self)

    def generate_validated_json(self, validator: Callable[[dict[str, Any]], Any], **kwargs: Any) -> GeminiResult:
        return self.validation_session().generate_validated_json(validator, **kwargs)

    def generate_json(self, *, _validator: Callable | None = None, **kwargs: Any) -> GeminiResult:
        started = time.monotonic()
        attempts: list[dict[str, Any]] = []
        decisions: list[dict[str, str]] = []
        deadline = self.policy._clock() + self.settings.get("max_wait_seconds", 180) if self.policy else 0
        for model in self.order if self.policy else ():
            for attempt in range(self.settings.get("max_attempts", 2)):
                reason = self.policy.reserve_attempt(model, deadline=deadline, sleep=self._sleep, progress=self._log)
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
                    retry = isinstance(exc, (GeminiRetryableError, GeminiEmptyResponseError,
                                             GeminiTruncatedError, httpx.TransportError))
                    delay = getattr(exc, "retry_after_seconds", 0)
                    exhausted = attempt + 1 >= self.settings.get("max_attempts", 2)
                    if status == 429:
                        self.policy.failed(model, delay=delay, rate_limited=True)
                        if exhausted:
                            break
                        # Rejoin the queue; all workers honor this cooldown and
                        # retry with spacing, within the original wait deadline.
                        continue
                    if delay > 0 or not retry or exhausted:
                        self.policy.failed(model, delay=delay)
                        break
                finally:
                    self.policy.release(model)
                self._sleep(1)
        self._log(f"fall back to {self.fallback.model}")
        try:
            result = (generate_validated_json(self.fallback, _validator, **kwargs) if _validator
                      else self.fallback.generate_json(**kwargs))
        except Exception as exc:
            if rejected := getattr(exc, "llm_result", None):
                exc.llm_result = self._with_routing(rejected, attempts, decisions, started)
            else:
                exc.routing_attempts = [*attempts, *getattr(exc, "routing_attempts", [])]
            raise
        return self._with_routing(result, attempts, decisions, started)

    @staticmethod
    def _with_routing(
        result: GeminiResult, attempts: list[dict[str, Any]], decisions: list[dict[str, str]], started: float,
    ) -> GeminiResult:
        return GeminiResult(result.payload, result.model, round((time.monotonic() - started) * 1000), {
            **result.usage, "model": result.model,
            "routingAttempts": [*attempts, *result.usage.get("routingAttempts", [])],
            "routingDecisions": [*decisions, *result.usage.get("routingDecisions", [])],
        })


class ValidationSession:
    """A failed free answer never consumes a paid repair or resets another worker's route."""

    def __init__(self, route: FreeFirstClient) -> None:
        self.route = route
        self.client: Any = route
        self.model = route.model

    def for_role(self, role: str) -> ValidationSession:
        view = copy.copy(self)
        view.route = self.route.for_role(role)
        view.client = view.route if self.client is self.route else llm_client_for_role(self.client, role)
        return view

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        return self.client.generate_json(**kwargs)

    def generate_validated_json(self, validator: Callable[[dict[str, Any]], Any], **kwargs: Any) -> GeminiResult:
        try:
            result = (self.route.generate_json(_validator=validator, **kwargs) if self.client is self.route
                      else generate_validated_json(self.client, validator, **kwargs))
        except ValueError:
            self.client = self.route.fallback
            raise
        try:
            validator(result.payload)
        except ValueError as exc:
            if result.model not in {f"openrouter/{m}" for m in self.route.order}:
                self.client = self.route.fallback
                exc.llm_result = result
                raise
            # Do not mutate the shared route, weaken the validator, or promote bulk work to Flash.
            self.client = self.route.fallback
            self.route._log(f"{result.model} failed domain validation; retry with {self.client.model}: {exc}")
            rejected = {"model": result.model, "usage": result.usage}
            try:
                replacement = generate_validated_json(self.client, validator, **kwargs)
            except Exception as paid_error:
                if paid_result := getattr(paid_error, "llm_result", None):
                    paid_error.llm_result = self._account(paid_result, rejected)
                else:
                    paid_error.routing_attempts = [rejected, *getattr(paid_error, "routing_attempts", [])]
                raise
            return self._account(replacement, rejected)
        return result

    @staticmethod
    def _account(result: GeminiResult, rejected: dict[str, Any]) -> GeminiResult:
        return GeminiResult(result.payload, result.model, result.elapsed_ms, {
            **result.usage, "model": result.model,
            "routingAttempts": [rejected, *(result.usage.get("routingAttempts") or [])],
            "routingDecisions": [*(result.usage.get("routingDecisions") or []),
                                 {"model": rejected["model"], "reason": "domain validation failed"}],
        })
