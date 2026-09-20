"""Production duplicate decisions on Jev, with bounded failover to Flash."""
from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx

from pipeline.jev_shadow import MODEL, build_request, encoded, normalize_usage, retry_delay, validate_response
from pipeline.llm import GeminiResult, load_dotenv


class JevDuplicateClient:
    def __init__(self, fallback: Any, options: dict[str, Any], *,
                 progress: Callable[[str], None] | None = None, client: httpx.Client | None = None) -> None:
        load_dotenv()
        self.fallback = fallback
        self.model = "openrouter/" + MODEL
        self.options = options
        self.progress = progress
        self.api_key = os.environ.get("OPENROUTER_API_KEY", "")
        self.client = client or httpx.Client()
        self.lock = threading.Lock()
        self.slots = threading.BoundedSemaphore(max(1, min(4, int(options.get("concurrency", 4)))))
        self.next_request = 0.0
        self.cooldown_until = 0.0

    def close(self) -> None:
        self.client.close()
        close = getattr(self.fallback, "close", None)
        if close:
            close()

    def generate_json(self, **kwargs: Any) -> GeminiResult:
        # Only native pair calls may use Jev; it is not a general JSON generator.
        return self.fallback.generate_json(**kwargs)

    def generate_pair(self, payload1: dict[str, Any], payload2: dict[str, Any], **kwargs: Any) -> GeminiResult:
        started = time.monotonic()
        usage = None
        reason = "missing API key"
        if self.api_key:
            with self.slots:
                with self.lock:
                    cooling = time.monotonic() < self.cooldown_until
                    due = max(time.monotonic(), self.next_request)
                    if not cooling:
                        self.next_request = due + 60 / max(1, min(120, self.options.get("requests_per_minute", 60)))
                reason = "provider cooldown"
                if not cooling:
                    time.sleep(max(0, due - time.monotonic()))
                    with self.lock:
                        cooling = time.monotonic() < self.cooldown_until
                    if not cooling:
                        delay = max(1, float(self.options.get("cooldown_seconds", 60)))
                        try:
                            body, truncation = build_request(
                                {"event_a": payload1, "event_b": payload2},
                                min(118_000, int(self.options.get("max_request_bytes", 118_000))),
                            )
                            reply = self.client.post(
                                "https://openrouter.ai/api/alpha/decisions", content=encoded(body),
                                headers={"Authorization": "Bearer " + self.api_key, "Content-Type": "application/json"},
                                timeout=min(30, max(1, float(self.options.get("timeout_seconds", 20)))),
                            )
                            delay = max(delay, retry_delay(reply.headers.get("retry-after", "60")))
                            if reply.status_code in {401, 402, 403}:
                                delay = max(delay, 3600)
                            raw = reply.json()
                            if isinstance(raw, dict):
                                usage = normalize_usage(raw)
                            reply.raise_for_status()
                            if not isinstance(raw, dict) or raw.get("error"):
                                raise ValueError("Invalid Jev response")
                            decision = validate_response(raw)
                            model = "openrouter/" + decision["model"]
                            rationale = (f"Jev selected {'merge' if decision['should_merge'] else 'separate'}; "
                                         f"probability={decision['probability']:.4f}; "
                                         f"confidence={decision['confidence']:.4f}; "
                                         f"truncated={truncation['truncated']}; "
                                         f"retained_fraction={truncation['retained_fraction']:.4f}")
                            if self.progress:
                                self.progress("deduplicate: " + rationale)
                            return GeminiResult(
                                payload={"should_merge": decision["should_merge"],
                                         "confidence": decision["confidence"], "rationale": rationale},
                                model=model, elapsed_ms=int((time.monotonic() - started) * 1000),
                                usage=usage or {},
                            )
                        except Exception as exc:
                            # Exception messages may contain provider/request details; report only the type.
                            reason = type(exc).__name__
                            with self.lock:
                                self.cooldown_until = max(self.cooldown_until, time.monotonic() + delay)
        if self.progress:
            self.progress(f"deduplicate: Jev unavailable ({reason}); falling back to Flash")
        attempts = [{"model": self.model, "usage": usage}] if usage else []
        try:
            result = self.fallback.generate_json(**kwargs)
        except Exception as exc:
            if attempts:
                exc.jev_usage = attempts
            raise
        combined = dict(result.usage or {})
        combined["routingAttempts"] = attempts + list(combined.get("routingAttempts") or [])
        return GeminiResult(result.payload, result.model, result.elapsed_ms, combined)
