"""List current OpenRouter text-output models and their JSON capabilities; no inference calls."""
from __future__ import annotations

import argparse
import json

import httpx


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--free", action="store_true", help="Only models with zero input and output token prices")
    parser.add_argument("--structured", action="store_true", help="Only models advertising JSON Schema support")
    args = parser.parse_args()
    response = httpx.get("https://openrouter.ai/api/v1/models", timeout=30)
    response.raise_for_status()
    models = []
    for model in response.json()["data"]:
        if model["id"].startswith("openrouter/"):
            continue  # Routers do not identify a repeatable model candidate.
        if "text" not in (model.get("architecture") or {}).get("output_modalities", []):
            continue
        prices = model.get("pricing") or {}
        input_price = float(prices["prompt"]) if prices.get("prompt") is not None else None
        output_price = float(prices["completion"]) if prices.get("completion") is not None else None
        free = input_price == 0 and output_price == 0
        parameters = model.get("supported_parameters") or []
        structured = "structured_outputs" in parameters
        if (args.free and not free) or (args.structured and not structured):
            continue
        models.append({
            "id": model["id"], "context_length": model.get("context_length"),
            "input_usd_per_million": input_price * 1e6 if input_price is not None else None,
            "output_usd_per_million": output_price * 1e6 if output_price is not None else None,
            "free_tokens": free, "structured_outputs": structured,
            "response_format": "response_format" in parameters,
        })
    print(json.dumps({"models": sorted(models, key=lambda item: item["id"]), "inference_calls": 0}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
