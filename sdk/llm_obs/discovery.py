"""
URL-based provider and model discovery.

Given any URL, this module probes it and figures out:
  - What service is running (Ollama, OpenAI-compatible, Bedrock, etc.)
  - What models are available

Env vars:
  LLM_ENDPOINTS   comma-separated list of base URLs to probe
                  e.g. "http://localhost:11434,http://private-vpc:8080"

  Falling back to legacy keys if LLM_ENDPOINTS is not set:
  OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, OLLAMA_BASE_URL

Detection strategy per URL (tried in order):
  1. Known cloud API URL patterns  → OpenAI / Anthropic / Google
  2. AWS Bedrock URL pattern        → Bedrock (boto3 model listing)
  3. Probe GET /api/tags            → Ollama
  4. Probe GET /v1/models           → OpenAI-compatible (vLLM, LiteLLM, LocalAI, etc.)
  5. Unknown                        → openai_compatible with empty model list
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

from .logging import get_logger

logger = get_logger("discovery")

ProviderType = Literal[
    "openai",
    "anthropic",
    "google",
    "ollama",
    "openai_compatible",   # vLLM, LiteLLM, LocalAI, private OpenAI-compat API
    "bedrock",
    "unknown",
]

# Known cloud API URL substrings → provider
_URL_PROVIDER_MAP = [
    ("api.openai.com",                   "openai"),
    ("api.anthropic.com",                "anthropic"),
    ("generativelanguage.googleapis.com","google"),
    ("openai.azure.com",                 "openai"),       # Azure OpenAI
]


@dataclass
class DiscoveredProvider:
    provider:  ProviderType
    base_url:  str
    models:    list[str] = field(default_factory=list)
    api_key:   str | None = None
    # Extra info for OpenAI-compat or Bedrock
    meta:      dict = field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def discover_from_env() -> list[DiscoveredProvider]:
    """
    Read LLM_ENDPOINTS (or legacy keys) from env and return discovered providers.
    Call this once at startup — results are used by available_providers() and stream_chat().
    """
    discovered: list[DiscoveredProvider] = []

    # ── New-style: LLM_ENDPOINTS=url1,url2,... ────────────────────────────────
    endpoints_raw = os.environ.get("LLM_ENDPOINTS", "").strip()
    if endpoints_raw:
        for url in [u.strip() for u in endpoints_raw.split(",") if u.strip()]:
            api_key = None
            # Allow "url|api_key" syntax for authenticated private endpoints
            if "|" in url:
                url, api_key = url.split("|", 1)
            result = detect_provider(url, api_key=api_key)
            if result:
                discovered.append(result)
            else:
                logger.warning("Could not detect provider at %s", url)

    # ── Legacy keys — still honoured if LLM_ENDPOINTS not set ─────────────────
    if not endpoints_raw:
        if os.environ.get("OPENAI_API_KEY"):
            discovered.append(DiscoveredProvider(
                provider="openai",
                base_url="https://api.openai.com",
                models=["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1"],
                api_key=os.environ["OPENAI_API_KEY"],
            ))
        if os.environ.get("ANTHROPIC_API_KEY"):
            discovered.append(DiscoveredProvider(
                provider="anthropic",
                base_url="https://api.anthropic.com",
                models=["claude-3-5-haiku-latest", "claude-3-5-sonnet-latest", "claude-sonnet-4-5"],
                api_key=os.environ["ANTHROPIC_API_KEY"],
            ))
        if os.environ.get("GOOGLE_API_KEY"):
            discovered.append(DiscoveredProvider(
                provider="google",
                base_url="https://generativelanguage.googleapis.com",
                models=["gemini-1.5-flash", "gemini-1.5-pro", "gemini-2.0-flash"],
                api_key=os.environ["GOOGLE_API_KEY"],
            ))
        ollama_url = os.environ.get("OLLAMA_BASE_URL", "").strip()
        if ollama_url:
            result = detect_provider(ollama_url)
            if result:
                discovered.append(result)

    return discovered


def detect_provider(url: str, api_key: str | None = None) -> DiscoveredProvider | None:
    """
    Probe a single URL and return a DiscoveredProvider, or None if unreachable.

    Detection order:
      1. Bedrock URL pattern (no HTTP probe needed)
      2. Known cloud API URL pattern
      3. Ollama  — GET /api/tags
      4. OpenAI-compatible — GET /v1/models
      5. Unknown fallback
    """
    url = url.rstrip("/")

    # 1. AWS Bedrock — detect by URL pattern, no HTTP probe
    if _is_bedrock_url(url):
        models = _list_bedrock_models()
        logger.info("Detected Bedrock at %s (%d models)", url, len(models))
        return DiscoveredProvider(provider="bedrock", base_url=url, models=models)

    # 2. Known cloud API URL patterns
    for pattern, provider in _URL_PROVIDER_MAP:
        if pattern in url:
            logger.info("Detected %s from URL pattern", provider)
            return DiscoveredProvider(
                provider=provider,  # type: ignore[arg-type]
                base_url=url,
                models=_known_cloud_models(provider),
                api_key=api_key,
            )

    # 3–4. Probe unknown URL
    return _probe_url(url, api_key)


def to_providers_dict(discovered: list[DiscoveredProvider]) -> dict[str, list[str]]:
    """Convert DiscoveredProvider list to the {provider: [models]} dict the UI expects."""
    result: dict[str, list[str]] = {}
    for d in discovered:
        key = d.provider
        # Multiple endpoints of the same type → merge model lists
        result.setdefault(key, [])
        for m in d.models:
            if m not in result[key]:
                result[key].append(m)
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _is_bedrock_url(url: str) -> bool:
    return "amazonaws.com" in url and "bedrock" in url


def _known_cloud_models(provider: str) -> list[str]:
    _CLOUD_MODELS = {
        "openai":    ["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1"],
        "anthropic": ["claude-3-5-haiku-latest", "claude-3-5-sonnet-latest", "claude-sonnet-4-5"],
        "google":    ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-2.0-flash"],
    }
    return _CLOUD_MODELS.get(provider, [])


def _probe_url(url: str, api_key: str | None) -> DiscoveredProvider | None:
    import httpx

    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    # ── Try Ollama: GET /api/tags ─────────────────────────────────────────────
    try:
        resp = httpx.get(f"{url}/api/tags", timeout=2.0)
        if resp.status_code == 200:
            data = resp.json()
            if "models" in data:
                models = [m["name"] for m in data["models"]]
                logger.info("Detected Ollama at %s (%d models pulled)", url, len(models))
                return DiscoveredProvider(
                    provider="ollama",
                    base_url=url,
                    models=models or _ollama_common_fallback(),
                    api_key=api_key,
                )
    except Exception:
        pass

    # ── Try OpenAI-compatible: GET /v1/models ─────────────────────────────────
    try:
        resp = httpx.get(f"{url}/v1/models", headers=headers, timeout=2.0)
        if resp.status_code == 200:
            data = resp.json()
            models = [m["id"] for m in data.get("data", [])]
            logger.info(
                "Detected OpenAI-compatible endpoint at %s (%d models)", url, len(models)
            )
            return DiscoveredProvider(
                provider="openai_compatible",
                base_url=url,
                models=models,
                api_key=api_key,
                meta={"detected_via": "v1/models"},
            )
    except Exception:
        pass

    # ── Unreachable or unrecognised ───────────────────────────────────────────
    logger.warning("Could not detect provider at %s — not reachable or unrecognised", url)
    return None


def _list_bedrock_models() -> list[str]:
    """List available foundation models from AWS Bedrock via boto3."""
    try:
        import boto3
        client = boto3.client("bedrock", region_name=os.environ.get("AWS_REGION", "us-east-1"))
        resp = client.list_foundation_models(byOutputModality="TEXT")
        models = [
            m["modelId"]
            for m in resp.get("modelSummaries", [])
            if m.get("responseStreamingSupported", False)
        ]
        logger.info("Discovered %d Bedrock streaming models", len(models))
        return models
    except Exception as exc:
        logger.warning("Bedrock model listing failed: %s", exc)
        # Return common Bedrock models as fallback
        return [
            "anthropic.claude-3-5-sonnet-20241022-v2:0",
            "anthropic.claude-3-5-haiku-20241022-v1:0",
            "anthropic.claude-3-haiku-20240307-v1:0",
            "meta.llama3-8b-instruct-v1:0",
            "meta.llama3-70b-instruct-v1:0",
            "amazon.titan-text-express-v1",
            "mistral.mistral-7b-instruct-v0:2",
        ]


def _ollama_common_fallback() -> list[str]:
    return [
        "gemma3:4b", "gemma3:1b", "llama3.2", "llama3.1:8b",
        "mistral", "phi3", "qwen2.5:7b", "deepseek-r1:7b",
    ]
