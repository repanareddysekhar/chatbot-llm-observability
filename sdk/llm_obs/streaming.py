"""
Unified streaming interface for all LLM providers.

The web app (or any client) calls stream_chat() — that's it.
All provider-specific logic lives here in the SDK.
Providers are auto-detected from URLs via discovery.py.

Environment variables:
  LLM_ENDPOINTS   comma-separated URLs (new-style, recommended)
                  e.g. "http://localhost:11434,http://private-vpc:8080"
  Legacy keys still supported: OPENAI_API_KEY, ANTHROPIC_API_KEY, etc.
"""
from __future__ import annotations

import asyncio
import os
from typing import AsyncIterator, Any

from .logging import get_logger
from .discovery import (
    DiscoveredProvider,
    discover_from_env,
    to_providers_dict,
)

logger = get_logger("streaming")

# Populated lazily on first call
_discovered_cache: list[DiscoveredProvider] | None = None


def _get_discovered() -> list[DiscoveredProvider]:
    global _discovered_cache
    if _discovered_cache is None:
        _discovered_cache = discover_from_env()
    return _discovered_cache


def _find_provider(provider: str) -> DiscoveredProvider | None:
    for d in _get_discovered():
        if d.provider == provider:
            return d
    return None


def available_providers() -> dict[str, list[str]]:
    """
    Returns {provider: [models]} based on URLs detected from env.
    Used by the web UI to build provider/model dropdowns.
    """
    return to_providers_dict(_get_discovered())


async def stream_chat(
    provider: str,
    model: str,
    messages: list[dict],
    cancel_event: asyncio.Event | None = None,
) -> AsyncIterator[str]:
    """
    Unified async streaming entry point. Yields text delta chunks.
    Provider config (URL, API key) resolved from discovered providers.
    Observability injected transparently by auto_instrument().
    """
    if provider in ("openai", "openai_compatible", "ollama"):
        async for chunk in _openai_compat_stream(provider, model, messages, cancel_event):
            yield chunk
    elif provider == "anthropic":
        async for chunk in _anthropic_stream(model, messages, cancel_event):
            yield chunk
    elif provider == "google":
        async for chunk in _gemini_stream(model, messages, cancel_event):
            yield chunk
    elif provider == "bedrock":
        async for chunk in _bedrock_stream(model, messages, cancel_event):
            yield chunk
    else:
        raise ValueError(f"Unknown provider: {provider!r}")


async def _openai_compat_stream(
    provider: str,
    model: str,
    messages: list[dict],
    cancel_event: asyncio.Event | None,
) -> AsyncIterator[str]:
    """
    Handles OpenAI, Ollama, vLLM, LiteLLM and any OpenAI-compatible endpoint.
    URL and api_key resolved from discovered provider registry.
    """
    from openai import AsyncOpenAI

    info = _find_provider(provider)
    if info and info.base_url and "openai.com" not in info.base_url:
        base_url = info.base_url.rstrip("/") + "/v1"
        api_key = info.api_key or "no-key"
    else:
        base_url = None
        api_key = (info.api_key if info else None) or os.environ.get("OPENAI_API_KEY")

    client = AsyncOpenAI(base_url=base_url, api_key=api_key)
    stream = await client.chat.completions.create(
        model=model,
        messages=messages,
        stream=True,
        stream_options={"include_usage": True},
    )
    async for chunk in stream:
        if cancel_event and cancel_event.is_set():
            await stream.close()
            return
        if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content


async def _anthropic_stream(
    model: str,
    messages: list[dict],
    cancel_event: asyncio.Event | None,
) -> AsyncIterator[str]:
    from anthropic import AsyncAnthropic

    info = _find_provider("anthropic")
    api_key = (info.api_key if info else None) or os.environ.get("ANTHROPIC_API_KEY")
    client = AsyncAnthropic(api_key=api_key)

    system = next((m["content"] for m in messages if m["role"] == "system"), None)
    user_messages = [m for m in messages if m["role"] != "system"]
    kwargs: dict[str, Any] = {
        "model": model, "messages": user_messages, "max_tokens": 2048, "stream": True,
    }
    if system:
        kwargs["system"] = system

    stream = await client.messages.create(**kwargs)
    async for event in stream:
        if cancel_event and cancel_event.is_set():
            await stream.close()
            return
        if type(event).__name__ == "RawContentBlockDeltaEvent":
            text = getattr(event.delta, "text", "")
            if text:
                yield text


async def _gemini_stream(
    model: str,
    messages: list[dict],
    cancel_event: asyncio.Event | None,
) -> AsyncIterator[str]:
    import google.generativeai as genai

    info = _find_provider("google")
    api_key = (info.api_key if info else None) or os.environ.get("GOOGLE_API_KEY")
    genai.configure(api_key=api_key)

    system_parts = [m["content"] for m in messages if m["role"] == "system"]
    convo = [m for m in messages if m["role"] != "system"]
    history = [
        {"role": "model" if m["role"] == "assistant" else "user", "parts": [m["content"]]}
        for m in convo[:-1]
    ]
    gemini_model = genai.GenerativeModel(
        model, system_instruction=system_parts[0] if system_parts else None
    )
    response = await gemini_model.generate_content_async(messages, stream=True)
    async for chunk in response:
        if cancel_event and cancel_event.is_set():
            return
        try:
            if chunk.text:
                yield chunk.text
        except Exception:
            pass


async def _bedrock_stream(
    model: str,
    messages: list[dict],
    cancel_event: asyncio.Event | None,
) -> AsyncIterator[str]:
    """Stream from AWS Bedrock. Uses Anthropic-format body (supported by most models)."""
    import json
    import boto3

    region = os.environ.get("AWS_REGION", "us-east-1")
    bedrock = boto3.client("bedrock-runtime", region_name=region)

    system = next((m["content"] for m in messages if m["role"] == "system"), None)
    user_messages = [m for m in messages if m["role"] != "system"]
    body: dict[str, Any] = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": user_messages,
        "max_tokens": 2048,
    }
    if system:
        body["system"] = system

    response = await asyncio.to_thread(
        bedrock.invoke_model_with_response_stream,
        modelId=model,
        body=json.dumps(body),
    )
    for event in response.get("body", []):
        if cancel_event and cancel_event.is_set():
            return
        chunk = event.get("chunk", {})
        if chunk:
            data = json.loads(chunk.get("bytes", b"{}"))
            if data.get("type") == "content_block_delta":
                text = data.get("delta", {}).get("text", "")
                if text:
                    yield text
