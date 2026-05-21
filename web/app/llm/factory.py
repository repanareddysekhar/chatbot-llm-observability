from __future__ import annotations

import asyncio
from typing import AsyncIterator, Any

from ..config import settings

PROVIDER_MODELS = {
    "openai": ["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1"],
    "anthropic": ["claude-3-5-haiku-latest", "claude-3-5-sonnet-latest", "claude-sonnet-4-5"],
    "google": ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-2.0-flash"],
    # Ollama models — populated dynamically from the running Ollama instance
    "ollama": [],
}

# Common small models people run locally with Ollama
OLLAMA_COMMON_MODELS = [
    "gemma3:4b", "gemma3:1b", "gemma3:12b",
    "llama3.2", "llama3.2:1b", "llama3.1", "llama3.1:8b",
    "mistral", "mistral:7b", "mistral-nemo",
    "gemma2", "gemma2:2b", "gemma2:9b",
    "phi3", "phi3:mini",
    "qwen2.5", "qwen2.5:7b",
    "deepseek-r1:7b", "deepseek-r1:1.5b",
    "codellama", "codellama:7b",
]


async def stream_chat(
    provider: str,
    model: str,
    messages: list[dict],
    obs_client: Any | None = None,
    conversation_id: str | None = None,
    cancel_event: asyncio.Event | None = None,
) -> AsyncIterator[str]:
    """
    Unified streaming chat across providers.
    Yields text delta chunks.
    Logs to obs_client if provided.
    """
    if provider == "openai":
        async for chunk in _openai_stream(model, messages, obs_client, conversation_id, cancel_event):
            yield chunk
    elif provider == "anthropic":
        async for chunk in _anthropic_stream(model, messages, obs_client, conversation_id, cancel_event):
            yield chunk
    elif provider == "google":
        async for chunk in _gemini_stream(model, messages, obs_client, conversation_id, cancel_event):
            yield chunk
    elif provider == "ollama":
        async for chunk in _ollama_stream(model, messages, obs_client, conversation_id, cancel_event):
            yield chunk
    else:
        raise ValueError(f"Unknown provider: {provider}")


async def _openai_stream(model, messages, obs_client, conv_id, cancel_event):
    import time
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=settings.openai_api_key)
    started = time.monotonic()
    started_iso = _now_iso()
    span_id = _new_id()
    first_token = True
    ttft_ms = None
    output_chunks = []

    try:
        stream = await client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
            stream_options={"include_usage": True},
        )
        async for chunk in stream:
            if cancel_event and cancel_event.is_set():
                await stream.close()
                _send_log(obs_client, span_id, "openai", model, conv_id, messages,
                          output_chunks, None, "cancelled", started, started_iso, ttft_ms, True)
                return

            if chunk.choices:
                delta = chunk.choices[0].delta
                if delta and delta.content:
                    if first_token:
                        ttft_ms = int((time.monotonic() - started) * 1000)
                        first_token = False
                    output_chunks.append(delta.content)
                    yield delta.content

            if hasattr(chunk, "usage") and chunk.usage:
                usage = {"prompt_tokens": chunk.usage.prompt_tokens, "completion_tokens": chunk.usage.completion_tokens}
                _send_log(obs_client, span_id, "openai", model, conv_id, messages,
                          output_chunks, usage, "success", started, started_iso, ttft_ms, True)
                return

    except Exception as exc:
        _send_log(obs_client, span_id, "openai", model, conv_id, messages,
                  output_chunks, None, "error", started, started_iso, ttft_ms, True,
                  error={"type": type(exc).__name__, "message": str(exc)[:500]})
        raise


async def _anthropic_stream(model, messages, obs_client, conv_id, cancel_event):
    import asyncio
    import time
    from anthropic import AsyncAnthropic

    client = AsyncAnthropic(api_key=settings.anthropic_api_key)
    started = time.monotonic()
    started_iso = _now_iso()
    span_id = _new_id()
    first_token = True
    ttft_ms = None
    output_chunks = []
    usage = {}

    try:
        async with client.messages.stream(
            model=model, messages=messages, max_tokens=2048
        ) as stream:
            async for text in stream.text_stream:
                if cancel_event and cancel_event.is_set():
                    _send_log(obs_client, span_id, "anthropic", model, conv_id, messages,
                              output_chunks, usage or None, "cancelled", started, started_iso, ttft_ms, True)
                    return
                if first_token:
                    ttft_ms = int((time.monotonic() - started) * 1000)
                    first_token = False
                output_chunks.append(text)
                yield text

            msg = await stream.get_final_message()
            if msg.usage:
                usage = {"prompt_tokens": msg.usage.input_tokens, "completion_tokens": msg.usage.output_tokens}

        _send_log(obs_client, span_id, "anthropic", model, conv_id, messages,
                  output_chunks, usage or None, "success", started, started_iso, ttft_ms, True)

    except Exception as exc:
        _send_log(obs_client, span_id, "anthropic", model, conv_id, messages,
                  output_chunks, None, "error", started, started_iso, ttft_ms, True,
                  error={"type": type(exc).__name__, "message": str(exc)[:500]})
        raise


async def _gemini_stream(model, messages, obs_client, conv_id, cancel_event):
    import asyncio
    import time
    import google.generativeai as genai

    genai.configure(api_key=settings.google_api_key)
    gemini_model = genai.GenerativeModel(model)

    # Convert messages to Gemini format
    prompt = "\n".join(f"{m['role'].upper()}: {m['content']}" for m in messages)

    started = time.monotonic()
    started_iso = _now_iso()
    span_id = _new_id()
    first_token = True
    ttft_ms = None
    output_chunks = []

    try:
        response = await asyncio.to_thread(
            gemini_model.generate_content, prompt, stream=True
        )
        for chunk in response:
            if cancel_event and cancel_event.is_set():
                _send_log(obs_client, span_id, "google", model, conv_id, messages,
                          output_chunks, None, "cancelled", started, started_iso, ttft_ms, True)
                return
            text = chunk.text if hasattr(chunk, "text") else ""
            if text:
                if first_token:
                    ttft_ms = int((time.monotonic() - started) * 1000)
                    first_token = False
                output_chunks.append(text)
                yield text

        _send_log(obs_client, span_id, "google", model, conv_id, messages,
                  output_chunks, None, "success", started, started_iso, ttft_ms, True)

    except Exception as exc:
        _send_log(obs_client, span_id, "google", model, conv_id, messages,
                  output_chunks, None, "error", started, started_iso, ttft_ms, True,
                  error={"type": type(exc).__name__, "message": str(exc)[:500]})
        raise


async def _ollama_stream(model, messages, obs_client, conv_id, cancel_event):
    """
    Ollama exposes an OpenAI-compatible API at /v1 — we reuse the OpenAI async client
    pointed at the Ollama base URL. No real API key required ('ollama' is the dummy).
    """
    import time
    from openai import AsyncOpenAI

    base_url = (settings.ollama_base_url or "http://localhost:11434").rstrip("/") + "/v1"
    client = AsyncOpenAI(base_url=base_url, api_key="ollama")

    started = time.monotonic()
    started_iso = _now_iso()
    span_id = _new_id()
    first_token = True
    ttft_ms = None
    output_chunks = []

    try:
        stream = await client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
        )
        async for chunk in stream:
            if cancel_event and cancel_event.is_set():
                await stream.close()
                _send_log(obs_client, span_id, "ollama", model, conv_id, messages,
                          output_chunks, None, "cancelled", started, started_iso, ttft_ms, True)
                return

            if chunk.choices:
                delta = chunk.choices[0].delta
                if delta and delta.content:
                    if first_token:
                        ttft_ms = int((time.monotonic() - started) * 1000)
                        first_token = False
                    output_chunks.append(delta.content)
                    yield delta.content

        _send_log(obs_client, span_id, "ollama", model, conv_id, messages,
                  output_chunks, None, "success", started, started_iso, ttft_ms, True)

    except Exception as exc:
        _send_log(obs_client, span_id, "ollama", model, conv_id, messages,
                  output_chunks, None, "error", started, started_iso, ttft_ms, True,
                  error={"type": type(exc).__name__, "message": str(exc)[:500]})
        raise


def _send_log(obs_client, span_id, provider, model, conv_id, messages,
              output_chunks, usage, status, started, started_iso, ttft_ms, streamed, error=None):
    if not obs_client:
        return
    import time
    from datetime import datetime, timezone

    latency_ms = int((time.monotonic() - started) * 1000)
    ended_iso = datetime.now(timezone.utc).isoformat()
    full_output = "".join(output_chunks)

    payload = {
        "id": span_id,
        "conversation_id": conv_id,
        "provider": provider,
        "model": model,
        "status": status,
        "started_at": started_iso,
        "ended_at": ended_iso,
        "latency_ms": latency_ms,
        "ttft_ms": ttft_ms,
        "streamed": streamed,
        "request": {
            "messages": [{"role": m.get("role"), "content": str(m.get("content", ""))[:500]} for m in messages],
        },
        "response": {"content": full_output[:2048]},
        "usage": usage,
        "error": error,
    }
    obs_client.log(payload)


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    try:
        import ulid
        return str(ulid.new())
    except ImportError:
        import uuid
        return str(uuid.uuid4())


def get_provider_client() -> dict[str, list[str]]:
    """Returns available providers based on configured API keys + Ollama discovery."""
    available = {}
    if settings.openai_api_key:
        available["openai"] = PROVIDER_MODELS["openai"]
    if settings.anthropic_api_key:
        available["anthropic"] = PROVIDER_MODELS["anthropic"]
    if settings.google_api_key:
        available["google"] = PROVIDER_MODELS["google"]

    # Check Ollama — no API key needed, just needs OLLAMA_BASE_URL set
    if settings.ollama_base_url:
        models = _discover_ollama_models(settings.ollama_base_url)
        # Always show Ollama if URL is configured, even if discovery fails
        available["ollama"] = models if models else OLLAMA_COMMON_MODELS

    return available


def _discover_ollama_models(base_url: str) -> list[str]:
    """
    Call GET /api/tags on the Ollama server to list pulled models.
    Returns the pulled model names, or [] if Ollama is unreachable.
    """
    import httpx

    url = base_url.rstrip("/") + "/api/tags"
    try:
        resp = httpx.get(url, timeout=2.0)
        if resp.status_code == 200:
            data = resp.json()
            models = [m["name"] for m in data.get("models", [])]
            return models  # may be empty if nothing is pulled
    except Exception:
        pass
    return []
