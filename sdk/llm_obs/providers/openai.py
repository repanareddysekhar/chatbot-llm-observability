from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from openai import OpenAI, AsyncOpenAI
    from ..client import ObservabilityClient


def wrap_openai(
    client: "OpenAI",
    obs: "ObservabilityClient",
    conversation_id_fn: Callable[[], str | None] | None = None,
) -> "OpenAI":
    """
    Wraps an OpenAI client so every chat.completions.create call is
    automatically logged to the observability backend.
    """
    original_create = client.chat.completions.create

    def instrumented_create(**kwargs: Any) -> Any:
        conv_id = conversation_id_fn() if conversation_id_fn else None
        messages = kwargs.get("messages", [])
        model = kwargs.get("model", "unknown")
        stream = kwargs.get("stream", False)

        span = obs.start_span(
            provider="openai",
            model=model,
            request={
                "messages": [{"role": m.get("role"), "content": str(m.get("content", ""))[:500]} for m in messages],
                "temperature": kwargs.get("temperature"),
                "max_tokens": kwargs.get("max_tokens"),
            },
            conversation_id=conv_id,
        )

        try:
            result = original_create(**kwargs)

            if stream:
                return _wrap_stream_openai(result, span)
            else:
                usage = result.usage
                if usage:
                    span.set_usage(
                        prompt_tokens=usage.prompt_tokens,
                        completion_tokens=usage.completion_tokens,
                    )
                content = ""
                if result.choices:
                    content = result.choices[0].message.content or ""
                span.append_output(content)
                finish = result.choices[0].finish_reason if result.choices else None
                span.end(status="success", finish_reason=finish, streamed=False)
                return result

        except Exception as exc:
            span.set_error(type(exc).__name__, str(exc)[:500])
            span.end(status="error")
            raise

    client.chat.completions.create = instrumented_create
    return client


def _wrap_stream_openai(stream: Any, span: Any) -> Any:
    first_token = True
    finish_reason = None
    try:
        for chunk in stream:
            if chunk.choices:
                delta = chunk.choices[0].delta
                if delta and delta.content:
                    if first_token:
                        span.set_ttft()
                        first_token = False
                    span.append_output(delta.content)
                if chunk.choices[0].finish_reason:
                    finish_reason = chunk.choices[0].finish_reason
            # Yield usage if present (stream_options={"include_usage": True})
            if hasattr(chunk, "usage") and chunk.usage:
                span.set_usage(
                    prompt_tokens=chunk.usage.prompt_tokens,
                    completion_tokens=chunk.usage.completion_tokens,
                )
            yield chunk
    except Exception as exc:
        span.set_error(type(exc).__name__, str(exc)[:500])
        span.end(status="error", streamed=True)
        raise
    finally:
        if not span._ended:
            span.end(status="success", finish_reason=finish_reason, streamed=True)
