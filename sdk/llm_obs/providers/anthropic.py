from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from anthropic import Anthropic
    from ..client import ObservabilityClient


def wrap_anthropic(
    client: "Anthropic",
    obs: "ObservabilityClient",
    conversation_id_fn: Callable[[], str | None] | None = None,
) -> "Anthropic":
    original_create = client.messages.create

    def instrumented_create(**kwargs: Any) -> Any:
        conv_id = conversation_id_fn() if conversation_id_fn else None
        messages = kwargs.get("messages", [])
        model = kwargs.get("model", "unknown")
        stream = kwargs.get("stream", False)

        span = obs.start_span(
            provider="anthropic",
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
                return _wrap_stream_anthropic(result, span)
            else:
                usage = result.usage
                if usage:
                    span.set_usage(
                        prompt_tokens=usage.input_tokens,
                        completion_tokens=usage.output_tokens,
                    )
                content = ""
                for block in result.content:
                    if hasattr(block, "text"):
                        content += block.text
                span.append_output(content)
                span.end(status="success", finish_reason=result.stop_reason, streamed=False)
                return result

        except Exception as exc:
            span.set_error(type(exc).__name__, str(exc)[:500])
            span.end(status="error")
            raise

    client.messages.create = instrumented_create
    return client


def _wrap_stream_anthropic(stream: Any, span: Any) -> Any:
    first_token = True
    finish_reason = None
    try:
        with stream as s:
            for event in s:
                event_type = type(event).__name__
                if event_type == "RawContentBlockDeltaEvent":
                    delta = getattr(event.delta, "text", "")
                    if delta:
                        if first_token:
                            span.set_ttft()
                            first_token = False
                        span.append_output(delta)
                elif event_type == "RawMessageDeltaEvent":
                    finish_reason = getattr(event.delta, "stop_reason", None)
                    usage = getattr(event.usage, None, None)
                    if usage:
                        span.set_usage(completion_tokens=getattr(usage, "output_tokens", None))
                elif event_type == "RawMessageStartEvent":
                    usage = getattr(event.message, "usage", None)
                    if usage:
                        span.set_usage(prompt_tokens=getattr(usage, "input_tokens", None))
                yield event
    except Exception as exc:
        span.set_error(type(exc).__name__, str(exc)[:500])
        span.end(status="error", streamed=True)
        raise
    finally:
        if not span._ended:
            span.end(status="success", finish_reason=finish_reason, streamed=True)
