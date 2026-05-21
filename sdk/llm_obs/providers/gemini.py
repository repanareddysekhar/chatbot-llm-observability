from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from ..client import ObservabilityClient


def wrap_gemini(
    model_instance: Any,
    obs: "ObservabilityClient",
    model_name: str = "gemini-1.5-flash",
    conversation_id_fn: Callable[[], str | None] | None = None,
) -> Any:
    """
    Wraps a google.generativeai.GenerativeModel instance.
    Instruments generate_content and generate_content_stream.
    """
    original_generate = model_instance.generate_content

    def instrumented_generate(contents: Any, **kwargs: Any) -> Any:
        conv_id = conversation_id_fn() if conversation_id_fn else None
        stream = kwargs.get("stream", False)

        prompt_text = _extract_gemini_prompt(contents)
        span = obs.start_span(
            provider="google",
            model=model_name,
            request={"prompt": prompt_text[:500], "extra": kwargs},
            conversation_id=conv_id,
        )

        try:
            result = original_generate(contents, **kwargs)
            if stream:
                return _wrap_stream_gemini(result, span)
            else:
                content = result.text if hasattr(result, "text") else ""
                span.append_output(content)
                usage = getattr(result, "usage_metadata", None)
                if usage:
                    span.set_usage(
                        prompt_tokens=getattr(usage, "prompt_token_count", None),
                        completion_tokens=getattr(usage, "candidates_token_count", None),
                    )
                span.end(status="success", streamed=False)
                return result
        except Exception as exc:
            span.set_error(type(exc).__name__, str(exc)[:500])
            span.end(status="error")
            raise

    model_instance.generate_content = instrumented_generate
    return model_instance


def _wrap_stream_gemini(stream: Any, span: Any) -> Any:
    first_token = True
    try:
        for chunk in stream:
            text = chunk.text if hasattr(chunk, "text") else ""
            if text:
                if first_token:
                    span.set_ttft()
                    first_token = False
                span.append_output(text)
            yield chunk
    except Exception as exc:
        span.set_error(type(exc).__name__, str(exc)[:500])
        span.end(status="error", streamed=True)
        raise
    finally:
        if not span._ended:
            span.end(status="success", streamed=True)


def _extract_gemini_prompt(contents: Any) -> str:
    if isinstance(contents, str):
        return contents
    if isinstance(contents, list):
        parts = []
        for item in contents:
            if isinstance(item, str):
                parts.append(item)
            elif hasattr(item, "parts"):
                for p in item.parts:
                    if hasattr(p, "text"):
                        parts.append(p.text)
        return " ".join(parts)
    return str(contents)
