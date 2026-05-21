from __future__ import annotations

import os
from typing import Any

from .span import InferenceSpan
from .transport import BatchTransport


class ObservabilityClient:
    """
    Central client for recording LLM inference logs.
    Thread-safe. Batches and ships events to the ingestion API.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        api_key: str | None = None,
        environment: str | None = None,
        sdk_version: str = "0.1.0",
        batch_size: int = 20,
        flush_interval_s: float = 2.0,
        max_retries: int = 3,
        on_error: Any = None,
        default_metadata: dict[str, Any] | None = None,
    ) -> None:
        self.endpoint = endpoint or os.environ.get("INGEST_URL", "http://localhost:4000")
        self.api_key = api_key or os.environ.get("INGEST_API_KEY")
        self.environment = environment or os.environ.get("ENVIRONMENT", "dev")
        self.sdk_version = sdk_version
        self.default_metadata = default_metadata or {}

        self._transport = BatchTransport(
            endpoint=self.endpoint,
            api_key=self.api_key,
            batch_size=batch_size,
            flush_interval_s=flush_interval_s,
            max_retries=max_retries,
            on_error=on_error,
        )

    def start_span(
        self,
        provider: str,
        model: str,
        request: dict[str, Any],
        conversation_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> InferenceSpan:
        span = InferenceSpan(
            provider=provider,
            model=model,
            request=request,
            conversation_id=conversation_id,
            session_id=session_id,
            metadata={**self.default_metadata, **(metadata or {})},
            _client=self,
        )
        return span

    def log(self, payload: dict[str, Any]) -> None:
        payload.setdefault("environment", self.environment)
        payload.setdefault("sdk_version", self.sdk_version)
        self._transport.enqueue(payload)

    def flush(self) -> None:
        self._transport.flush()

    def shutdown(self) -> None:
        self._transport.shutdown()
