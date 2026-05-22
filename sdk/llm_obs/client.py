from __future__ import annotations

import os
from typing import Any

from .logging import get_logger
from .span import InferenceSpan
from .transport import BatchTransport

logger = get_logger("client")


class ObservabilityClient:
    """
    Central client for recording LLM inference logs.

    Typical usage — one line in your app startup:

        from llm_obs import ObservabilityClient
        obs = ObservabilityClient(endpoint="http://ingestion:4000", api_key="dev-key")
        obs.auto_instrument()   # patches openai / anthropic / gemini / bedrock automatically

    From that point on every LLM call is logged automatically.
    No other observability code needed anywhere in the application.
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
        redact_pii: bool = True,
    ) -> None:
        self.endpoint = endpoint or os.environ.get("INGEST_URL", "http://localhost:4000")
        self.api_key = api_key or os.environ.get("INGEST_API_KEY")
        self.environment = environment or os.environ.get("ENVIRONMENT", "dev")
        self.sdk_version = sdk_version
        self.default_metadata = default_metadata or {}
        self.redact_pii = redact_pii

        self._transport = BatchTransport(
            endpoint=self.endpoint,
            api_key=self.api_key,
            batch_size=batch_size,
            flush_interval_s=flush_interval_s,
            max_retries=max_retries,
            on_error=on_error,
        )

        # Lazy-loaded PII redactor
        self._pii_redact_fn = None
        if redact_pii:
            self._load_pii()

    def _load_pii(self) -> None:
        try:
            from .pii import redact_deep
            self._pii_redact_fn = redact_deep
            logger.debug("PII redaction enabled")
        except ImportError:
            logger.warning("llm_obs.pii not available — PII redaction disabled")

    def auto_instrument(self) -> "ObservabilityClient":
        """
        Monkey-patch all installed LLM libraries so every call is automatically
        logged. Call once at application startup — no other observability code needed.

        Patches:
          - openai.AsyncOpenAI  (AsyncCompletions.create)
          - anthropic.AsyncAnthropic  (AsyncMessages.create)
          - google.generativeai  (GenerativeModel.generate_content_async)
          - boto3 bedrock-runtime  (invoke_model / invoke_model_with_response_stream)
        """
        from .providers.openai import patch_openai_class
        from .providers.anthropic import patch_anthropic_class
        from .providers.gemini import patch_gemini_class
        from .providers.bedrock import patch_bedrock_client

        patched = []
        if patch_openai_class(self):
            patched.append("openai")
        if patch_anthropic_class(self):
            patched.append("anthropic")
        if patch_gemini_class(self):
            patched.append("gemini")
        if patch_bedrock_client(self):
            patched.append("bedrock")

        if patched:
            logger.info("auto_instrument patched: %s", ", ".join(patched))
        else:
            logger.warning("auto_instrument: no LLM libraries found to patch")

        return self

    def start_span(
        self,
        provider: str,
        model: str,
        request: dict[str, Any],
        conversation_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> InferenceSpan:
        return InferenceSpan(
            provider=provider,
            model=model,
            request=request,
            conversation_id=conversation_id,
            session_id=session_id,
            metadata={**self.default_metadata, **(metadata or {})},
            _client=self,
        )

    def log(self, payload: dict[str, Any]) -> None:
        """
        Enqueue a log payload for async delivery to the ingestion API.
        PII redaction runs here — in the client process, before data leaves via HTTP.
        """
        if self._pii_redact_fn:
            payload = self._redact_payload(payload)

        payload.setdefault("environment", self.environment)
        payload.setdefault("sdk_version", self.sdk_version)
        self._transport.enqueue(payload)

    def _redact_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Redact PII from request and response fields before transport."""
        import copy
        payload = copy.deepcopy(payload)
        try:
            if payload.get("request"):
                clean_req, _ = self._pii_redact_fn(payload["request"])
                payload["request"] = clean_req
            if payload.get("response"):
                clean_resp, _ = self._pii_redact_fn(payload["response"])
                payload["response"] = clean_resp
        except Exception as exc:
            logger.warning("PII redaction failed: %s", exc)
        return payload

    def flush(self) -> None:
        self._transport.flush()

    def shutdown(self) -> None:
        self._transport.shutdown()
