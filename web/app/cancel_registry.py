"""In-memory registry of active inference AbortControllers keyed by inference_log_id."""
from __future__ import annotations

import asyncio
from typing import Optional

_registry: dict[str, asyncio.Event] = {}


def register(inference_log_id: str) -> asyncio.Event:
    """Register a cancel event for a given inference. Returns the event to watch."""
    event = asyncio.Event()
    _registry[inference_log_id] = event
    return event


def cancel(inference_log_id: str) -> bool:
    """Signal cancellation. Returns True if found."""
    event = _registry.get(inference_log_id)
    if event:
        event.set()
        return True
    return False


def unregister(inference_log_id: str) -> None:
    _registry.pop(inference_log_id, None)
